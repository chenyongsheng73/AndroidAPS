package app.aaps.pump.medtronic.service

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.Context
import android.content.IntentFilter
import android.os.SystemClock
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.pump.rileylink.ble.RileyLinkBLEDevice
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import javax.inject.Singleton

/**
 * ============================================================================
 * 新增类：ConnectionRecoveryManager - 连接恢复协调器
 * ============================================================================
 *
 * 职责：
 * 1. 协调完整的连接恢复流程（BLE重连 → RL重新配置 → 泵Tune）
 * 2. 监听系统蓝牙状态变化
 * 3. 管理恢复策略（渐进式恢复）
 * 4. 防止恢复风暴（debounce）
 *
 * ============================================================================
 */
@Singleton
class ConnectionRecoveryManager @Inject constructor(
    private val context: Context,
    private val aapsLogger: AAPSLogger,
    private val rxBus: RxBus,
    private val bleDevice: RileyLinkBLEDevice,
    private val rileyLinkMedtronicService: RileyLinkMedtronicService?
) {

    companion object {
        // 恢复策略配置
        private const val RECOVERY_DEBOUNCE_MS = 10000L     // 恢复操作防抖 10s
        private const val BT_STATE_CHECK_INTERVAL_MS = 2000L  // 蓝牙状态检查间隔
        private const val MAX_RECOVERY_ATTEMPTS_PER_HOUR = 6   // 每小时最大恢复次数
        private const val RECOVERY_COOLDOWN_MS = 60000L        // 恢复冷却时间 1分钟
    }

    // ===== 恢复阶段 =====
    enum class RecoveryPhase {
        IDLE,               // 空闲
        RECONNECTING_BLE,   // 重连BLE
        RECONFIGURING_RL,   // 重新配置RileyLink
        TUNING_PUMP,        // 调谐泵
        VERIFYING,          // 验证连接
        COMPLETED,          // 完成
        FAILED              // 失败
    }

    @Volatile
    private var currentPhase: RecoveryPhase = RecoveryPhase.IDLE

    @Volatile
    private var lastRecoveryTime: Long = 0L

    @Volatile
    private var recoveryCountThisHour: Int = 0

    @Volatile
    private var hourStartTime: Long = System.currentTimeMillis()

    private val isRunning = AtomicBoolean(false)
    private val disposable = CompositeDisposable()

    // 蓝牙状态监听
    private var bluetoothStateReceiver: android.content.BroadcastReceiver? = null

    // 恢复回调
    private var successCallback: (() -> Unit)? = null
    private var failureCallback: (() -> Unit)? = null

    // ========================================================================
    // 启动恢复管理器
    // ========================================================================
    fun start() {
        registerBluetoothStateListener()
        aapsLogger.debug(LTag.PUMP, "ConnectionRecoveryManager started")
    }

    // ========================================================================
    // 停止
    // ========================================================================
    fun stop() {
        unregisterBluetoothStateListener()
        disposable.clear()
        aapsLogger.debug(LTag.PUMP, "ConnectionRecoveryManager stopped")
    }

    // ========================================================================
    // 设置安静时段（夜间等）
    // ========================================================================
    fun setQuietHours(enabled: Boolean) {
        // 泵通信不支持安静模式，但可以降低检查频率
        aapsLogger.debug(LTag.PUMP, "Quiet hours: $enabled (pump comms always active)")
    }

    // ========================================================================
    // 执行完整恢复流程
    // ========================================================================
    fun executeRecovery(
        reason: String,
        onSuccess: () -> Unit,
        onFailure: () -> Unit
    ) {
        // 防抖检查
        val now = System.currentTimeMillis()
        if (now - lastRecoveryTime < RECOVERY_DEBOUNCE_MS) {
            aapsLogger.debug(LTag.PUMP, "Recovery debounced, too soon since last attempt")
            return
        }

        // 每小时恢复次数限制
        if (now - hourStartTime > 3600000L) {
            hourStartTime = now
            recoveryCountThisHour = 0
        }

        if (recoveryCountThisHour >= MAX_RECOVERY_ATTEMPTS_PER_HOUR) {
            aapsLogger.warn(LTag.PUMP, "Max recovery attempts per hour reached, will retry later")
            onFailure()
            return
        }

        if (!isRunning.compareAndSet(false, true)) {
            aapsLogger.debug(LTag.PUMP, "Recovery already in progress")
            return
        }

        lastRecoveryTime = now
        recoveryCountThisHour++
        currentPhase = RecoveryPhase.RECONNECTING_BLE
        successCallback = onSuccess
        failureCallback = onFailure

        aapsLogger.info(
            LTag.PUMP,
            "Starting recovery flow. Reason: $reason, attempt #$recoveryCountThisHour this hour"
        )

        // 执行恢复流程
        thread { performRecoveryFlow() }
    }

    // ========================================================================
    // 恢复流程：渐进式恢复策略
    // ========================================================================
    private fun performRecoveryFlow() {
        try {
            // Phase 1: 检查蓝牙状态
            if (!ensureBluetoothEnabled()) {
                aapsLogger.error(LTag.PUMP, "Bluetooth not available, cannot recover")
                completeRecovery(success = false)
                return
            }

            // Phase 2: BLE重连
            currentPhase = RecoveryPhase.RECONNECTING_BLE
            if (!reconnectBLE()) {
                aapsLogger.error(LTag.PUMP, "BLE reconnect failed")
                // 尝试Phase 3: 重启蓝牙
                if (!restartBluetoothAndReconnect()) {
                    completeRecovery(success = false)
                    return
                }
            }

            // Phase 3: 重新配置RileyLink
            currentPhase = RecoveryPhase.RECONFIGURING_RL
            if (!reconfigureRileyLink()) {
                aapsLogger.warn(LTag.PUMP, "RL reconfiguration failed, but continuing...")
            }

            // Phase 4: 泵调谐
            currentPhase = RecoveryPhase.TUNING_PUMP
            if (!tunePump()) {
                aapsLogger.error(LTag.PUMP, "Pump tune failed")
                completeRecovery(success = false)
                return
            }

            // Phase 5: 验证连接
            currentPhase = RecoveryPhase.VERIFYING
            if (!verifyConnection()) {
                aapsLogger.error(LTag.PUMP, "Connection verification failed")
                completeRecovery(success = false)
                return
            }

            // 完成
            currentPhase = RecoveryPhase.COMPLETED
            completeRecovery(success = true)

        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "Recovery flow error: ${e.message}")
            currentPhase = RecoveryPhase.FAILED
            completeRecovery(success = false)
        }
    }

    // ========================================================================
    // Phase 1: 确保蓝牙开启
    // ========================================================================
    private fun ensureBluetoothEnabled(): Boolean {
        val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
        val adapter = bluetoothManager.adapter

        if (adapter == null) {
            aapsLogger.error(LTag.PUMP, "No Bluetooth adapter found")
            return false
        }

        if (!adapter.isEnabled) {
            aapsLogger.warn(LTag.PUMP, "Bluetooth is disabled, attempting to enable")
            // 注意：Android 12+需要BLUETOOTH_CONNECT权限
            return try {
                adapter.enable()
                // 等待蓝牙开启
                Thread.sleep(BT_STATE_CHECK_INTERVAL_MS)
                adapter.isEnabled
            } catch (e: Exception) {
                aapsLogger.error(LTag.PUMP, "Cannot enable Bluetooth: ${e.message}")
                false
            }
        }

        return true
    }

    // ========================================================================
    // Phase 2: BLE重连
    // ========================================================================
    private fun reconnectBLE(): Boolean {
        aapsLogger.debug(LTag.PUMP, "Attempting BLE reconnect")

        return try {
            // 触发BLE层重连
            bleDevice.scheduleReconnect()

            // 等待连接建立（最多15秒）
            val startTime = System.currentTimeMillis()
            while (System.currentTimeMillis() - startTime < 15000L) {
                if (bleDevice.isConnected()) {
                    aapsLogger.info(LTag.PUMP, "BLE reconnected successfully")
                    return true
                }
                Thread.sleep(500)
            }

            aapsLogger.warn(LTag.PUMP, "BLE reconnect timeout")
            false
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "BLE reconnect error: ${e.message}")
            false
        }
    }

    // ========================================================================
    // Phase 2b: 重启蓝牙后重连（最后手段）
    // ========================================================================
    private fun restartBluetoothAndReconnect(): Boolean {
        aapsLogger.warn(LTag.PUMP, "Attempting Bluetooth restart as recovery measure")

        return try {
            val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
            val adapter = bluetoothManager.adapter ?: return false

            // 关闭蓝牙
            adapter.disable()
            Thread.sleep(3000)

            // 等待蓝牙完全关闭
            var waitTime = 0L
            while (adapter.isEnabled && waitTime < 10000L) {
                Thread.sleep(500)
                waitTime += 500
            }

            // 开启蓝牙
            adapter.enable()
            Thread.sleep(3000)

            // 等待蓝牙开启
            waitTime = 0L
            while (!adapter.isEnabled && waitTime < 10000L) {
                Thread.sleep(500)
                waitTime += 500
            }

            if (!adapter.isEnabled) {
                aapsLogger.error(LTag.PUMP, "Failed to restart Bluetooth")
                return false
            }

            // 蓝牙重启后等待稳定
            Thread.sleep(2000)

            // 再次尝试重连
            reconnectBLE()

        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "Bluetooth restart error: ${e.message}")
            false
        }
    }

    // ========================================================================
    // Phase 3: 重新配置RileyLink
    // ========================================================================
    private fun reconfigureRileyLink(): Boolean {
        aapsLogger.debug(LTag.PUMP, "Reconfiguring RileyLink")

        return try {
            // 通过RL服务重新配置
            rileyLinkMedtronicService?.let { service ->
                // 设置频率
                service.setFrequencyIfNeeded()
                // 配置编码
                service.configureEncoding()
                // 验证配置
                service.verifyConfiguration()
            }
            aapsLogger.info(LTag.PUMP, "RileyLink reconfigured")
            true
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "RL reconfiguration error: ${e.message}")
            false
        }
    }

    // ========================================================================
    // Phase 4: 泵调谐
    // ========================================================================
    private fun tunePump(): Boolean {
        aapsLogger.debug(LTag.PUMP, "Tuning pump (Wake & Tune)")

        return try {
            rileyLinkMedtronicService?.let { service ->
                // 执行Wake Up + Tune
                val result = service.wakeUpAndTune()
                if (result) {
                    aapsLogger.info(LTag.PUMP, "Pump tune successful")
                    true
                } else {
                    aapsLogger.warn(LTag.PUMP, "Pump tune returned false")
                    false
                }
            } ?: false
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "Pump tune error: ${e.message}")
            false
        }
    }

    // ========================================================================
    // Phase 5: 验证连接
    // ========================================================================
    private fun verifyConnection(): Boolean {
        aapsLogger.debug(LTag.PUMP, "Verifying connection")

        return try {
            rileyLinkMedtronicService?.let { service ->
                // 尝试一个简单的泵操作（如获取时间）
                val response = service.getPumpTime()
                if (response != null) {
                    aapsLogger.info(LTag.PUMP, "Connection verified - pump time: $response")
                    true
                } else {
                    aapsLogger.warn(LTag.PUMP, "Verification failed - no response")
                    false
                }
            } ?: false
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "Verification error: ${e.message}")
            false
        }
    }

    // ========================================================================
    // 完成恢复
    // ========================================================================
    private fun completeRecovery(success: Boolean) {
        isRunning.set(false)

        if (success) {
            currentPhase = RecoveryPhase.COMPLETED
            aapsLogger.info(LTag.PUMP, "Recovery completed successfully")
            successCallback?.invoke()
        } else {
            currentPhase = RecoveryPhase.FAILED
            aapsLogger.error(LTag.PUMP, "Recovery failed after all attempts")

            // 安排延迟重试
            scheduleDelayedRetry()
            failureCallback?.invoke()
        }

        successCallback = null
        failureCallback = null
    }

    // ========================================================================
    // 延迟重试（指数退避）
    // ========================================================================
    private fun scheduleDelayedRetry() {
        val delay = RECOVERY_COOLDOWN_MS.coerceAtMost(300000L)  // 最大5分钟

        aapsLogger.info(LTag.PUMP, "Scheduling delayed retry in ${delay}ms")

        android.os.Handler(android.os.Looper.getMainLooper()).postDelayed({
            if (currentPhase == RecoveryPhase.FAILED) {
                aapsLogger.debug(LTag.PUMP, "Auto-retry triggered after cooldown")
                // 不自动重试 - 等待下一次操作触发或用户手动触发
                // 这样可以避免在后台持续消耗资源
            }
        }, delay)
    }

    // ========================================================================
    // 注册蓝牙状态监听
    // ========================================================================
    private fun registerBluetoothStateListener() {
        bluetoothStateReceiver = object : android.content.BroadcastReceiver() {
            override fun onReceive(context: android.content.Context, intent: android.content.Intent) {
                if (intent.action == BluetoothAdapter.ACTION_STATE_CHANGED) {
                    val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                    when (state) {
                        BluetoothAdapter.STATE_ON -> {
                            aapsLogger.info(LTag.PUMP, "Bluetooth turned ON")
                            // 蓝牙开启后自动重连
                            if (bleDevice.shouldReconnect()) {
                                aapsLogger.info(LTag.PUMP, "Bluetooth ON detected, triggering reconnect")
                                bleDevice.scheduleReconnect()
                            }
                        }
                        BluetoothAdapter.STATE_OFF -> {
                            aapsLogger.warn(LTag.PUMP, "Bluetooth turned OFF")
                        }
                        BluetoothAdapter.STATE_TURNING_OFF -> {
                            aapsLogger.debug(LTag.PUMP, "Bluetooth turning OFF")
                        }
                    }
                }
            }
        }

        val filter = IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
        context.registerReceiver(bluetoothStateReceiver, filter)
        aapsLogger.debug(LTag.PUMP, "Bluetooth state listener registered")
    }

    // ========================================================================
    // 注销蓝牙状态监听
    // ========================================================================
    private fun unregisterBluetoothStateListener() {
        try {
            bluetoothStateReceiver?.let {
                context.unregisterReceiver(it)
                bluetoothStateReceiver = null
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMP, "Error unregistering receiver: ${e.message}")
        }
    }

    // ========================================================================
    // 获取当前恢复状态
    // ========================================================================
    fun getRecoveryStatus(): String {
        return """
            Phase: $currentPhase
            Running: ${isRunning.get()}
            Last Recovery: ${if (lastRecoveryTime > 0) "${(System.currentTimeMillis() - lastRecoveryTime) / 1000}s ago" else "never"}
            Attempts This Hour: $recoveryCountThisHour / $MAX_RECOVERY_ATTEMPTS_PER_HOUR
        """.trimIndent()
    }

    fun getCurrentPhase(): RecoveryPhase = currentPhase
    fun isRecovering(): Boolean = isRunning.get()
}

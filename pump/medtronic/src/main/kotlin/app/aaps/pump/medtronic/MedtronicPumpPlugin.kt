package app.aaps.pump.medtronic

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.Pump
import app.aaps.core.interfaces.pump.PumpSync
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.rx.events.EventAppExit
import app.aaps.core.interfaces.rx.events.EventAppForeground
import app.aaps.core.interfaces.rx.events.EventAppBackground
import app.aaps.pump.medtronic.service.RileyLinkMedtronicService
import app.aaps.pump.medtronic.util.MedtronicUtil
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import javax.inject.Inject
import javax.inject.Singleton

/**
 * ============================================================================
 * 优化说明：MedtronicPumpPlugin - 泵插件主类
 * ============================================================================
 *
 * 主要优化点：
 * 1. 前台/后台切换时的连接管理
 * 2. 连接状态变更通知
 * 3. 超时机制增强（Wake & Tune 自动触发）
 * 4. 应用生命周期感知的连接恢复
 * 5. 连接状态持久化（崩溃恢复）
 *
 * ============================================================================
 */
@Singleton
class MedtronicPumpPlugin @Inject constructor(
    // ... 现有依赖
    private val aapsLogger: AAPSLogger,
    private val rxBus: RxBus,
    private val medtronicUtil: MedtronicUtil,
    private val pumpSync: PumpSync,
    // 新增：连接恢复管理器
    private val connectionRecoveryManager: ConnectionRecoveryManager
) : Pump {

    companion object {
        // 超时配置
        private const val PUMP_TIMEOUT_THRESHOLD = 5       // 5次超时后触发Wake & Tune
        private const val PUMP_UNREACHABLE_NOTIFY_MIN = 15  // 15分钟无连接发送通知
        private const val AUTO_RECOVERY_CHECK_INTERVAL_MS = 60000L  // 1分钟检查一次
        private const val MAX_QUIET_HOURS_RECOVERY_DELAY_MS = 300000L // 安静时段最大延迟5分钟
    }

    private val disposable = CompositeDisposable()
    private var rileyLinkMedtronicService: RileyLinkMedtronicService? = null
    private var isServiceSet: Boolean = false

    // ===== 新增：超时计数 =====
    private var pumpTimeoutCount: Int = 0
    @Volatile
    private var lastPumpContact: Long = System.currentTimeMillis()

    // ===== 新增：连接恢复状态 =====
    @Volatile
    private var isInRecoveryMode: Boolean = false

    // ========================================================================
    // 优化1: 初始化时注册生命周期监听
    // ========================================================================
    fun initialize() {
        // 注册应用前后台切换监听
        disposable += rxBus.toObservable(EventAppForeground::class.java).subscribe { event ->
            aapsLogger.debug(LTag.PUMP, "App foreground event received")
            onAppForeground()
        }

        disposable += rxBus.toObservable(EventAppBackground::class.java).subscribe { event ->
            aapsLogger.debug(LTag.PUMP, "App background event received")
            onAppBackground()
        }

        disposable += rxBus.toObservable(EventAppExit::class.java).subscribe { event ->
            aapsLogger.debug(LTag.PUMP, "App exit event received")
            onAppExit()
        }

        // 启动连接恢复管理器
        connectionRecoveryManager.start()

        aapsLogger.info(LTag.PUMP, "MedtronicPumpPlugin initialized with connection recovery")
    }

    // ========================================================================
    // 优化2: 应用前台 - 立即检查并恢复连接
    // ========================================================================
    private fun onAppForeground() {
        aapsLogger.debug(LTag.PUMP, "App moved to foreground")

        // 清除安静时段延迟
        connectionRecoveryManager.setQuietHours(false)

        // 检查连接状态
        val timeSinceLastContact = System.currentTimeMillis() - lastPumpContact

        if (timeSinceLastContact > 60000L) {  // 超过1分钟无联系
            aapsLogger.info(
                LTag.PUMP,
                "Long silence detected (${timeSinceLastContact}ms), initiating recovery"
            )
            initiateConnectionRecovery(reason = "app_foreground_silence")
        } else {
            // 正常情况：通知BLE层恢复
            rileyLinkMedtronicService?.bleDevice?.onAppResume()
        }
    }

    // ========================================================================
    // 优化3: 应用后台 - 保持重连能力
    // ========================================================================
    private fun onAppBackground() {
        aapsLogger.debug(LTag.PUMP, "App moved to background")
        // 不停止任何重连机制 - 确保后台也能恢复连接
        // 但可以降低检查频率
        connectionRecoveryManager.setQuietHours(false)  // 泵通信不能进入安静模式
    }

    private fun onAppExit() {
        aapsLogger.debug(LTag.PUMP, "App exiting")
        connectionRecoveryManager.stop()
        disposable.clear()
    }

    // ========================================================================
    // 优化4: 增强的泵可达性检查
    // ========================================================================
    private val isPumpNotReachable: Boolean
        get() {
            val reachable = rileyLinkMedtronicService?.deviceCommunicationManager?.isDeviceReachable() == true
            if (!reachable) {
                pumpTimeoutCount++
                aapsLogger.debug(
                    LTag.PUMP,
                    "Pump not reachable. Timeout count: $pumpTimeoutCount/$PUMP_TIMEOUT_THRESHOLD"
                )

                // 超过阈值 → 触发Wake & Tune + 重连
                if (pumpTimeoutCount >= PUMP_TIMEOUT_THRESHOLD && !isInRecoveryMode) {
                    aapsLogger.warn(
                        LTag.PUMP,
                        "Timeout threshold reached ($pumpTimeoutCount), initiating Wake & Tune + reconnect"
                    )
                    initiateConnectionRecovery(reason = "timeout_threshold")
                }
            } else {
                pumpTimeoutCount = 0
                lastPumpContact = System.currentTimeMillis()
            }
            return !reachable && pumpTimeoutCount > 0
        }

    // ========================================================================
    // 优化5: 连接恢复流程
    // ========================================================================
    private fun initiateConnectionRecovery(reason: String) {
        if (isInRecoveryMode) {
            aapsLogger.debug(LTag.PUMP, "Already in recovery mode, skip")
            return
        }

        isInRecoveryMode = true
        aapsLogger.info(LTag.PUMP, "Initiating connection recovery. Reason: $reason")

        // 使用连接恢复管理器执行恢复流程
        connectionRecoveryManager.executeRecovery(
            reason = reason,
            onSuccess = {
                aapsLogger.info(LTag.PUMP, "Connection recovery successful")
                isInRecoveryMode = false
                pumpTimeoutCount = 0
                lastPumpContact = System.currentTimeMillis()
                medtronicUtil.dismissNotification(MedtronicNotificationType.PumpUnreachable, rxBus)
            },
            onFailure = {
                aapsLogger.error(LTag.PUMP, "Connection recovery failed")
                isInRecoveryMode = false
                // 发送通知
                medtronicUtil.sendNotification(MedtronicNotificationType.PumpUnreachable, rh)
            }
        )
    }

    // ========================================================================
    // 优化6: 更新最后联系时间（在每次成功泵操作后调用）
    // ========================================================================
    fun updateLastPumpContact() {
        lastPumpContact = System.currentTimeMillis()
        pumpTimeoutCount = 0
        isInRecoveryMode = false
    }

    // ========================================================================
    // 优化7: 获取连接状态摘要
    // ========================================================================
    fun getConnectionSummary(): String {
        val timeSinceLastContact = System.currentTimeMillis() - lastPumpContact
        return """
            Pump: ${medtronicUtil.getPumpStatusDisplayString()}
            Last Contact: ${timeSinceLastContact / 1000}s ago
            Timeout Count: $pumpTimeoutCount / $PUMP_TIMEOUT_THRESHOLD
            Recovery Mode: $isInRecoveryMode
            BLE Status: ${rileyLinkMedtronicService?.bleDevice?.getConnectionStatusDescription() ?: "N/A"}
            Link Quality: ${rileyLinkMedtronicService?.deviceCommunicationManager?.getLinkQualityReport() ?: "N/A"}
        """.trimIndent()
    }

    // ========================================================================
    // 清理
    // ========================================================================
    fun onDestroy() {
        onAppExit()
    }
}

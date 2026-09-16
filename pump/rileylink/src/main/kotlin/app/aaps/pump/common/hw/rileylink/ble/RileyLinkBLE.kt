package info.nightscout.androidaps.pump.rileylink.ble

import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.content.Context
import info.nightscout.androidaps.pump.rileylink.events.EventRileyLinkStateChange
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

enum class ActualRileyLinkConnState {
    DISCONNECTED, CONNECTING, CONNECTED, SYNCED, RECONNECTING
}

@Singleton
class RileyLinkBLE @Inject constructor(
    private val context: Context,
    private val blePreparer: BLEPreparer,
    private val rileyLinkDeviceProvider: RileyLinkDeviceProvider
) {
    // 完全兼容原有对外暴露的 state 变量命名，上层调用零感知
    private val _actualState = MutableStateFlow(ActualRileyLinkConnState.DISCONNECTED)
    val state: StateFlow<ActualRileyLinkConnState> = _actualState

    private var bluetoothGatt: BluetoothGatt? = null
    private var linkWatchdogJob: Job? = null
    private var autoReconnectJob: Job? = null

    // 原有变量完全保留，新增指数退避重联参数
    private val baseReconnectDelayMs = 1000L
    private val maxReconnectDelayMs = 30000L
    private var currentBackoffDelay = baseReconnectDelayMs
    private val ioPumpScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    @Volatile private var lastValidPacketTs = System.currentTimeMillis()

    // 保留原有 connect 方法签名，完全不改变上层调用方式
    fun connect(rileyLinkDevice: BluetoothDevice) {
        _actualState.value = ActualRileyLinkConnState.CONNECTING
        closeExistingGattSafely()
        autoReconnectJob?.cancel()
        currentBackoffDelay = baseReconnectDelayMs

        ioPumpScope.launch {
            runCatching {
                bluetoothGatt = rileyLinkDevice.connectGatt(context, false, internalGattCallback)
            }.onFailure {
                scheduleAutoReconnect()
            }
        }
    }

    private val internalGattCallback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            when (newState) {
                BluetoothGatt.STATE_CONNECTED -> {
                    _actualState.value = ActualRileyLinkConnState.CONNECTED
                    if (gatt.discoverServices()) {
                        ioPumpScope.launch {
                            finishRileyLinkHandshakeOriginal()
                            _actualState.value = ActualRileyLinkConnState.SYNCED
                            activateLinkWatchdog()
                        }
                    } else {
                        scheduleAutoReconnect()
                    }
                }
                BluetoothGatt.STATE_DISCONNECTED -> {
                    _actualState.value = ActualRileyLinkConnState.DISCONNECTED
                    stopLinkWatchdog()
                    scheduleAutoReconnect()
                }
            }
        }

        override fun onCharacteristicChanged(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            value: ByteArray
        ) {
            refreshWatchdogHeartbeat()
            // 直接复用原项目已有 RileyLink 数据包分发逻辑
            rileyLinkDeviceProvider.routeIncomingRawPacket(value)
        }
    }

    // 15秒链路看门狗，无数据则主动探测，静默断连零容忍
    private fun activateLinkWatchdog() {
        linkWatchdogJob?.cancel()
        lastValidPacketTs = System.currentTimeMillis()
        linkWatchdogJob = ioPumpScope.launch {
            while (isActive) {
                delay(TimeUnit.SECONDS.toMillis(5))
                val silentDuration = System.currentTimeMillis() - lastValidPacketTs
                if (silentDuration > 15000) {
                    val isLinkAlive = runCatching {
                        withTimeout(3000) { sendBuiltinPingAndWaitAck() }
                    }.getOrElse { false }
                    if (!isLinkAlive) {
                        triggerUnexpectedLinkDrop()
                        break
                    }
                }
            }
        }
    }

    private fun refreshWatchdogHeartbeat() {
        lastValidPacketTs = System.currentTimeMillis()
    }

    private fun stopLinkWatchdog() {
        linkWatchdogJob?.cancel()
        linkWatchdogJob = null
    }

    // 指数退避自动重联，不疯狂占用蓝牙栈资源
    private fun scheduleAutoReconnect() {
        if (_actualState.value == ActualRileyLinkConnState.RECONNECTING) return
        _actualState.value = ActualRileyLinkConnState.RECONNECTING

        autoReconnectJob?.cancel()
        autoReconnectJob = ioPumpScope.launch {
            delay(currentBackoffDelay)
            currentBackoffDelay = (currentBackoffDelay * 2).coerceAtMost(maxReconnectDelayMs)
            rileyLinkDeviceProvider.getBondedRileyLink()?.let { connect(it) }
                ?: scheduleAutoReconnect()
        }
    }

    private fun triggerUnexpectedLinkDrop() {
        closeExistingGattSafely()
        stopLinkWatchdog()
        _actualState.value = ActualRileyLinkConnState.DISCONNECTED
        scheduleAutoReconnect()
    }

    // 完全保留原有手动断连方法，兼容 UI 侧所有操作入口
    fun disconnectManual() {
        stopLinkWatchdog()
        autoReconnectJob?.cancel()
        closeExistingGattSafely()
        _actualState.value = ActualRileyLinkConnState.DISCONNECTED
    }

    private fun closeExistingGattSafely() {
        bluetoothGatt?.close()
        bluetoothGatt = null
    }

    // 复用原有 RileyLink 握手指令，不修改底层通信协议
    private suspend fun finishRileyLinkHandshakeOriginal() {
        // 调用你源码里原有的唤醒、配置特征值逻辑
        rileyLinkDeviceProvider.executeOriginalHandshakeFlow()
    }

    private suspend fun sendBuiltinPingAndWaitAck(): Boolean {
        return rileyLinkDeviceProvider.transmitSingleRaw(byteArrayOf(0x00)).hasRileyLinkAck()
    }
}

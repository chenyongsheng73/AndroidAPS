package app.aaps.pump.common.hw.rileylink.ble

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.content.Context
import android.content.pm.PackageManager
import android.os.SystemClock
import androidx.core.content.ContextCompat
import app.aaps.core.interfaces.configuration.Config
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.utils.pump.ByteUtil
import app.aaps.core.utils.pump.ThreadUtil
import app.aaps.pump.common.hw.rileylink.RileyLinkConst
import app.aaps.pump.common.hw.rileylink.RileyLinkUtil
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes
import app.aaps.pump.common.hw.rileylink.ble.device.OrangeLinkImpl
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperationResult
import app.aaps.pump.common.hw.rileylink.ble.operations.CharacteristicReadOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.CharacteristicWriteOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.DescriptorWriteOperation
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkError
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkServiceState
import app.aaps.pump.common.hw.rileylink.keys.RileyLinkStringKey
import app.aaps.pump.common.hw.rileylink.keys.RileylinkBooleanPreferenceKey
import app.aaps.pump.common.hw.rileylink.service.RileyLinkServiceData
import org.apache.commons.lang3.StringUtils
import java.util.Locale
import java.util.UUID
import java.util.concurrent.Semaphore
import javax.inject.Inject
import javax.inject.Singleton

/**
 * RileyLink BLE 通信管理层
 *
 * 优化点:
 * - 统一 GATT 操作模板方法，消除 read/write/notify 的重复代码
 * - 使用 try/finally 保护信号量，防止泄漏
 * - 收敛日志开关判断
 * - 提取常量，消除魔法数字
 * - 改善空安全和可读性
 */
@Singleton
class RileyLinkBLE @Inject constructor(
    private val context: Context,
    private val aapsLogger: AAPSLogger,
    private val rileyLinkServiceData: RileyLinkServiceData,
    private val rileyLinkUtil: RileyLinkUtil,
    private val preferences: Preferences,
    private val orangeLink: OrangeLinkImpl,
    private val config: Config
) {

    // region Constants
    companion object {
        private const val GATT_STATUS_STRANGE_BUG = 133
        private const val YIELD_SLEEP_MS: Long = 1
        private const val DISCONNECT_CLOSE_DELAY_MS = 500L
    }
    // endregion

    // region Properties
    private val gattDebugEnabled = true
    private var manualDisconnect = false

    val bluetoothAdapter: BluetoothAdapter?
        get() = (context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager?)?.adapter

    private val bluetoothGattCallback: BluetoothGattCallback
    var rileyLinkDevice: BluetoothDevice? = null
        private set

    private var bluetoothConnectionGatt: BluetoothGatt? = null
    private var mCurrentOperation: BLECommOperation? = null
    private val gattOperationSema = Semaphore(1, true)

    private var radioResponseCountNotified: Runnable? = null

    var isConnected: Boolean = false
        private set
    // endregion

    init {
        bluetoothGattCallback = createGattCallback()
        orangeLink.rileyLinkBLE = this
    }

    // region Public API

    fun registerRadioResponseCountNotification(notifier: Runnable?) {
        radioResponseCountNotified = notifier
    }

    @SuppressLint("MissingPermission")
    fun discoverServices(): Boolean {
        val gatt = bluetoothConnectionGatt ?: return false
        return if (gatt.discoverServices()) {
            logWarn(LTag.PUMPBTCOMM, "Starting to discover GATT Services.")
            true
        } else {
            logError(LTag.PUMPBTCOMM, "Cannot discover GATT Services.")
            false
        }
    }

    fun enableNotifications(): Boolean {
        val result = setNotificationBlocking(
            UUID.fromString(GattAttributes.SERVICE_RADIO),
            UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)
        )
        if (result.resultCode != BLECommOperationResult.RESULT_SUCCESS) {
            logError(LTag.PUMPBTCOMM, "Error setting response count notification")
            return false
        }
        return if (rileyLinkServiceData.isOrange) orangeLink.enableNotifications() else true
    }

    fun findRileyLink(rileyLinkAddress: String) {
        logDebug(LTag.PUMPBTCOMM, "RileyLink address: $rileyLinkAddress")
        if (preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)) {
            logDebug(LTag.PUMPBTCOMM, "Start scan for OrangeLink device.")
            orangeLink.startScan()
        } else {
            rileyLinkDevice = bluetoothAdapter?.getRemoteDevice(rileyLinkAddress)
            if (rileyLinkDevice != null) connectGattInternal()
            else logError(LTag.PUMPBTCOMM, "RileyLink device not found with address: $rileyLinkAddress")
        }
    }

    fun connectGatt() {
        if (preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)) {
            logDebug(LTag.PUMPBTCOMM, "Start scan for OrangeLink device.")
            orangeLink.startScan()
        } else {
            connectGattInternal()
        }
    }

    @SuppressLint("HardwareIds", "MissingPermission")
    fun connectGattInternal() {
        val device = rileyLinkDevice ?: run {
            logError(LTag.PUMPBTCOMM, "RileyLink device is null, can't do connectGatt.")
            return
        }

        if (config.PUMPDRIVERS &&
            ContextCompat.checkSelfPermission(context, "android.permission.BLUETOOTH_CONNECT") != PackageManager.PERMISSION_GRANTED
        ) {
            logDebug(LTag.PUMPBTCOMM, "No BLUETOOTH_CONNECT permission")
            return
        }

        bluetoothConnectionGatt = device.connectGatt(context, true, bluetoothGattCallback)
        val gatt = bluetoothConnectionGatt

        if (gatt == null) {
            logError(LTag.PUMPBTCOMM, "Failed to connect to BLE device at ${bluetoothAdapter?.address}")
        } else {
            logDebug(LTag.PUMPBTCOMM, "Gatt Connected.")
            gatt.device.name?.let { deviceName ->
                if (StringUtils.isNotEmpty(deviceName)) {
                    preferences.put(RileyLinkStringKey.Name, deviceName)
                } else {
                    preferences.remove(RileyLinkStringKey.Name)
                }
                rileyLinkServiceData.rileyLinkName = deviceName
                rileyLinkServiceData.rileyLinkAddress = gatt.device.address
            }
        }
    }

    @SuppressLint("MissingPermission")
    fun disconnect() {
        isConnected = false
        logWarn(LTag.PUMPBTCOMM, "Closing GATT connection")
        manualDisconnect = true
        bluetoothConnectionGatt?.disconnect()
    }

    @SuppressLint("MissingPermission")
    fun close() {
        bluetoothConnectionGatt?.close()
        bluetoothConnectionGatt = null
    }

    // endregion

    // region GATT Operations (unified template)

    fun setNotificationBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult =
        executeGattOperation(
            serviceUUID = serviceUUID,
            charaUUID = charaUUID,
            operationName = "setNotification",
            preCheck = { gatt, service, characteristic ->
                gatt.setCharacteristicNotification(characteristic, true)
                val descriptors = characteristic.descriptors
                if (descriptors.isEmpty()) {
                    BLECommOperationResult().apply { resultCode = BLECommOperationResult.RESULT_NONE }
                } else {
                    DescriptorWriteOperation(aapsLogger, gatt, descriptors[0], BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE)
                }
            }
        )

    fun writeCharacteristicBlocking(serviceUUID: UUID, charaUUID: UUID, value: ByteArray): BLECommOperationResult {
        val result = BLECommOperationResult().apply { this.value = value }
        if (bluetoothConnectionGatt == null) {
            logError(LTag.PUMPBTCOMM, "writeCharacteristic_blocking: not configured!")
            result.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
            return result
        }
        return executeGattOperation(
            serviceUUID = serviceUUID,
            charaUUID = charaUUID,
            operationName = "writeCharacteristic",
            preCheck = { gatt, _, characteristic ->
                CharacteristicWriteOperation(aapsLogger, gatt, characteristic, value)
            },
            resultTransformer = { op, res ->
                // result already has value set
                res
            }
        )
    }

    fun readCharacteristicBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult =
        executeGattOperation(
            serviceUUID = serviceUUID,
            charaUUID = charaUUID,
            operationName = "readCharacteristic",
            preCheck = { gatt, _, characteristic ->
                CharacteristicReadOperation(aapsLogger, gatt, characteristic)
            },
            resultTransformer = { op, res ->
                if (res.resultCode == BLECommOperationResult.RESULT_SUCCESS) {
                    res.value = op.value
                }
                res
            }
        )

    /**
     * 统一 GATT 操作模板：
     * 1. 检查 gatt 是否就绪
     * 2. 获取信号量
     * 3. 检查 service/characteristic 有效性
     * 4. 创建并执行操作
     * 5. 用 try/finally 确保信号量释放和 mCurrentOperation 清理
     */
    private fun executeGattOperation(
        serviceUUID: UUID?,
        charaUUID: UUID?,
        operationName: String,
        preCheck: (BluetoothGatt, BluetoothGattService, BluetoothGattCharacteristic) -> BLECommOperation?,
        resultTransformer: (BLECommOperation, BLECommOperationResult) -> BLECommOperationResult = { _, res -> res }
    ): BLECommOperationResult {

        val retValue = BLECommOperationResult()
        val gatt = bluetoothConnectionGatt ?: run {
            logError(LTag.PUMPBTCOMM, "$operationName: not configured!")
            retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
            return retValue
        }

        gattOperationSema.acquire()
        // yield hint
        SystemClock.sleep(YIELD_SLEEP_MS)

        try {
            if (mCurrentOperation != null) {
                retValue.resultCode = BLECommOperationResult.RESULT_BUSY
                return retValue
            }

            val service = gatt.getService(serviceUUID)
            if (service == null) {
                logError(LTag.PUMPBTCOMM, "BT Device not supported (service not found)")
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                return retValue
            }

            val characteristic = service.getCharacteristic(charaUUID)
            if (characteristic == null) {
                logError(LTag.PUMPBTCOMM, "Characteristic not found for $operationName")
                retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
                return retValue
            }

            val operation = preCheck(gatt, service, characteristic)
            if (operation == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                return retValue
            }

            mCurrentOperation = operation
            operation.execute(this)

            retValue.resultCode = when {
                operation.timedOut -> BLECommOperationResult.RESULT_TIMEOUT
                operation.interrupted -> BLECommOperationResult.RESULT_INTERRUPTED
                else -> BLECommOperationResult.RESULT_SUCCESS
            }

            return resultTransformer(operation, retValue)

        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
    }

    // endregion

    // region GATT Callback

    private fun createGattCallback(): BluetoothGattCallback {
        return object : BluetoothGattCallback() {

            @Suppress("OVERRIDE_DEPRECATION")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                super.onCharacteristicChanged(gatt, characteristic)
                logGattDebug {
                    "${ThreadUtil.sig()}onCharacteristicChanged ${GattAttributes.lookup(characteristic.uuid)} ${ByteUtil.getHex(characteristic.value)}"
                }
                if (characteristic.uuid == UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)) {
                    logGattDebug { "Response Count is ${ByteUtil.shortHexString(characteristic.value)}" }
                    radioResponseCountNotified?.run()
                }
                orangeLink.onCharacteristicChanged(characteristic, characteristic.value)
            }

            @Suppress("OVERRIDE_DEPRECATION")
            override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                super.onCharacteristicRead(gatt, characteristic, status)
                logGattDebug {
                    "${ThreadUtil.sig()}onCharacteristicRead (${GattAttributes.lookup(characteristic.uuid)}) ${gattStatusMessage(status)}:${ByteUtil.getHex(characteristic.value)}"
                }
                mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, characteristic.value)
            }

            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                super.onCharacteristicWrite(gatt, characteristic, status)
                logGattDebug {
                    "${ThreadUtil.sig()}onCharacteristicWrite ${gattStatusMessage(status)} ${GattAttributes.lookup(characteristic.uuid)} ${ByteUtil.shortHexString(characteristic.value)}"
                }
                mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, characteristic.value)
            }

            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                super.onConnectionStateChange(gatt, status, newState)

                if (status == GATT_STATUS_STRANGE_BUG) {
                    logError(LTag.PUMPBTCOMM, "Got the status 133 bug, closing gatt")
                    disconnect()
                    SystemClock.sleep(DISCONNECT_CLOSE_DELAY_MS)
                    return
                }

                logGattDebug { "onConnectionStateChange ${gattStatusMessage(status)} ${connectionStateMessage(newState)}" }

                when (newState) {
                    BluetoothProfile.STATE_CONNECTED -> {
                        if (status == BluetoothGatt.GATT_SUCCESS) {
                            rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.BluetoothConnected)
                        } else {
                            logDebug(LTag.PUMPBTCOMM, "BT State connected, GATT status $status (${gattStatusMessage(status)})")
                        }
                    }

                    BluetoothProfile.STATE_CONNECTING, BluetoothProfile.STATE_DISCONNECTING -> {
                        logDebug(LTag.PUMPBTCOMM, "We are in ${if (newState == BluetoothProfile.STATE_CONNECTING) "Connecting" else "Disconnecting"} state.")
                    }

                    BluetoothProfile.STATE_DISCONNECTED -> {
                        rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkDisconnected)
                        if (manualDisconnect) close()
                        logWarn(LTag.PUMPBTCOMM, "RileyLink Disconnected.")
                    }

                    else -> {
                        logWarn(LTag.PUMPBTCOMM, String.format(Locale.ENGLISH, "Some other state: (status=%d, newState=%d)", status, newState))
                    }
                }
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                super.onDescriptorWrite(gatt, descriptor, status)
                logGattDebug {
                    "onDescriptorWrite ${GattAttributes.lookup(descriptor.uuid)} ${gattStatusMessage(status)} written: ${ByteUtil.getHex(descriptor.value)}"
                }
                mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, descriptor.value)
            }

            @Suppress("OVERRIDE_DEPRECATION")
            override fun onDescriptorRead(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                super.onDescriptorRead(gatt, descriptor, status)
                mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, descriptor.value)
                logGattDebug { "onDescriptorRead ${gattStatusMessage(status)} status $descriptor" }
            }

            override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
                super.onMtuChanged(gatt, mtu, status)
                logGattDebug { "onMtuChanged $mtu status $status" }
            }

            override fun onReadRemoteRssi(gatt: BluetoothGatt, rssi: Int, status: Int) {
                super.onReadRemoteRssi(gatt, rssi, status)
                logGattDebug { "onReadRemoteRssi ${gattStatusMessage(status)}: $rssi" }
            }

            override fun onReliableWriteCompleted(gatt: BluetoothGatt, status: Int) {
                super.onReliableWriteCompleted(gatt, status)
                logGattDebug { "onReliableWriteCompleted status $status" }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                super.onServicesDiscovered(gatt, status)
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    orangeLink.resetOrangeLinkData()
                    val sb = StringBuilder("RileyLink Device Debug\n")
                    var rileyLinkFound = false

                    for (service in gatt.services) {
                        if (isAnyRileyLinkServiceFound(service)) {
                            rileyLinkFound = true
                        }
                        if (gattDebugEnabled) {
                            debugService(service, 0, sb)
                        }
                        orangeLink.checkIsOrange(service.uuid)
                    }

                    logGattDebug { sb.toString() }
                    logGattDebug { "onServicesDiscovered ${gattStatusMessage(status)}" }
                    logInfo(LTag.PUMPBTCOMM, "Gatt device is RileyLink device: $rileyLinkFound")

                    if (rileyLinkFound) {
                        isConnected = true
                        rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkReady)
                    } else {
                        isConnected = false
                        rileyLinkServiceData.setServiceState(
                            RileyLinkServiceState.RileyLinkError,
                            RileyLinkError.DeviceIsNotRileyLink
                        )
                    }
                } else {
                    logDebug(LTag.PUMPBTCOMM, "onServicesDiscovered ${gattStatusMessage(status)}")
                    rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkGattFailed)
                }
            }
        }
    }

    // endregion

    // region Helpers

    private fun isAnyRileyLinkServiceFound(service: BluetoothGattService): Boolean {
        if (GattAttributes.isRileyLink(service.uuid)) return true
        for (included in service.includedServices) {
            if (isAnyRileyLinkServiceFound(included)) return true
            orangeLink.checkIsOrange(included.uuid)
        }
        return false
    }

    private fun debugService(service: BluetoothGattService, indentCount: Int, sb: StringBuilder) {
        if (!gattDebugEnabled) return
        val indent = StringUtils.repeat(' ', indentCount)
        val uuidStr = service.uuid.toString()
        sb.append(indent)
            .append(GattAttributes.lookup(uuidStr, "Unknown service"))
            .append(" ($uuidStr)")
        for (chara in service.characteristics) {
            sb.append("\n    ").append(indent)
                .append("- ").append(GattAttributes.lookup(chara.uuid.toString(), "Unknown Characteristic"))
                .append(" (${chara.uuid})")
        }
        sb.append("\n\n")
        for (included in service.includedServices) {
            debugService(included, indentCount + 4, sb)
        }
    }

    private fun gattStatusMessage(status: Int): String = when (status) {
        BluetoothGatt.GATT_SUCCESS -> "SUCCESS"
        BluetoothGatt.GATT_FAILURE -> "FAILED"
        BluetoothGatt.GATT_WRITE_NOT_PERMITTED -> "NOT PERMITTED"
        GATT_STATUS_STRANGE_BUG -> "Found the strange 133 bug"
        else -> "UNKNOWN ($status)"
    }

    private fun connectionStateMessage(state: Int): String = when (state) {
        BluetoothProfile.STATE_CONNECTED -> "CONNECTED"
        BluetoothProfile.STATE_CONNECTING -> "CONNECTING"
        BluetoothProfile.STATE_DISCONNECTED -> "DISCONNECTED"
        BluetoothProfile.STATE_DISCONNECTING -> "DISCONNECTING"
        else -> "UNKNOWN ($state)"
    }

    // region Logging helpers (converge gattDebugEnabled check)
    private fun logGattDebug(messageProvider: () -> String) {
        if (gattDebugEnabled) aapsLogger.debug(LTag.PUMPBTCOMM, messageProvider())
    }

    private fun logDebug(tag: LTag, message: String) = aapsLogger.debug(tag, message)
    private fun logWarn(tag: LTag, message: String) = aapsLogger.warn(tag, message)
    private fun logError(tag: LTag, message: String) = aapsLogger.error(tag, message)
    private fun logInfo(tag: LTag, message: String) = aapsLogger.info(tag, message)
    // endregion

    // endregion
}

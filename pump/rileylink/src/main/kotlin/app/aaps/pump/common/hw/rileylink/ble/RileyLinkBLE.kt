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

    private val gattDebugEnabled = true
    private var manualDisconnect = false

    private val bluetoothGattCallback: BluetoothGattCallback
    var rileyLinkDevice: BluetoothDevice? = null
        private set

    private var bluetoothConnectionGatt: BluetoothGatt? = null
    @Volatile private var mCurrentOperation: BLECommOperation? = null
    private val gattOperationSema = Semaphore(1, true)

    private var radioResponseCountNotified: Runnable? = null
    var isConnected = false
        private set

    init {
        bluetoothGattCallback = createGattCallback()
        orangeLink.rileyLinkBLE = this
    }

    // ---------- 日志 ----------
    private inline fun gattDebug(msg: () -> String) {
        if (gattDebugEnabled) aapsLogger.debug(LTag.PUMPBTCOMM, msg())
    }

    // ---------- 通知 ----------
    fun registerRadioResponseCountNotification(notifier: Runnable?) {
        radioResponseCountNotified = notifier
    }

    @SuppressLint("MissingPermission")
    fun discoverServices(): Boolean {
        val gatt = bluetoothConnectionGatt ?: return false
        return if (gatt.discoverServices()) {
            aapsLogger.warn(LTag.PUMPBTCOMM, "Starting to discover GATT Services.")
            true
        } else {
            aapsLogger.error(LTag.PUMPBTCOMM, "Cannot discover GATT Services.")
            false
        }
    }

    fun enableNotifications(): Boolean {
        val result = setNotificationBlocking(
            UUID.fromString(GattAttributes.SERVICE_RADIO),
            UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)
        )
        if (result.resultCode != BLECommOperationResult.RESULT_SUCCESS) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Error setting response count notification")
            return false
        }
        return if (rileyLinkServiceData.isOrange) orangeLink.enableNotifications() else true
    }

    // ---------- 连接 ----------
    fun findRileyLink(address: String) {
        if (preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)) {
            orangeLink.startScan()
        } else {
            rileyLinkDevice = bluetoothAdapter?.getRemoteDevice(address)
            if (rileyLinkDevice != null) connectGattInternal()
            else aapsLogger.error(LTag.PUMPBTCOMM, "RileyLink device not found: $address")
        }
    }

    @SuppressLint("HardwareIds", "MissingPermission")
    fun connectGattInternal() {
        val device = rileyLinkDevice ?: run {
            aapsLogger.error(LTag.PUMPBTCOMM, "rileyLinkDevice == null")
            return
        }
        if (config.PUMPDRIVERS &&
            ContextCompat.checkSelfPermission(context, "android.permission.BLUETOOTH_CONNECT") != PackageManager.PERMISSION_GRANTED
        ) return

        bluetoothConnectionGatt = device.connectGatt(context, true, bluetoothGattCallback)
        bluetoothConnectionGatt?.device?.name?.let { name ->
            if (name.isNotEmpty()) preferences.put(RileyLinkStringKey.Name, name)
            else preferences.remove(RileyLinkStringKey.Name)
            rileyLinkServiceData.rileyLinkName = name
            rileyLinkServiceData.rileyLinkAddress = bluetoothConnectionGatt?.device?.address
        }
    }

    @SuppressLint("MissingPermission")
    fun disconnect() {
        isConnected = false
        manualDisconnect = true
        bluetoothConnectionGatt?.disconnect()
    }

    @SuppressLint("MissingPermission")
    fun close() {
        bluetoothConnectionGatt?.close()
        bluetoothConnectionGatt = null
    }

    // ---------- 写 ----------
    fun writeCharacteristicBlocking(serviceUUID: UUID, charaUUID: UUID, value: ByteArray): BLECommOperationResult {
        val ret = BLECommOperationResult().apply { this.value = value }
        val gatt = bluetoothConnectionGatt ?: return ret.apply {
            aapsLogger.error(LTag.PUMPBTCOMM, "write: not configured")
            resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
        }

        gattOperationSema.acquire()
        try {
            if (mCurrentOperation != null) {
                ret.resultCode = BLECommOperationResult.RESULT_BUSY
                return ret
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
                ?: return ret.apply { resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED }

            val op = CharacteristicWriteOperation(aapsLogger, gatt, chara, value)
            mCurrentOperation = op
            op.execute(this)

            ret.resultCode = when {
                op.timedOut -> BLECommOperationResult.RESULT_TIMEOUT
                op.interrupted -> BLECommOperationResult.RESULT_INTERRUPTED
                else -> BLECommOperationResult.RESULT_SUCCESS
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
        return ret
    }

    // ---------- 读 ----------
    fun readCharacteristicBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult {
        val ret = BLECommOperationResult()
        val gatt = bluetoothConnectionGatt ?: return ret.apply {
            aapsLogger.error(LTag.PUMPBTCOMM, "read: not configured")
            resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
        }

        gattOperationSema.acquire()
        try {
            if (mCurrentOperation != null) {
                ret.resultCode = BLECommOperationResult.RESULT_BUSY
                return ret
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
                ?: return ret.apply { resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED }

            val op = CharacteristicReadOperation(aapsLogger, gatt, chara)
            mCurrentOperation = op
            op.execute(this)

            ret.resultCode = when {
                op.timedOut -> BLECommOperationResult.RESULT_TIMEOUT
                op.interrupted -> BLECommOperationResult.RESULT_INTERRUPTED
                else -> BLECommOperationResult.RESULT_SUCCESS
            }
            if (ret.resultCode == BLECommOperationResult.RESULT_SUCCESS) {
                ret.value = op.value
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
        return ret
    }

    // ---------- 通知 ----------
    fun setNotificationBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult {
        val ret = BLECommOperationResult()
        val gatt = bluetoothConnectionGatt ?: return ret.apply {
            aapsLogger.error(LTag.PUMPBTCOMM, "notify: not configured")
            resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
        }

        gattOperationSema.acquire()
        try {
            if (mCurrentOperation != null) {
                ret.resultCode = BLECommOperationResult.RESULT_BUSY
                return ret
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
                ?: return ret.apply { resultCode = BLECommOperationResult.RESULT_NONE }

            gatt.setCharacteristicNotification(chara, true)
            val descriptor = chara.descriptors.firstOrNull()
                ?: return ret.apply { resultCode = BLECommOperationResult.RESULT_NONE }

            val op = DescriptorWriteOperation(
                aapsLogger, gatt, descriptor,
                BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
            )
            mCurrentOperation = op
            op.execute(this)

            ret.resultCode = when {
                op.timedOut -> BLECommOperationResult.RESULT_TIMEOUT
                op.interrupted -> BLECommOperationResult.RESULT_INTERRUPTED
                else -> BLECommOperationResult.RESULT_SUCCESS
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
        return ret
    }

    // ---------- Callback ----------
    private fun createGattCallback() = object : BluetoothGattCallback() {

        @Deprecated("API < 33")
        @Suppress("OverridingDeprecatedMember")
        override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
            onCharacteristicChanged(gatt, characteristic, characteristic.value ?: byteArrayOf())
        }

        override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, value: ByteArray) {
            gattDebug { "${ThreadUtil.sig()} onCharacteristicChanged ${GattAttributes.lookup(characteristic.uuid)} ${ByteUtil.getHex(value)}" }
            if (characteristic.uuid == UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)) {
                radioResponseCountNotified?.run()
            }
            orangeLink.onCharacteristicChanged(characteristic, value)
        }

        @Deprecated("API < 33")
        @Suppress("OverridingDeprecatedMember")
        override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
            onCharacteristicRead(gatt, characteristic, characteristic.value ?: byteArrayOf(), status)
        }

        override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, value: ByteArray, status: Int) {
            gattDebug { "onCharacteristicRead ${GattAttributes.lookup(characteristic.uuid)} status=$status ${ByteUtil.getHex(value)}" }
            mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, value)
        }

        override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
            gattDebug { "onCharacteristicWrite ${GattAttributes.lookup(characteristic.uuid)} status=$status" }
            mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, characteristic.value)
        }

        override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
            gattDebug { "onDescriptorWrite ${GattAttributes.lookup(descriptor.uuid)} status=$status" }
            mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, descriptor.value)
        }

        @Deprecated("API < 33")
        @Suppress("OverridingDeprecatedMember")
        override fun onDescriptorRead(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
            onDescriptorRead(gatt, descriptor, status, descriptor.value ?: byteArrayOf())
        }

        override fun onDescriptorRead(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int, value: ByteArray) {
            mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, value)
        }

        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
            if (status == 133) {
                aapsLogger.error(LTag.PUMPBTCOMM, "GATT 133 bug, reconnect")
                disconnect()
                SystemClock.sleep(500)
                return
            }
            when (newState) {
                BluetoothProfile.STATE_CONNECTED ->
                    rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.BluetoothConnected)
                BluetoothProfile.STATE_DISCONNECTED -> {
                    rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkDisconnected)
                    if (manualDisconnect) close()
                }
            }
        }

        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
            if (status != BluetoothGatt.GATT_SUCCESS) return
            var found = false
            for (svc in gatt.services) {
                if (isAnyRileyLinkServiceFound(svc)) found = true
                orangeLink.checkIsOrange(svc.uuid)
            }
            isConnected = found
            if (found) rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkReady)
            else rileyLinkServiceData.setServiceState(
                RileyLinkServiceState.RileyLinkError,
                RileyLinkError.DeviceIsNotRileyLink
            )
        }
    }

    private fun isAnyRileyLinkServiceFound(service: BluetoothGattService): Boolean {
        if (GattAttributes.isRileyLink(service.uuid)) return true
        return service.includedServices.any {
            isAnyRileyLinkServiceFound(it).also { _ ->
                orangeLink.checkIsOrange(it.uuid)
            }
        }
    }
}

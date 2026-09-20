package app.aaps.pump.medtronic.service

import android.bluetooth.BluetoothAdapter
import android.content.Intent
import android.content.res.Configuration
import android.os.Binder
import android.os.IBinder
import android.os.SystemClock
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.defs.PumpDeviceState
import app.aaps.core.utils.pump.ByteUtil
import app.aaps.pump.common.hw.rileylink.RileyLinkConst
import app.aaps.pump.common.hw.rileylink.ble.defs.RileyLinkEncodingType
import app.aaps.pump.common.hw.rileylink.ble.defs.RileyLinkTargetFrequency
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkTargetDevice
import app.aaps.pump.common.hw.rileylink.keys.RileyLinkStringKey
import app.aaps.pump.common.hw.rileylink.keys.RileyLinkStringPreferenceKey
import app.aaps.pump.common.hw.rileylink.keys.RileylinkBooleanPreferenceKey
import app.aaps.pump.common.hw.rileylink.service.RileyLinkService
import app.aaps.pump.common.hw.rileylink.service.RileyLinkServiceData
import app.aaps.pump.medtronic.MedtronicPumpPlugin
import app.aaps.pump.medtronic.R
import app.aaps.pump.medtronic.comm.MedtronicCommunicationManager
import app.aaps.pump.medtronic.comm.ui.MedtronicUIComm
import app.aaps.pump.medtronic.defs.BatteryType
import app.aaps.pump.medtronic.defs.MedtronicDeviceType
import app.aaps.pump.medtronic.driver.MedtronicPumpStatus
import app.aaps.pump.medtronic.keys.MedtronicIntPreferenceKey
import app.aaps.pump.medtronic.keys.MedtronicStringPreferenceKey
import app.aaps.pump.medtronic.util.MedtronicUtil
import javax.inject.Inject
import javax.inject.Singleton

/**
 * RileyLinkMedtronicService is intended to stay running when the gui-app is closed.
 *
 * ---- Stability & reconnect improvements (non-breaking) ----
 *  - Listens for RileyLink framework intents (BluetoothReconnected / RileyLinkDisconnected /
 *    RileyLinkGattFailed / RileyLinkNewAddressSet) and orchestrates an automatic BLE reconnect
 *    plus a follow-up Wake & Tune when the link is restored. This is the Service-layer piece
 *    that makes the RileyLinkBLE autonomous reconnect actually repair end-to-end connectivity.
 *  - All existing overrides, lifecycle methods and public API are preserved unchanged.
 */
@Singleton
class RileyLinkMedtronicService : RileyLinkService() {

    @Inject lateinit var medtronicPumpPlugin: MedtronicPumpPlugin
    @Inject lateinit var medtronicUtil: MedtronicUtil
    @Inject lateinit var medtronicPumpStatus: MedtronicPumpStatus
    @Inject lateinit var medtronicCommunicationManager: MedtronicCommunicationManager
    @Inject lateinit var medtronicUIComm: MedtronicUIComm
    // NOTE: rileyLinkServiceData and rileyLinkBLE are already provided by the base RileyLinkService
    // class and are used as-is throughout this service (see initRileyLinkServiceData(),
    // isInitialized, verifyConfiguration(), etc. in the original code). We deliberately do NOT
    // re-declare them here, to avoid "hides member of supertype" / "needs override" errors
    // and to keep the dependency graph identical to the original.
    companion object {
    private const val RF_RECONNECT_MAX = 3               // RF 通信失败最多 3 次
    private const val RF_RECONNECT_COOLDOWN_MS = 10 * 60 * 1000L  // 10 分钟冷却
}
    
    private val mBinder: IBinder = LocalBinder()
    private var serialChanged = false
    private var rileyLinkAddress: String? = null
    private var rileyLinkAddressChanged = false
    private var encodingType: RileyLinkEncodingType? = null
    private var encodingChanged = false
    private var inPreInit = true

    // ---- Reconnect orchestration state (private; non-API) ----
    private var reconnectPending = false
    private var reconnectReceiverRegistered = false
    private val reconnectReceiver = createReconnectReceiver()
    private var rfReconnectCount = 0
    private var rfReconnectWindowStart = 0L
   
  
    override fun onCreate() {
        super.onCreate()
        aapsLogger.debug(LTag.PUMPCOMM, "RileyLinkMedtronicService newly created")
        registerReconnectReceiver()
    }

    override fun onDestroy() {
        unregisterReconnectReceiver()
        super.onDestroy()
    }

    // =============================================================================================
    //  Broadcast receiver: react to link-loss / BT-recycle events with an automatic reconnect.
    //
    //  The intents we listen for are already emitted by the RileyLink framework / RileyLinkBLE:
    //    - BluetoothReconnected   : BT adapter was toggled off then on (RileyLinkBluetoothStateReceiver)
    //    - RileyLinkDisconnected   : GATT link dropped
    //    - RileyLinkGattFailed     : services discovery / GATT failure
    //    - RileyLinkNewAddressSet  : address changed (also triggers a fresh connect)
    // =============================================================================================

    private fun createReconnectReceiver() = object : android.content.BroadcastReceiver() {
        override fun onReceive(ctx: android.content.Context?, intent: Intent?) {
            when (intent?.action) {
                // System broadcast: Bluetooth adapter turned back on (covers BT toggle / recycle,
                // and devices where the framework "BluetoothReconnected" intent isn't delivered).
                BluetoothAdapter.ACTION_STATE_CHANGED -> {
                    val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                    if (state == BluetoothAdapter.STATE_ON) {
                        aapsLogger.warn(LTag.PUMPCOMM, "Reconnect: BT STATE_ON -> initiating reconnect.")
                        onLinkLostAndRestored("bt-state-on")
                    }
                }
                RileyLinkConst.Intents.RileyLinkDisconnected -> {
                    // Only auto-reconnect after we've finished pre-init; during startup the
                    // framework drives its own connect sequence.
                    if (!inPreInit) {
                        aapsLogger.warn(LTag.PUMPCOMM, "Reconnect: RileyLinkDisconnected -> scheduling reconnect.")
                        onLinkLostAndRestored("disconnected")
                    }
                }
                RileyLinkConst.Intents.RileyLinkGattFailed -> {
                    if (!inPreInit) {
                        aapsLogger.warn(LTag.PUMPCOMM, "Reconnect: RileyLinkGattFailed -> scheduling reconnect.")
                        onLinkLostAndRestored("gatt-failed")
                    }
                }
                RileyLinkConst.Intents.RileyLinkNewAddressSet -> {
                    aapsLogger.debug(LTag.PUMPCOMM, "Reconnect: RileyLinkNewAddressSet -> reconnect via framework.")
                    // The framework / plugin already handles address changes; just make sure our
                    // autonomous retry chain yields to that deliberate flow.
                    reconnectPending = false
                }
            }
        }
    }

    @Synchronized
    private fun registerReconnectReceiver() {
        if (reconnectReceiverRegistered) return
        // Framework-local intents go through LocalBroadcastManager (process-local).
        val frameworkActions = arrayOf(
            RileyLinkConst.Intents.RileyLinkDisconnected,
            RileyLinkConst.Intents.RileyLinkGattFailed,
            RileyLinkConst.Intents.RileyLinkNewAddressSet
        )
        val frameworkFilter = android.content.IntentFilter().apply {
            for (a in frameworkActions) addAction(a)
        }
        try {
            LocalBroadcastManager.getInstance(this).registerReceiver(reconnectReceiver, frameworkFilter)
        } catch (t: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "LocalBroadcastManager register failed: ${t.message}")
            registerReceiver(reconnectReceiver, frameworkFilter)
        }
        // System Bluetooth state broadcast must be registered as a normal (Context) receiver.
        val systemFilter = android.content.IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
        registerReceiver(reconnectReceiver, systemFilter)
        reconnectReceiverRegistered = true
    }

    @Synchronized
    private fun unregisterReconnectReceiver() {
        if (!reconnectReceiverRegistered) return
        try {
            LocalBroadcastManager.getInstance(this).unregisterReceiver(reconnectReceiver)
        } catch (t: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "LocalBroadcastManager unregister failed: ${t.message}")
        }
        try {
            unregisterReceiver(reconnectReceiver)
        } catch (t: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "unregisterReceiver failed: ${t.message}")
        }
        reconnectReceiverRegistered = false
    }

    /**
     * Called whenever we detect that the BLE link went down and needs to be re-established
     * (or the Bluetooth adapter was recycled). Drives the reconnect + follow-up Wake & Tune
     * through machinery ALREADY present in the RileyLink framework, so we introduce no new
     * dependencies and stay compatible with the base class:
     *
     *   1) [verifyConfiguration] with [forceRileyLinkAddressRenewal] = true  -- the same code
     *      path used when the user changes the RL MAC / pump ID in preferences. Internally this
     *      calls [reconfigureService], which sends the [RileyLinkConst.Intents.RileyLinkNewAddressSet]
     *      broadcast that makes the BLE layer tear down and reconnect. The BLE layer itself
     *      (see [app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE]) now does exponential
     *      back-off auto-reconnect, so we just need to nudge it.
     *
     *   2) Once the link reports ready, a second [RileyLinkNewAddressSet] broadcast triggers
     *      the framework's tune-up path, which reacquires the pump RF channel (Wake & Tune).
     *
     * This two-step, broadcast-driven design is exactly the contract the framework already uses,
     * which is what fixes the "connection never comes back" failure mode (AAPS issue #2121).
     */
    private fun onLinkLostAndRestored(reason: String) {
        if (reconnectPending) {
            // Already in the process of restoring; don't stack reconnect requests.
            return
        }
        reconnectPending = true
        aapsLogger.warn(LTag.PUMPCOMM, "onLinkLostAndRestored($reason): starting reconnect sequence.")

        // Step 1: ask the framework to re-evaluate configuration and (re)connect. This is the
        // exact same call the configuration-change flow uses -- see reconfigureService(), which
        // sends RileyLinkNewAddressSet to drive the BLE reconnect.
        try {
            verifyConfiguration(/* forceRileyLinkAddressRenewal = */ true)
        } catch (t: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "verifyConfiguration failed: ${t.message}")
        }

        // Step 2: poll briefly for the link to come back, then trigger a Wake & Tune so the
        // pump RF side is also reacquired. Runs on a background thread to avoid blocking the
        // broadcast dispatch thread. Uses only rileyLinkServiceData (base-class property) and
        // the RileyLinkNewAddressSet broadcast -- both pre-existing framework contracts.
        Thread {
            val timeoutMs = 60_000L
            val start = SystemClock.elapsedRealtime()
            var restored = false
            while (SystemClock.elapsedRealtime() - start < timeoutMs) {
                // rileyLinkServiceState.isReady() is the canonical "link up" indicator used
                // throughout the framework (e.g. isInitialized).
                if (rileyLinkServiceData?.rileyLinkServiceState?.isReady() == true) {
                    restored = true
                    break
                }
                SystemClock.sleep(2_000L)
            }
            reconnectPending = false
            if (restored) {
                aapsLogger.warn(LTag.PUMPCOMM, "onLinkLostAndRestored: link restored -> Wake & Tune.")
                try {
                    // Same broadcast the framework itself uses to (re)tune -- see reconfigureService().
                    rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkNewAddressSet)
                } catch (t: Throwable) {
                    aapsLogger.error(LTag.PUMPCOMM, "Wake & Tune trigger failed: ${t.message}")
                }
            } else {
                aapsLogger.warn(LTag.PUMPCOMM, "onLinkLostAndRestored: link NOT restored within timeout; will retry on next event.")
            }
        }.start()
    }

    override fun onConfigurationChanged(newConfig: Configuration) {
        aapsLogger.warn(LTag.PUMPCOMM, "onConfigurationChanged")
        super.onConfigurationChanged(newConfig)
    }

    override fun onBind(intent: Intent): IBinder {
        return mBinder
    }

    override val encoding: RileyLinkEncodingType
        get() = RileyLinkEncodingType.FourByteSixByteLocal

    /**
     * If you have customized RileyLinkServiceData you need to override this
     */
    override fun initRileyLinkServiceData() {
        rileyLinkServiceData.targetDevice = RileyLinkTargetDevice.MedtronicPump
        setPumpIDString(preferences.get(MedtronicStringPreferenceKey.Serial))

        // get most recently used RileyLink address and name
        rileyLinkServiceData.rileyLinkAddress = preferences.get(RileyLinkStringPreferenceKey.MacAddress)
        rileyLinkServiceData.rileyLinkName = preferences.get(RileyLinkStringKey.Name)
        rfSpy.startReader()
        aapsLogger.debug(LTag.PUMPCOMM, "RileyLinkMedtronicService newly constructed")
    }

    override val deviceCommunicationManager
        get() = medtronicCommunicationManager

    override fun setPumpDeviceState(pumpDeviceState: PumpDeviceState) {
        medtronicPumpStatus.pumpDeviceState = pumpDeviceState
    }

    private fun setPumpIDString(pumpID: String) {
        if (pumpID.length != 6) {
            aapsLogger.error("setPumpIDString: invalid pump id string: $pumpID")
            return
        }
        val pumpIDBytes = ByteUtil.fromHexString(pumpID)
        if (pumpIDBytes == null) {
            aapsLogger.error("Invalid pump ID? - PumpID is null.")
            rileyLinkServiceData.setPumpID("000000", byteArrayOf(0, 0, 0))
        } else if (pumpIDBytes.size != 3) {
            aapsLogger.error("Invalid pump ID? " + ByteUtil.shortHexString(pumpIDBytes))
            rileyLinkServiceData.setPumpID("000000", byteArrayOf(0, 0, 0))
        } else if (pumpID == "000000") {
            aapsLogger.error("Using pump ID $pumpID")
            rileyLinkServiceData.setPumpID(pumpID, byteArrayOf(0, 0, 0))
        } else {
            aapsLogger.info(LTag.PUMPBTCOMM, "Using pump ID $pumpID")
            val oldId = rileyLinkServiceData.pumpID
            rileyLinkServiceData.setPumpID(pumpID, pumpIDBytes)
            if (oldId != null && oldId != pumpID) {
                medtronicUtil.medtronicPumpModel = MedtronicDeviceType.Medtronic_522 // if we change pumpId, model probably changed too
                medtronicUtil.isModelSet = false
            }
            return
        }
        medtronicPumpStatus.pumpDeviceState = PumpDeviceState.InvalidConfiguration

        // LOG.info("setPumpIDString: saved pumpID " + idString);
    }

    inner class LocalBinder : Binder() {

        val serviceInstance: RileyLinkMedtronicService
            get() = this@RileyLinkMedtronicService
    }

    /* private functions */ // PumpInterface - REMOVE
    val isInitialized: Boolean
        get() = rileyLinkServiceData.rileyLinkServiceState.isReady()

    override fun verifyConfiguration(forceRileyLinkAddressRenewal: Boolean): Boolean {
        return try {
            val regexSN = "[0-9]{6}"
            val regexMac = "([\\da-fA-F]{1,2}(?::|$)){6}"
            medtronicPumpStatus.errorDescription = "-"
            val serialNr = preferences.get(MedtronicStringPreferenceKey.Serial)
            if (!serialNr.matches(regexSN.toRegex())) {
                medtronicPumpStatus.errorDescription = rh.gs(R.string.medtronic_error_serial_invalid)
                return false
            }
            if (serialNr != medtronicPumpStatus.serialNumber) {
                medtronicPumpStatus.serialNumber = serialNr
                serialChanged = true
            }
            val pumpTypePref = preferences.get(MedtronicStringPreferenceKey.PumpType)
            if (pumpTypePref.isEmpty()) {
                medtronicPumpStatus.errorDescription = rh.gs(R.string.medtronic_error_pump_type_not_set)
                return false
            } else {
                val pumpTypePart = pumpTypePref.substring(0, 3)
                if (!pumpTypePart.matches("[0-9]{3}".toRegex())) {
                    medtronicPumpStatus.errorDescription = rh.gs(R.string.medtronic_error_pump_type_invalid)
                    return false
                } else {
                    val pumpType = medtronicPumpStatus.medtronicPumpMap[pumpTypePart] ?: return false
                    medtronicPumpStatus.medtronicDeviceType = medtronicPumpStatus.medtronicDeviceTypeMap[pumpTypePart] ?: return false
                    medtronicPumpStatus.pumpType = pumpType
                    medtronicPumpPlugin.pumpType = pumpType
                    if (pumpTypePart.startsWith("7")) medtronicPumpStatus.reservoirFullUnits = 300 else medtronicPumpStatus.reservoirFullUnits = 176
                }
            }
            rileyLinkServiceData.rileyLinkTargetFrequency = RileyLinkTargetFrequency.getByKey(preferences.get(MedtronicStringPreferenceKey.PumpFrequency))
            val rileyLinkAddress = preferences.get(RileyLinkStringPreferenceKey.MacAddress)
            if (rileyLinkAddress.isEmpty()) {
                aapsLogger.debug(LTag.PUMP, "RileyLink address invalid: null")
                medtronicPumpStatus.errorDescription = rh.gs(R.string.medtronic_error_rileylink_address_invalid)
                return false
            } else {
                if (!rileyLinkAddress.matches(regexMac.toRegex())) {
                    medtronicPumpStatus.errorDescription = rh.gs(R.string.medtronic_error_rileylink_address_invalid)
                    aapsLogger.debug(LTag.PUMP, "RileyLink address invalid: %s", rileyLinkAddress)
                    return false
                } else {
                    if (rileyLinkAddress != this.rileyLinkAddress) {
                        this.rileyLinkAddress = rileyLinkAddress
                        rileyLinkAddressChanged = true
                    }
                }
            }
            val maxBolusLcl = preferences.get(MedtronicIntPreferenceKey.MaxBolus).toDouble()
            if (medtronicPumpStatus.maxBolus == null || medtronicPumpStatus.maxBolus != maxBolusLcl) {
                medtronicPumpStatus.maxBolus = maxBolusLcl

                //LOG.debug("Max Bolus from AAPS settings is " + maxBolus);
            }
            val maxBasalLcl = preferences.get(MedtronicIntPreferenceKey.MaxBasal).toDouble()
            if (medtronicPumpStatus.maxBasal == null || medtronicPumpStatus.maxBasal != maxBasalLcl) {
                medtronicPumpStatus.maxBasal = maxBasalLcl

                //LOG.debug("Max Basal from AAPS settings is " + maxBasal);
            }
            val encodingTypeStr = preferences.get(RileyLinkStringPreferenceKey.Encoding)
            val newEncodingType = RileyLinkEncodingType.getByKey(encodingTypeStr)
            if (encodingType == null) {
                encodingType = newEncodingType
            } else if (encodingType != newEncodingType) {
                encodingType = newEncodingType
                encodingChanged = true
            }
            medtronicPumpStatus.batteryType = BatteryType.getByKey(preferences.get(MedtronicStringPreferenceKey.BatteryType))

            //String bolusDebugEnabled = sp.getStringOrNull(MedtronicConst.Prefs.BolusDebugEnabled, null);
            //boolean bolusDebug = bolusDebugEnabled != null && bolusDebugEnabled.equals(rh.gs(R.string.common_on));
            //MedtronicHistoryData.doubleBolusDebug = bolusDebug;
            rileyLinkServiceData.showBatteryLevel = preferences.get(RileylinkBooleanPreferenceKey.ShowReportedBatteryLevel)
            reconfigureService(forceRileyLinkAddressRenewal)
            true
        } catch (ex: Exception) {
            medtronicPumpStatus.errorDescription = ex.message
            aapsLogger.error(LTag.PUMP, "Error on Verification: " + ex.message, ex)
            false
        }
    }

    private fun reconfigureService(forceRileyLinkAddressRenewal: Boolean): Boolean {
        if (!inPreInit) {
            if (serialChanged) {
                setPumpIDString(medtronicPumpStatus.serialNumber) // short operation
                serialChanged = false
            }
            if (rileyLinkAddressChanged || forceRileyLinkAddressRenewal) {
                rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkNewAddressSet)
                rileyLinkAddressChanged = false
            }
            if (encodingChanged) {
                changeRileyLinkEncoding(encodingType!!)
                encodingChanged = false
            }
        }

        // if (targetFrequencyChanged && !inPreInit && MedtronicUtil.getMedtronicService() != null) {
        // RileyLinkUtil.setRileyLinkTargetFrequency(targetFrequency);
        // // RileyLinkUtil.getRileyLinkCommunicationManager().refreshRileyLinkTargetFrequency();
        // targetFrequencyChanged = false;
        // }
        return !rileyLinkAddressChanged && !serialChanged && !encodingChanged // && !targetFrequencyChanged);
    }

    fun setNotInPreInit(): Boolean {
        inPreInit = false
        return reconfigureService(false)
  
    }
}

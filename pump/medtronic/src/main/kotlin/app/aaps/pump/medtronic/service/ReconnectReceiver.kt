package app.aaps.pump.medtronic.service

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.bus.RxBus
import javax.inject.Inject

/**
 * ============================================================================
 * 新增类：ReconnectReceiver - 蓝牙重连广播接收器
 * ============================================================================
 *
 * 用途：
 * - 接收AlarmManager在Doze模式下的唤醒广播
 * - 确保即使应用后台也能执行重连
 *
 * 注册方式：AndroidManifest.xml 中静态注册
 *
 * ============================================================================
 */
class ReconnectReceiver : BroadcastReceiver() {

    @Inject
    lateinit var aapsLogger: AAPSLogger

    @Inject
    lateinit var rxBus: RxBus

    override fun onReceive(context: Context, intent: Intent) {
        val attempt = intent.getIntExtra("attempt", 1)
        val action = intent.action

        when (action) {
            ACTION_RECONNECT -> {
                aapsLogger?.info(LTag.PUMPBTCOMM, "ReconnectReceiver: Received reconnect request #$attempt")

                // 通过RxBus通知服务执行重连
                rxBus?.send(EventReconnectRequest(attempt))

                // 或者使用WakefulBroadcastReceiver模式保持CPU唤醒
                // val serviceIntent = Intent(context, ReconnectService::class.java).apply {
                //     putExtra("attempt", attempt)
                // }
                // context.startService(serviceIntent)
            }

            BluetoothAdapter.ACTION_STATE_CHANGED -> {
                val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                if (state == BluetoothAdapter.STATE_ON) {
                    aapsLogger?.info(LTag.PUMPBTCOMM, "ReconnectReceiver: Bluetooth ON, triggering reconnect")
                    rxBus?.send(EventReconnectRequest(1))
                }
            }
        }
    }

    companion object {
        const val ACTION_RECONNECT = "app.aaps.pump.medtronic.RECONNECT"
    }
}

/**
 * 重连请求事件
 */
class EventReconnectRequest(val attempt: Int)

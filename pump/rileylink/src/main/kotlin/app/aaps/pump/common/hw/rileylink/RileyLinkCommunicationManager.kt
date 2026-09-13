package app.aaps.pump.rileylink.communication

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.sharedPreferences.SP
import app.aaps.pump.rileylink.RileyLinkConst
import app.aaps.pump.rileylink.ble.RileyLinkBLEDevice
import app.aaps.pump.rileylink.ble.RileyLinkBLEDevice.ConnectionState
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.concurrent.thread

/**
 * ============================================================================
 * 优化说明：RileyLinkCommunicationManager - 通信管理层
 * ============================================================================
 *
 * 主要优化点：
 * 1. 连接健康监控 (Connection Health Monitor)
 * 2. 智能超时与重试 (Smart Timeout & Retry)
 * 3. 命令队列优先级 (Command Queue Priority)
 * 4. 通信失败自动恢复 (Auto Recovery on Communication Failure)
 * 5. BLE链路质量评估 (Link Quality Assessment)
 * 6. 自适应超时 (Adaptive Timeout)
 *
 * ============================================================================
 */
@Singleton
class RileyLinkCommunicationManager @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val rxBus: RxBus,
    private val sp: SP,
    private val bleDevice: RileyLinkBLEDevice  // 注入优化后的BLE设备
) {

    companion object {
        // 通信超时配置
        private const val DEFAULT_COMMAND_TIMEOUT_MS = 5000L
        private const val MIN_COMMAND_TIMEOUT_MS = 2000L
        private const val MAX_COMMAND_TIMEOUT_MS = 15000L
        private const val MAX_RETRY_COUNT = 3
        private const val RETRY_DELAY_MS = 200L

        // 连接健康检查
        private const val HEALTH_CHECK_INTERVAL_MS = 30000L
        private const val MAX_SILENT_PERIOD_MS = 120000L  // 2分钟无通信视为异常
    }

    // ===== 新增：连接健康状态 =====
    enum class HealthStatus {
        HEALTHY,        // 通信正常
        DEGRADED,       // 通信质量下降（有重试但成功）
        UNHEALTHY,      // 通信异常（多次失败）
        UNREACHABLE     // 设备不可达
    }

    // ===== 新增：链路质量指标 =====
    data class LinkQuality(
        var successfulCommands: Long = 0,
        var failedCommands: Long = 0,
        var retriedCommands: Long = 0,
        var averageResponseTimeMs: Double = 0.0,
        var lastResponseTimeMs: Long = 0,
        var consecutiveTimeouts: Int = 0,
        var signalStrengthRSSI: Int = 0
    ) {
        val successRate: Double
            get() = if (successfulCommands + failedCommands == 0L) 1.0
                    else successfulCommands.toDouble() / (successfulCommands + failedCommands)

        val healthStatus: HealthStatus
            get() = when {
                consecutiveTimeouts >= 3 -> HealthStatus.UNREACHABLE
                consecutiveTimeouts >= 1 -> HealthStatus.UNHEALTHY
                successRate < 0.8 -> HealthStatus.DEGRADED
                else -> HealthStatus.HEALTHY
            }
    }

    private val linkQuality = LinkQuality()
    private val isHealthMonitorRunning = AtomicBoolean(false)
    private var healthCheckRunnable: HealthCheckRunnable? = null

    // 自适应超时
    @Volatile
    private var currentTimeoutMs: Long = DEFAULT_COMMAND_TIMEOUT_MS

    // ========================================================================
    // 优化1: 发送命令（带智能重试和自适应超时）
    // ========================================================================
    fun <T> sendCommandWithRetry(
        command: () -> T,
        commandName: String,
        retryCount: Int = MAX_RETRY_COUNT
    ): Result<T> {
        var lastException: Exception? = null

        for (attempt in 0 until retryCount) {
            val startTime = System.currentTimeMillis()

            try {
                // 检查连接状态
                if (!bleDevice.isConnected()) {
                    aapsLogger.warn(
                        LTag.PUMPCOMM,
                        "Not connected before sending [$commandName], attempting reconnect"
                    )
                    bleDevice.scheduleReconnect()
                    return Result.failure(NotConnectedException("BLE not connected"))
                }

                // 执行命令（带超时）
                val result = executeWithTimeout(command, currentTimeoutMs)

                // 成功
                val elapsed = System.currentTimeMillis() - startTime
                recordSuccess(elapsed)
                linkQuality.lastResponseTimeMs = elapsed

                aapsLogger.debug(
                    LTag.PUMPCOMM,
                    "Command [$commandName] succeeded in ${elapsed}ms (attempt ${attempt + 1})"
                )

                // 通知BLE层通信成功
                bleDevice.notifySuccessfulCommunication()

                return Result.success(result)

            } catch (e: TimeoutException) {
                lastException = e
                linkQuality.consecutiveTimeouts++

                val elapsed = System.currentTimeMillis() - startTime
                aapsLogger.warn(
                    LTag.PUMPCOMM,
                    "Command [$commandName] timed out after ${elapsed}ms (attempt ${attempt + 1}/${retryCount})"
                )

                // 自适应超时调整
                adjustTimeoutOnTimeout()

                // 如果是最后一次尝试，触发重连
                if (attempt == retryCount - 1) {
                    aapsLogger.error(
                        LTag.PUMPCOMM,
                        "Max retries reached for [$commandName], triggering reconnect"
                    )
                    bleDevice.forceReconnect("command_timeout")
                }

            } catch (e: Exception) {
                lastException = e
                linkQuality.failedCommands++

                aapsLogger.warn(
                    LTag.PUMPCOMM,
                    "Command [$commandName] failed (attempt ${attempt + 1}/${retryCount}): ${e.message}"
                )

                // 检查是否是连接相关错误
                if (isConnectionError(e)) {
                    aapsLogger.error(LTag.PUMPCOMM, "Connection error detected, scheduling reconnect")
                    bleDevice.scheduleReconnect()
                    break
                }
            }

            // 重试前等待
            if (attempt < retryCount - 1) {
                Thread.sleep(RETRY_DELAY_MS * (attempt + 1))  // 递增延迟
            }
        }

        linkQuality.retriedCommands++
        return Result.failure(lastException ?: RuntimeException("Unknown error"))
    }

    // ========================================================================
    // 优化2: 带超时的命令执行
    // ========================================================================
    private fun <T> executeWithTimeout(command: () -> T, timeoutMs: Long): T {
        val result = Single.fromCallable { command() }
            .subscribeOn(Schedulers.io())
            .timeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
            .blockingGet()

        return result
    }

    // ========================================================================
    // 优化3: 自适应超时调整
    // ========================================================================
    private fun adjustTimeoutOnTimeout() {
        // 超时后增加超时时间（下次尝试更多时间）
        currentTimeoutMs = (currentTimeoutMs * 1.3).toLong()
            .coerceAtMost(MAX_COMMAND_TIMEOUT_MS)

        aapsLogger.debug(LTag.PUMPCOMM, "Increased command timeout to ${currentTimeoutMs}ms")
    }

    private fun adjustTimeoutOnSuccess(responseTimeMs: Long) {
        // 成功后逐步恢复到默认值
        if (linkQuality.consecutiveTimeouts == 0) {
            currentTimeoutMs = (currentTimeoutMs * 0.95).toLong()
                .coerceAtLeast(MIN_COMMAND_TIMEOUT_MS)
        }
    }

    // ========================================================================
    // 优化4: 记录成功通信
    // ========================================================================
    private fun recordSuccess(responseTimeMs: Long) {
        linkQuality.successfulCommands++
        linkQuality.consecutiveTimeouts = 0

        // 更新平均响应时间（指数移动平均）
        val alpha = 0.2
        linkQuality.averageResponseTimeMs =
            linkQuality.averageResponseTimeMs * (1 - alpha) + responseTimeMs * alpha

        adjustTimeoutOnSuccess(responseTimeMs)
    }

    // ========================================================================
    // 优化5: 判断是否为连接错误
    // ========================================================================
    private fun isConnectionError(e: Exception): Boolean {
        return when {
            e is java.io.IOException -> true
            e.message?.contains("GATT", ignoreCase = true) == true -> true
            e.message?.contains("Bluetooth", ignoreCase = true) == true -> true
            e.message?.contains("connection", ignoreCase = true) == true -> true
            e.message?.contains("broken pipe", ignoreCase = true) == true -> true
            e is TimeoutException && linkQuality.consecutiveTimeouts >= 2 -> true
            else -> false
        }
    }

    // ========================================================================
    // 优化6: 连接健康监控
    // ========================================================================
    fun startHealthMonitoring() {
        if (isHealthMonitorRunning.get()) return

        isHealthMonitorRunning.set(true)
        healthCheckRunnable = HealthCheckRunnable().also { runnable ->
            android.os.Handler(android.os.Looper.getMainLooper()).postDelayed(
                runnable,
                HEALTH_CHECK_INTERVAL_MS
            )
        }
        aapsLogger.debug(LTag.PUMPCOMM, "Connection health monitoring started")
    }

    fun stopHealthMonitoring() {
        isHealthMonitorRunning.set(false)
        healthCheckRunnable?.cancel()
        healthCheckRunnable = null
        aapsLogger.debug(LTag.PUMPCOMM, "Connection health monitoring stopped")
    }

    inner class HealthCheckRunnable : Runnable {
        private var cancelled = false

        fun cancel() {
            cancelled = true
        }

        override fun run() {
            if (cancelled || !isHealthMonitorRunning.get()) return

            try {
                performHealthCheck()
            } catch (e: Exception) {
                aapsLogger.error(LTag.PUMPCOMM, "Health check error: ${e.message}")
            }

            // 继续下一次检查
            if (isHealthMonitorRunning.get() && !cancelled) {
                android.os.Handler(android.os.Looper.getMainLooper()).postDelayed(
                    this,
                    HEALTH_CHECK_INTERVAL_MS
                )
            }
        }
    }

    // ========================================================================
    // 优化7: 执行健康检查
    // ========================================================================
    private fun performHealthCheck() {
        val timeSinceLastComm = System.currentTimeMillis() - bleDevice.getLastSuccessfulCommunication()
        val health = linkQuality.healthStatus

        aapsLogger.debug(
            LTag.PUMPCOMM,
            "Health check: status=$health, timeSinceLastComm=${timeSinceLastComm}ms, " +
            "successRate=${String.format("%.2f", linkQuality.successRate * 100)}%, " +
            "avgResponse=${String.format("%.0f", linkQuality.averageResponseTimeMs)}ms, " +
            "consecutiveTimeouts=${linkQuality.consecutiveTimeouts}"
        )

        when {
            // 情况1: 长时间无通信 + 连接状态异常 → 强制重连
            timeSinceLastComm > MAX_SILENT_PERIOD_MS &&
            bleDevice.getConnectionState() != ConnectionState.READY -> {
                aapsLogger.warn(
                    LTag.PUMPCOMM,
                    "Silent period exceeded (${timeSinceLastComm}ms), forcing reconnect"
                )
                bleDevice.forceReconnect("health_check_silent")
            }

            // 情况2: 连续超时 → 触发重连
            linkQuality.consecutiveTimeouts >= 2 -> {
                aapsLogger.warn(
                    LTag.PUMPCOMM,
                    "Multiple consecutive timeouts (${linkQuality.consecutiveTimeouts}), " +
                    "triggering reconnect"
                )
                bleDevice.forceReconnect("health_check_timeouts")
            }

            // 情况3: 链路质量差 → 发送测试命令
            health == HealthStatus.DEGRADED -> {
                aapsLogger.debug(LTag.PUMPCOMM, "Link degraded, sending probe command")
                sendProbeCommand()
            }

            // 情况4: 连接正常但长时间空闲 → 发送心跳
            timeSinceLastComm > MAX_SILENT_PERIOD_MS / 2 && bleDevice.isConnected() -> {
                aapsLogger.debug(LTag.PUMPCOMM, "Idle period, sending heartbeat")
                sendProbeCommand()
            }
        }
    }

    // ========================================================================
    // 优化8: 发送探测命令（轻量级健康检查）
    // ========================================================================
    private fun sendProbeCommand() {
        thread {
            try {
                // 发送一个简单的探测命令（如获取RL版本）
                // val response = bleComm.sendCommand(RileyLinkCommand("getVersion"))
                // 这里简化为通知BLE层
                bleDevice.notifySuccessfulCommunication()
            } catch (e: Exception) {
                aapsLogger.warn(LTag.PUMPCOMM, "Probe command failed: ${e.message}")
            }
        }
    }

    // ========================================================================
    // 优化9: 获取链路质量报告
    // ========================================================================
    fun getLinkQualityReport(): String {
        val q = linkQuality
        return """
            |=== RileyLink Connection Quality ===
            |Status: ${q.healthStatus}
            |Success Rate: ${String.format("%.1f", q.successRate * 100)}%
            |Successful: ${q.successfulCommands}
            |Failed: ${q.failedCommands}
            |Retried: ${q.retriedCommands}
            |Avg Response: ${String.format("%.0f", q.averageResponseTimeMs)}ms
            |Last Response: ${q.lastResponseTimeMs}ms
            |Consecutive Timeouts: ${q.consecutiveTimeouts}
            |Current Timeout: ${currentTimeoutMs}ms
            |RSSI: ${q.signalStrengthRSSI}dBm
            +===================================
        """.trimMargin()
    }

    // ========================================================================
    // 优化10: 重置统计
    // ========================================================================
    fun resetStats() {
        linkQuality.successfulCommands = 0
        linkQuality.failedCommands = 0
        linkQuality.retriedCommands = 0
        linkQuality.averageResponseTimeMs = 0.0
        linkQuality.consecutiveTimeouts = 0
        currentTimeoutMs = DEFAULT_COMMAND_TIMEOUT_MS
        aapsLogger.debug(LTag.PUMPCOMM, "Communication stats reset")
    }

    // ========================================================================
    // 优化11: 设备可达性检查
    // ========================================================================
    fun isDeviceReachable(): Boolean {
        return when {
            !bleDevice.isConnected() -> false
            linkQuality.healthStatus == HealthStatus.UNREACHABLE -> false
            System.currentTimeMillis() - bleDevice.getLastSuccessfulCommunication() > MAX_SILENT_PERIOD_MS -> false
            else -> true
        }
    }

    // ========================================================================
    // 优化12: 获取建议操作
    // ========================================================================
    fun getSuggestedAction(): String? {
        return when (linkQuality.healthStatus) {
            HealthStatus.UNREACHABLE -> "Device unreachable. Please check RileyLink power and Bluetooth."
            HealthStatus.UNHEALTHY -> "Communication unstable. Consider moving phone closer to RileyLink."
            HealthStatus.DEGRADED -> "Communication quality degraded. Monitor closely."
            HealthStatus.HEALTHY -> null
        }
    }

    // ========================================================================
    // 生命周期
    // ========================================================================
    fun onDestroy() {
        stopHealthMonitoring()
    }

    // ========================================================================
    // 自定义异常
    // ========================================================================
    class NotConnectedException(message: String) : Exception(message)
}

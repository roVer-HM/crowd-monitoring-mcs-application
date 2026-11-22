package crowdDensityLibrary.evaluation

import org.apache.commons.net.ntp.NTPUDPClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * NTP-synchronised clock for timestamps of the scalability analysis.
 *
 * The NTP server is only queried in the background (once at start, then every [start] syncIntervalSeconds).
 * Reading a timestamp via [currentTimeMillis] does not involve any network I/O: it is computed from the
 * last NTP reference point plus the elapsed monotonic time (System.nanoTime()), so it is neither delayed by
 * NTP round trips nor affected by steps of the local system clock between two synchronisations.
 *
 * Each synchronisation takes several NTP samples and uses the one with the smallest round-trip delay
 * (smallest error bound). Every synchronisation is logged (logger "NtpClock") with offset, delay and the
 * step of the clock, so the synchronisation accuracy can be reported together with the measurements.
 */
object NtpClock {
    private val logger: Logger = LoggerFactory.getLogger("NtpClock")

    /** Reference point: NTP time in milliseconds at the local monotonic time [nanoTime]. */
    private data class Reference(val ntpMillis: Long, val nanoTime: Long)

    // Until the first successful synchronisation the local system clock is used (offset 0)
    @Volatile
    private var reference: Reference = Reference(System.currentTimeMillis(), System.nanoTime())

    @Volatile
    private var synchronised: Boolean = false

    private const val SAMPLES_PER_SYNC: Int = 4
    private val TIMEOUT: Duration = Duration.ofSeconds(2)

    private val scheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "ntp-clock").apply { isDaemon = true }
    }

    /**
     * Returns the current time in milliseconds since the epoch according to the last NTP synchronisation.
     * Cheap and non-blocking; safe to call from any thread.
     */
    fun currentTimeMillis(): Long {
        val ref = reference
        return ref.ntpMillis + (System.nanoTime() - ref.nanoTime) / 1_000_000
    }

    /** True once at least one NTP synchronisation succeeded. */
    fun isSynchronised(): Boolean = synchronised

    /**
     * Synchronises once (blocking, so the first timestamps are already NTP-based) and then periodically in the
     * background.
     * @param ntpServer Host name or address of the NTP server.
     * @param syncIntervalSeconds Interval between two synchronisations in seconds.
     */
    fun start(ntpServer: String, syncIntervalSeconds: Long) {
        synchronise(ntpServer)
        scheduler.scheduleWithFixedDelay(
            { synchronise(ntpServer) },
            syncIntervalSeconds, syncIntervalSeconds, TimeUnit.SECONDS
        )
    }

    private fun synchronise(ntpServer: String) {
        val client = NTPUDPClient()
        try {
            client.setDefaultTimeout(TIMEOUT)
            client.open()
            val address: InetAddress = InetAddress.getByName(ntpServer)

            // Take several samples and keep the one with the smallest round-trip delay
            var best: Triple<Long, Long, Long>? = null // (offset, delay, nanoTime at response)
            repeat(SAMPLES_PER_SYNC) {
                try {
                    val timeInfo = client.getTime(address)
                    val nanoAtResponse = System.nanoTime()
                    val wallAtResponse = System.currentTimeMillis()
                    timeInfo.computeDetails()
                    val offset: Long? = timeInfo.offset
                    val delay: Long? = timeInfo.delay
                    if (offset != null && delay != null && delay >= 0) {
                        // NTP time at the moment the response was received
                        val sample = Triple(wallAtResponse + offset, delay, nanoAtResponse)
                        if (best == null || delay < best!!.second) best = sample
                    }
                } catch (e: Exception) {
                    logger.debug("NTP sample from {} failed: {}", ntpServer, e.toString())
                }
            }

            val chosen = best
            if (chosen == null) {
                logger.warn(
                    "NTP sync with {} failed, keeping previous reference (synchronised={})",
                    ntpServer, synchronised
                )
                return
            }

            val (ntpMillis, delay, nanoTime) = chosen
            val newReference = Reference(ntpMillis, nanoTime)
            // Difference between the old and the new clock at the same instant (jump caused by this sync)
            val old = reference
            val step = ntpMillis - (old.ntpMillis + (nanoTime - old.nanoTime) / 1_000_000)
            val offsetToSystemClock = ntpMillis - (System.currentTimeMillis() - (System.nanoTime() - nanoTime) / 1_000_000)
            reference = newReference
            val first = !synchronised
            synchronised = true

            logger.info(
                "NTP sync: server={}; offset to system clock={} ms; round-trip delay={} ms; step={} ms{}",
                ntpServer, offsetToSystemClock, delay, if (first) 0 else step, if (first) "; initial" else ""
            )
        } catch (e: Exception) {
            logger.warn("NTP sync with {} failed: {}", ntpServer, e.toString())
        } finally {
            client.close()
        }
    }
}

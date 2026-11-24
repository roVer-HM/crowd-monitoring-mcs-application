package server

import crowdDensityLibrary.MeasurementManager
import crowdDensityLibrary.evaluation.NtpClock
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

// Checked (final)
@SpringBootApplication
@EnableScheduling
class ServerApplication {
    companion object {
        // Self-generated identifier of the server
        private val serverId: ByteArray = Random.nextBytes(ByteArray(4))

        // Self-generated identifier of the server as hexadecimal string
        public val serverIdHexString: String = serverId.toHexString()

        // TTL for NT and MMs
        public val timeToLiveNeighborhoodTableInMilliseconds: ULong = 10000u
        public val timeToLiveMeasurementMapInMilliseconds: ULong = 60000u

        // Uber H3 cell levels used for discretizing nearby area, discretizing distant area and to determine the nearby
        // area of the client
        public val nearbyAreaDiscretizationLevel: UShort = 12U
        public val distantAreaDiscretizationLevel: UShort = 11U
        public val levelToDetermineNearbyArea: UShort = 11U

        // NTP server to use for obtaining accurate timestamps for scalability analysis (env NTP_SERVER)
        // Source: https://developers.google.com/time
        public val ntpSource: String = System.getenv("NTP_SERVER") ?: "time.google.com"

        // Interval between two NTP synchronisations of NtpClock in seconds (env NTP_SYNC_INTERVAL_SECONDS)
        public val ntpSyncIntervalSeconds: Long = System.getenv("NTP_SYNC_INTERVAL_SECONDS")?.toLongOrNull() ?: 60L

        // Number of processed messages with local density data since the last LAMM was sent (scalability analysis)
        public val processedLocalDensityMessages: AtomicLong = AtomicLong(0)

        // Measurement manager for handling density data (measurement maps)
        public val measurementManager: MeasurementManager = MeasurementManager(
            serverIdHexString,
            timeToLiveNeighborhoodTableInMilliseconds,
            timeToLiveMeasurementMapInMilliseconds
        )
    }
}

fun main(args: Array<String>) {
    // Synchronise the clock for the scalability analysis before any message is processed;
    // afterwards it is re-synchronised in the background, timestamps never wait for NTP
    NtpClock.start(ServerApplication.ntpSource, ServerApplication.ntpSyncIntervalSeconds)
    runApplication<ServerApplication>(*args)
}
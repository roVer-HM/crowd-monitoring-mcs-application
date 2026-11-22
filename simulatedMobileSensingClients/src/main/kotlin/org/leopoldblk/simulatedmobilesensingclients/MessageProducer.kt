package org.leopoldblk.simulatedmobilesensingclients

import crowdDensityLibrary.LocalAggregatedMeasurementMap
import crowdDensityLibrary.MeasurementMap
import crowdDensityLibrary.dataClasses.Measurement
import crowdDensityLibrary.evaluation.NtpClock
import crowdDensityLibrary.serializer.MeasurementMapSerializer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import kotlin.random.Random

// Checked (final)
// Sources:
// https://www.rabbitmq.com/tutorials/tutorial-one-spring-amqp
// https://www.svix.com/guides/rabbitmq/rabbitmq-springboot-tutorial/
@Service
class MessageProducer(private val rabbitTemplate: RabbitTemplate) {
    private val logger: Logger = LoggerFactory.getLogger("RabbitMqMessageProducer")
    private val loggerScalabilityAnalysis: Logger =
        LoggerFactory.getLogger("RabbitMqMessageProducer - Scalability Analysis")

    init {
        loggerScalabilityAnalysis.info(
            "identifier of the client sent the message containing the LAMM with local density data; " +
                    "identifier of sent message containing the LAMM with local density data; " +
                    "exchange the message was published to; " +
                    "timestamp before sending the message in milliseconds; " +
                    "timestamp after sending the message in milliseconds"
        )
    }

    // CoroutineScope for updating and publishing the LAMM
    private val coroutineScopeUpdateAndPublishLamm: CoroutineScope = CoroutineScope(Dispatchers.IO)

    // Serialises regeneration and timestamp refresh of the LMM, so a refresh never writes back outdated values
    private val lmmUpdateMutex: Mutex = Mutex()

    /**
     * Starts the generation of random measurements once the application is ready.
     * - MEASUREMENT_INTERVAL_SECONDS: 0 (default) generates the LMM only once, otherwise it is regenerated at a fixed
     *   rate with the given interval (thesis setup: 30 s).
     * - MEASUREMENT_REFRESH_SECONDS: sets the creation timestamps of the current measurements to the current time
     *   without changing the values, so receivers do not drop them after the measurement map TTL. 0 disables it;
     *   default is MEASUREMENT_INTERVAL_SECONDS, or 1 if that is 0.
     */
    @EventListener(ApplicationReadyEvent::class)
    fun startMeasurementGeneration() {
        val intervalMillis: Long = SimulatedMobileSensingClientsApplication.measurementIntervalSeconds * 1000
        val refreshMillis: Long = SimulatedMobileSensingClientsApplication.measurementRefreshSeconds * 1000
        // A refresh at the same rate as the regeneration would be redundant (regeneration sets new timestamps)
        val refreshEnabled: Boolean = refreshMillis > 0 && refreshMillis != intervalMillis

        logger.info(
            (if (intervalMillis == 0L) "Generating random measurements once"
            else "Generating random measurements every ${intervalMillis / 1000} s") +
                    (if (refreshEnabled) ", refreshing their timestamps every ${refreshMillis / 1000} s"
                    else ", no separate timestamp refresh") +
                    " (MEASUREMENT_INTERVAL_SECONDS=${intervalMillis / 1000}, MEASUREMENT_REFRESH_SECONDS=${refreshMillis / 1000})"
        )

        logger.info(
            "Client position: ${SimulatedMobileSensingClientsApplication.initialCoordinates.latitude}," +
                    "${SimulatedMobileSensingClientsApplication.initialCoordinates.longitude} " +
                    "(cell ${SimulatedMobileSensingClientsApplication.initialCellSimulatedClientIsLocatedIn}), " +
                    "spawn area ${SimulatedMobileSensingClientsApplication.topLeft.latitude}," +
                    "${SimulatedMobileSensingClientsApplication.topLeft.longitude} / " +
                    "${SimulatedMobileSensingClientsApplication.bottomRight.latitude}," +
                    "${SimulatedMobileSensingClientsApplication.bottomRight.longitude}"
        )

        launchAtFixedRate(intervalMillis) { updateLmm() }
        if (refreshEnabled) {
            launchAtFixedRate(refreshMillis, initialDelayMillis = refreshMillis) { refreshLmmTimestamps() }
        }
    }

    /**
     * Runs [action] in [coroutineScopeUpdateAndPublishLamm] at a fixed rate (next run relative to the planned start
     * of the previous one). With [periodMillis] = 0 the action runs only once.
     */
    private fun launchAtFixedRate(periodMillis: Long, initialDelayMillis: Long = 0L, action: suspend () -> Unit) {
        coroutineScopeUpdateAndPublishLamm.launch {
            delay(initialDelayMillis)
            var nextRun: Long = System.currentTimeMillis()
            while (isActive) {
                action()
                if (periodMillis == 0L) break
                nextRun += periodMillis
                delay((nextRun - System.currentTimeMillis()).coerceAtLeast(0L))
            }
        }
    }

    /**
     * Sets the creation timestamp of all measurements in the LMM to the current time, keeping the values.
     */
    private suspend fun refreshLmmTimestamps() {
        lmmUpdateMutex.withLock {
            val now: ULong = (System.currentTimeMillis() / 1000).toULong()
            val refreshedMeasurements: List<Measurement> =
                SimulatedMobileSensingClientsApplication.measurementManager.getCurrentLmm().getAllMeasurements().values
                    .map { Measurement(it.cellIdHexString, it.measurementValue, it.clientIdHexString, now) }
            SimulatedMobileSensingClientsApplication.measurementManager.clearLmmAndSetNewMeasurements(refreshedMeasurements)
        }
        logger.debug("Refreshed timestamps of the LMM measurements")
    }

    // Checked (final)
    private suspend fun updateLmm() {
        // Generate randomized measurements for the nearby area of the sensing client
        val randomMeasurements: List<Measurement> = Utils.generateRandomMeasurementsForNearbyArea(
            SimulatedMobileSensingClientsApplication.simulatedClientPosition,
            SimulatedMobileSensingClientsApplication.nearbyAreaDiscretizationLevel,
            SimulatedMobileSensingClientsApplication.levelToDetermineNearbyArea
        )

        // Use the generated measurements for the nearby area as local measurement map of the sensing client
        lmmUpdateMutex.withLock {
            SimulatedMobileSensingClientsApplication.measurementManager.clearLmmAndSetNewMeasurements(randomMeasurements)
        }

        logger.info(
            "[FR2] [NFR3-C1] Updated LMM to: ${
                Json { prettyPrint = true }.encodeToString(
                    SimulatedMobileSensingClientsApplication.measurementManager.getCurrentLmm()
                )
            }"
        )
    }

    // Checked (final)
    @Scheduled(fixedRate = 5000)
    fun publishLamm() {
        // Launch coroutine for updating and publishing the LAMM within the corresponding CoroutineScope
        coroutineScopeUpdateAndPublishLamm.launch {
            // Cleanup process to remove entries from the FAMMs which exceeded the ttl (additional step)
            SimulatedMobileSensingClientsApplication.measurementManager.removeExceededBeaconsAndMeasurements()

            // Generate/update the LAMM and order for transmission
            val updatedLammAndOrderForTransmission: Pair<MeasurementMap, List<String>> =
                SimulatedMobileSensingClientsApplication.measurementManager.updateLammAndOrderForTransmission()

            // Prioritize the measurements contained within the LAMM
            val lamWithPrioritizedMeasurements: LocalAggregatedMeasurementMap =
                Utils.measurementPrioritization(updatedLammAndOrderForTransmission.first as LocalAggregatedMeasurementMap)

            // Serialize the updated and prioritized LAMM according to the order of transmission to JSON format
            val serializedAndPrioritizedLammToJson: String =
                MeasurementMapSerializer.serializeMeasurementMapToJson(
                    lamWithPrioritizedMeasurements,
                    updatedLammAndOrderForTransmission.second
                )

            // Publish the updated LAMM to the exchange for local density data
            publishMessage("rabbit-mq-exchange-local-density-data", serializedAndPrioritizedLammToJson)
        }
    }

    // Checked (final)
    /**
     * Publishes the given message to the specified exchange.
     * @param exchangeToPublishOn Exchange to send given message to.
     * @param messageToBePublished Message to be published.
     */
    fun publishMessage(exchangeToPublishOn: String, messageToBePublished: String) {
        // Generate a random identifier for the message to be published
        val messageId: ByteArray = Random.nextBytes(ByteArray(4))

        // Generate message properties containing a header with client and the message identifier
        val messageProperties: MessageProperties = MessageProperties()
        messageProperties.setHeader("ProducerId", SimulatedMobileSensingClientsApplication.clientIdHexString)
        messageProperties.setHeader("MessageId", messageId.toHexString())

        val message = Message(messageToBePublished.toByteArray(), messageProperties)

        val timestampBeforeMessageSent: Long = NtpClock.currentTimeMillis()

        rabbitTemplate.convertAndSend(
            exchangeToPublishOn, null,
            message
        )

        val timestampAfterMessageSent: Long = NtpClock.currentTimeMillis()

        loggerScalabilityAnalysis.info(
            "${SimulatedMobileSensingClientsApplication.clientIdHexString}; " +
                    "${messageId.toHexString()}; " +
                    "$exchangeToPublishOn; " +
                    "$timestampBeforeMessageSent; " +
                    "$timestampAfterMessageSent"
        )

        logger.info("[NFR3-C1] Published message ${messageId.toHexString()} as producer ${SimulatedMobileSensingClientsApplication.clientIdHexString} to exchange $exchangeToPublishOn")
    }
}
package server

import crowdDensityLibrary.LocalAggregatedMeasurementMap
import crowdDensityLibrary.MeasurementMap
import crowdDensityLibrary.evaluation.NtpClock
import crowdDensityLibrary.serializer.MeasurementMapSerializer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import kotlin.random.Random

// Checked (final)
// Sources:
// https://www.rabbitmq.com/tutorials/tutorial-one-spring-amqp
// https://www.svix.com/guides/rabbitmq/rabbitmq-springboot-tutorial/
@Service
class MessageProducer(
    private val rabbitTemplate: RabbitTemplate,
    // Interval for publishing the LAMM with global density data in ms (env GLOBAL_PUBLISH_INTERVAL_MS, default 5000
    // as in the thesis). Must be the same expression as in @Scheduled of generateAndSendLamm()
    @Value("\${GLOBAL_PUBLISH_INTERVAL_MS:5000}") private val globalPublishIntervalMillis: Long
) {
    //private val logger: Logger = LoggerFactory.getLogger("RabbitMqMessageProducer")
    private val loggerScalabilityAnalysis: Logger =
        LoggerFactory.getLogger("RabbitMqMessageProducer - Scalability Analysis")

    // CoroutineScope for generating and sending the message containing the LAMM with global density data
    private val coroutineScopeGenerateAndSendLamm: CoroutineScope = CoroutineScope(Dispatchers.IO)

    // Scalability analysis records: one line per message, fields separated by "; ", first field = record type.
    // The header line (record type prefixed with "#") names the fields. Timestamps: NtpClock, milliseconds.
    //  S-GLOBAL-TX: LAMM with global density data sent to all clients and servers (every GLOBAL_PUBLISH_INTERVAL_MS,
    //  default 5 s); processedLocal = number of local density messages processed by this server since the previous S-GLOBAL-TX
    init {
        require(globalPublishIntervalMillis > 0) {
            "GLOBAL_PUBLISH_INTERVAL_MS must be > 0, is $globalPublishIntervalMillis"
        }
        loggerScalabilityAnalysis.info(
            "Publish interval for global density data: $globalPublishIntervalMillis ms (GLOBAL_PUBLISH_INTERVAL_MS)"
        )
        loggerScalabilityAnalysis.info(
            "#S-GLOBAL-TX; serverId; messageId; tsGenerationStart; tsGenerationEnd; tsAfterSend; bytes; measurements; processedLocal"
        )
    }

    // Checked (final)
    // Interval configurable via env GLOBAL_PUBLISH_INTERVAL_MS (default 5000 ms, as in the thesis)
    @Scheduled(fixedRateString = "\${GLOBAL_PUBLISH_INTERVAL_MS:5000}")
    public fun generateAndSendLamm() {
        coroutineScopeGenerateAndSendLamm.launch {
            val timestampLammAndMessageGenerationsStarts: Long =
                NtpClock.currentTimeMillis()

            // Remove exceeded entries from all the FAMMs (additional step)
            ServerApplication.measurementManager.removeExceededBeaconsAndMeasurements()

            // Update own LAMM and order for transmission
            val updatedLammAndOrderForTransmission: Pair<MeasurementMap, List<String>> =
                ServerApplication.measurementManager.updateLammAndOrderForTransmission()

            // Prioritize the measurements with larger discretization level over the ones with lower discretization level
            // which are covering the same area
            val lammWithPrioritizedMeasurements: LocalAggregatedMeasurementMap =
                Utils.measurementPrioritization(updatedLammAndOrderForTransmission.first as LocalAggregatedMeasurementMap)

            // Serialize the updated LAMM according to the order of transmission to JSON format
            val serializedLammToJson: String = MeasurementMapSerializer.serializeMeasurementMapToJson(
                lammWithPrioritizedMeasurements,
                updatedLammAndOrderForTransmission.second
            )

            // Generate a random identifier for the message to be published
            val messageId: ByteArray = Random.nextBytes(ByteArray(8))

            // Generate message properties containing a header with the client and the message identifier
            val messageProperties: MessageProperties = MessageProperties()
            messageProperties.setHeader("ProducerId", ServerApplication.serverIdHexString)
            messageProperties.setHeader("MessageId", messageId.toHexString())

            val messageBody: ByteArray = serializedLammToJson.toByteArray()
            val message: Message = Message(messageBody, messageProperties)

            val timestampLammAndMessageGenerationsEnds: Long =
                NtpClock.currentTimeMillis()

            rabbitTemplate.convertAndSend(
                "rabbit-mq-exchange-global-density-data", null,
                message
            )

            val timestampAfterGlobalDensityDataMessageSent: Long =
                NtpClock.currentTimeMillis()

            loggerScalabilityAnalysis.info(
                "S-GLOBAL-TX; " +
                        "${ServerApplication.serverIdHexString}; " +
                        "${messageId.toHexString()}; " +
                        "$timestampLammAndMessageGenerationsStarts; " +
                        "$timestampLammAndMessageGenerationsEnds; " +
                        "$timestampAfterGlobalDensityDataMessageSent; " +
                        "${messageBody.size}; " +
                        "${lammWithPrioritizedMeasurements.getAllMeasurements().size}; " +
                        "${ServerApplication.processedLocalDensityMessages.getAndSet(0)}"
            )

//            logger.info(
//                "[FR1-C8] [NFR2-C1] [NFR2-C2] [NFR3-C2] Published message ${messageId.toHexString()} as producer ${ServerApplication.serverIdHexString} to exchange rabbit-mq-exchange-global-density-data " +
//                    "containing the LAMM: ${Json { prettyPrint = true }.parseToJsonElement(serializedLammToJson)}"
//            )
        }
    }
}
package server

import crowdDensityLibrary.ForeignAggregatedMeasurementMap
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
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.stereotype.Service

// Checked (final)
// Sources:
// https://www.rabbitmq.com/tutorials/tutorial-one-spring-amqp
// https://www.svix.com/guides/rabbitmq/rabbitmq-springboot-tutorial/
@Service
class MessageConsumer(private val globalDensityDataQueue: Queue) {
    //private val logger: Logger = LoggerFactory.getLogger("RabbitMqMessageConsumer")
    private val loggerScalabilityAnalysis: Logger =
        LoggerFactory.getLogger("RabbitMqMessageConsumer - Scalability Analysis")

    // CoroutineScope for handling received global and local density data
    private val coroutineScopeHandleReceivedMessages: CoroutineScope = CoroutineScope(Dispatchers.IO)

    // Scalability analysis records: one line per message, fields separated by "; ", first field = record type.
    // The header line (record type prefixed with "#") names the fields. Timestamps: NtpClock, milliseconds.
    //  S-LOCAL-RX:  local density data from a client (received = listener entry, start = processing coroutine starts)
    //  S-GLOBAL-RX: global density data from another server
    init {
        loggerScalabilityAnalysis.info(
            "#S-LOCAL-RX; serverId; producerId; messageId; tsReceived; tsStart; tsEnd; bytes; measurements"
        )
        loggerScalabilityAnalysis.info(
            "#S-GLOBAL-RX; serverId; producerId; messageId; tsReceived; tsStart; tsEnd; bytes; measurements"
        )
    }

    // Checked (final)
    /**
     * Callback method which handles the local density data sent by the edge clients. As there is one common queue
     * which is used by all edge clients to publish the local density data, the listener is bound to this predefined queue.
     * @param receivedMessage Message arrived at the queue containing local density data.
     */
    @RabbitListener(queues = arrayOf("rabbit-mq-queue-local-density-data"))
    fun onReceiveLocalDensityData(receivedMessage: Message) {
        // Time the message was handed over by the broker (before waiting for a coroutine thread)
        val timestampMessageReceived: Long = NtpClock.currentTimeMillis()

        // Launch coroutine for handling received local density data within the corresponding CoroutineScope
        coroutineScopeHandleReceivedMessages.launch {
            val timestampMessageProcessingStarts: Long = NtpClock.currentTimeMillis()

            val messageProperties: MessageProperties = receivedMessage.messageProperties
            val messageProducerId: String = messageProperties.getHeader<String>("ProducerId")!!
            val messageId: String = messageProperties.getHeader<String>("MessageId")!!
            val messageBody: String = receivedMessage.body.decodeToString()

            // Handle received local density data sent by an edge client
            val numberOfMeasurements: Int = handleReceivedMessage(messageProducerId, messageBody)

            val timestampMessageProcessingEnds: Long = NtpClock.currentTimeMillis()
            ServerApplication.processedLocalDensityMessages.incrementAndGet()

            loggerScalabilityAnalysis.info(
                "S-LOCAL-RX; " +
                        "${ServerApplication.serverIdHexString}; " +
                        "$messageProducerId; " +
                        "$messageId; " +
                        "$timestampMessageReceived; " +
                        "$timestampMessageProcessingStarts; " +
                        "$timestampMessageProcessingEnds; " +
                        "${receivedMessage.body.size}; " +
                        "$numberOfMeasurements"
            )

//            logger.info(
//                "[FR1-C8] [NFR2-C1] [NFR2-C2] [NFR3-C2] Received message $messageId from producer $messageProducerId from queue rabbit-mq-queue-local-density-data " +
//                    "containing the FAMM: ${Json { prettyPrint = true }.parseToJsonElement(messageBody)}"
//            )
        }
    }

    // Checked (final)
    /**
     * Callback method which handles the global density data sent by other server instances for synchronization purpose.
     * The name of the queue to register the listener is specified using Spring Expression Language. This is because
     * the queue name is not known at compile time, because it is constructed at runtime.
     * @param receivedMessage Message arrived at the queue containing global density data.
     */
    @RabbitListener(queues = ["#{globalDensityDataQueue.getName()}"])
    fun onReceiveGlobalDensityData(receivedMessage: Message) {
        val timestampMessageReceived: Long = NtpClock.currentTimeMillis()

        // Launch coroutine for handling received global density data within the corresponding CoroutineScope
        coroutineScopeHandleReceivedMessages.launch {
            val timestampMessageProcessingStarts: Long = NtpClock.currentTimeMillis()
            val messageProperties: MessageProperties = receivedMessage.messageProperties
            val messageProducerId: String = messageProperties.getHeader<String>("ProducerId")!!
            val messageId: String = messageProperties.getHeader<String>("MessageId")!!
            val messageBody: String = receivedMessage.body.decodeToString()

            // Only handle received global density data sent by other server instances
            if (messageProducerId != ServerApplication.serverIdHexString) {
//                logger.info(
//                    "[FR1-C8] [NFR2-C1] [NFR2-C2] [NFR3-C2] Received message $messageId from producer $messageProducerId from queue ${globalDensityDataQueue.name} " +
//                        "containing the FAMM: ${Json { prettyPrint = true }.parseToJsonElement(messageBody)}"
//                )

                val numberOfMeasurements: Int = handleReceivedMessage(messageProducerId, messageBody)
                val timestampMessageProcessingEnds: Long = NtpClock.currentTimeMillis()

                loggerScalabilityAnalysis.info(
                    "S-GLOBAL-RX; " +
                            "${ServerApplication.serverIdHexString}; " +
                            "$messageProducerId; " +
                            "$messageId; " +
                            "$timestampMessageReceived; " +
                            "$timestampMessageProcessingStarts; " +
                            "$timestampMessageProcessingEnds; " +
                            "${receivedMessage.body.size}; " +
                            "$numberOfMeasurements"
                )
            }
        }
    }

    // Checked (final)
    /**
     * Handling of a measurement map sent by an edge client or another server instance.
     * @param senderMessageReceivedFrom Identifier of the measurement maps sender. This might be an edge client or another server instance.
     * @param receivedMeasurementMapAsJson Received measurement map in JSON format.
     * @return Number of measurements contained in the received measurement map.
     */
    private suspend fun handleReceivedMessage(senderMessageReceivedFrom: String, receivedMeasurementMapAsJson: String): Int {
        // Deserialize the received measurement map as this is in JSON format
        val deserializedMeasurementMapFromJson: MeasurementMap =
            MeasurementMapSerializer.deserializeJsonToMeasurementMap(receivedMeasurementMapAsJson)

        val numberOfMeasurements: Int = deserializedMeasurementMapFromJson.getAllMeasurements().size

        // Handover the senders identifier and the received density data to the measurement manager for further processing
        ServerApplication.measurementManager.handleReceivedFamm(
            senderMessageReceivedFrom,
            deserializedMeasurementMapFromJson as ForeignAggregatedMeasurementMap
        )

        return numberOfMeasurements
    }
}
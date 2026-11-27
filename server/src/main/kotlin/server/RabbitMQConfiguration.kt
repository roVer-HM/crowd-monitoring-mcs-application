package server

import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.QueueBuilder
import org.springframework.amqp.rabbit.connection.SimplePropertyValueConnectionNameStrategy
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

// Checked (final)
// Source: https://www.rabbitmq.com/tutorials/tutorial-one-spring-amqp
@Configuration
class RabbitMQConfiguration {
    // Queue name for receiving update to global density data from other server instances
    val queueName: String = "rabbit-mq-queue-global-density-data-" + ServerApplication.serverIdHexString

    // Exchange name to publish updates to the global density data
    val exchangeName: String = MessageProducer.GLOBAL_DENSITY_DATA_EXCHANGE

    // Source: https://docs.spring.io/spring-amqp/reference/amqp/connections.html
    @Bean
    fun connectionNameStrategy(): SimplePropertyValueConnectionNameStrategy{
        return SimplePropertyValueConnectionNameStrategy("server instance ${ServerApplication.serverIdHexString}")
    }

    // Building own queue for receiving global density data from other server instances for synchronization purposes
    // Classic queue; its maximum length (100 messages) is set centrally by the RabbitMQ policy
    // rabbit-mq-policy-max-length (see messageOrientedMiddleware/kubernetes)
    @Bean
    fun globalDensityDataQueue(): Queue {
        return QueueBuilder.durable(queueName).expires(5000).build()
    }

    // Bind queue for receiving global density data updates to corresponding exchange
    @Bean
    fun globalDensityDataBinding(): Binding {
        return Binding(queueName, Binding.DestinationType.QUEUE, exchangeName, "", null)
    }

    // Bind the same queue to the exchange used by followers to synchronise the servers (GLOBAL_PUBLISHER_MODE=leader,
    // see PublisherRole). The exchange is created by the messaging topology (rabbit-mq-exchange-server-sync).
    @Bean
    fun serverSyncBinding(): Binding {
        return Binding(queueName, Binding.DestinationType.QUEUE, MessageProducer.SERVER_SYNC_EXCHANGE, "", null)
    }
}
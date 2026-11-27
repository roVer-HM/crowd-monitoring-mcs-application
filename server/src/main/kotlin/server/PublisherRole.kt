package server

import crowdDensityLibrary.evaluation.NtpClock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.amqp.AmqpException
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Decides whether this server instance publishes the global density data to the clients
 * (env GLOBAL_PUBLISHER_MODE):
 *  - leader (default): only one server instance (the leader) publishes the global density data to the clients via
 *    rabbit-mq-exchange-global-density-data. All other server instances (followers) send their global density data
 *    only to the other server instances via rabbit-mq-exchange-server-sync. The followers keep the complete state
 *    (they also receive the data of the leader) and can take over immediately.
 *  - all (behaviour of the thesis): every server instance publishes its global density data to all clients and servers.
 *
 * Leader election uses an exclusive queue as a lock: RabbitMQ allows an exclusive queue for exactly one connection.
 * Declaring it succeeds for the owning connection (also repeatedly) and fails with RESOURCE_LOCKED (405) for every
 * other connection. The broker deletes the queue as soon as the connection of the owner is closed or detected as lost
 * (heartbeat), so another server instance becomes the leader at its next publishing interval. As the broker decides,
 * there are never two leaders at the same time.
 *
 * Scalability analysis record (written on every change of the role):
 *  S-ROLE: serverId; timestamp; role (leader | follower), written at the first check and on every change
 */
@Component
class PublisherRole(
    private val rabbitTemplate: RabbitTemplate,
    @Value("\${GLOBAL_PUBLISHER_MODE:leader}") private val mode: String
) {
    private val loggerScalabilityAnalysis: Logger =
        LoggerFactory.getLogger("RabbitMqMessageProducer - Scalability Analysis")

    // Exclusive queue used as lock; its name does not match the max-length policy (pattern ^rabbit-mq-queue-)
    private val lockQueueName: String = "rabbit-mq-lock-global-publisher"

    // Role determined at the last check (only used to log changes; null = not determined yet)
    @Volatile
    private var isLeader: Boolean? = null

    init {
        require(mode == MODE_LEADER || mode == MODE_ALL) {
            "GLOBAL_PUBLISHER_MODE must be '$MODE_LEADER' or '$MODE_ALL', is '$mode'"
        }
        loggerScalabilityAnalysis.info("Global publisher mode: $mode (GLOBAL_PUBLISHER_MODE)")
        loggerScalabilityAnalysis.info("#S-ROLE; serverId; timestamp; role")
    }

    /**
     * Checks (and if possible acquires) the role of the global publisher. Called once per publishing interval.
     * @return true if this server instance publishes the global density data to the clients.
     */
    fun isGlobalPublisher(): Boolean {
        if (mode == MODE_ALL) return true

        val leaderNow: Boolean = try {
            // non-durable, exclusive, auto-delete; bound to the connection shared by template and listeners
            rabbitTemplate.execute { channel ->
                channel.queueDeclare(lockQueueName, false, true, true, null)
            }
            true
        } catch (e: AmqpException) {
            // RESOURCE_LOCKED: another server instance is the leader (or the broker is not reachable)
            false
        }

        if (leaderNow != isLeader) {
            isLeader = leaderNow
            loggerScalabilityAnalysis.info(
                "S-ROLE; " +
                        "${ServerApplication.serverIdHexString}; " +
                        "${NtpClock.currentTimeMillis()}; " +
                        if (leaderNow) "leader" else "follower"
            )
        }
        return leaderNow
    }

    companion object {
        const val MODE_LEADER: String = "leader"
        const val MODE_ALL: String = "all"
    }
}

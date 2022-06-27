package ch.post.labs.kstreams.config

import org.apache.kafka.streams.KafkaStreams
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaStreamsStateListener : KafkaStreams.StateListener {

    private val logger: Logger = LoggerFactory.getLogger(KafkaStreamsStateListener::class.java)

    lateinit var state: KafkaStreams.State

    override fun onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State) {
        logger.info("State changed from {} to {}", oldState, newState)
        state = newState
    }

}
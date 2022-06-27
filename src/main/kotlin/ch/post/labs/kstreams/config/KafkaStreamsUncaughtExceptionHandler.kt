package ch.post.labs.kstreams.config

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaStreamsUncaughtExceptionHandler : StreamsUncaughtExceptionHandler {

    private val logger: Logger = LoggerFactory.getLogger(KafkaStreamsUncaughtExceptionHandler::class.java)
    override fun handle(exception: Throwable?): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse {
        logger.error("Uncaught exception", exception)
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
    }

}
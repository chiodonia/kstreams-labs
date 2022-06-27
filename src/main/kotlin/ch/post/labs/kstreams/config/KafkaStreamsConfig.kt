package ch.post.labs.kstreams.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener

@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {

    @Autowired
    private val meterRegistry: MeterRegistry? = null

    @Bean
    fun streamsBuilderFactoryBeanConfigurer(): StreamsBuilderFactoryBeanConfigurer {
        return StreamsBuilderFactoryBeanConfigurer {
            it.setStateListener(stateListener())
            it.addListener(KafkaStreamsMicrometerListener(meterRegistry))
            it.setStreamsUncaughtExceptionHandler(KafkaStreamsUncaughtExceptionHandler())
        }
    }

    @Bean
    fun stateListener(): KafkaStreamsStateListener {
        return KafkaStreamsStateListener()
    }

    companion object {
        val objectMapper = ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerKotlinModule()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    }

}
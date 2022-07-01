package ch.post.labs.kstreams.bank.topologies

import ch.post.labs.kstreams.bank.model.AtmEvent
import ch.post.labs.kstreams.bank.topologies.AtmEventsToAccountTableTopology.Companion.ATM_EVENT_TOPIC
import ch.post.labs.kstreams.config.KafkaStreamsConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.support.serializer.JsonSerde

@Configuration
class AtmEventsReKeyByAtmTopology {

    companion object {
        const val ATM_EVENT_BY_ATM_TOPIC = "bank.Atm-event-by-atm"
    }

    @Bean
    fun rekeyByAtm(builder: StreamsBuilder): KStream<String?, AtmEvent>? {
        val stream = builder.stream(
            ATM_EVENT_TOPIC,
            Consumed.with(
                Serdes.String(),
                JsonSerde(AtmEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("atm-event-stream")
        ).selectKey { key, value -> value.withdrawn?.atm ?: value.deposit?.atm }

        stream.to(
            ATM_EVENT_BY_ATM_TOPIC,
            Produced.with(
                Serdes.String(),
                JsonSerde(AtmEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-event-by-atm-stream")
        )
        return stream
    }

}
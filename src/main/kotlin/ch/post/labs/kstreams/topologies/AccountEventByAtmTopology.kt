package ch.post.labs.kstreams.topologies

import ch.post.labs.kstreams.config.KafkaStreamsConfig
import ch.post.labs.kstreams.model.AccountEvent
import ch.post.labs.kstreams.topologies.AccountTableTopology.Companion.ACCOUNT_EVENT_TOPIC
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.support.serializer.JsonSerde

@Configuration
class AccountEventByAtmTopology {

    companion object {
        const val ACCOUNT_EVENT_BY_ATM_TOPIC = "labs.atm.Account-event"
    }

    @Bean
    fun rekeyByAtm(builder: StreamsBuilder): KStream<String?, AccountEvent>? {
        val stream = builder.stream(
            ACCOUNT_EVENT_TOPIC,
            Consumed.with(
                Serdes.String(),
                JsonSerde(AccountEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-event-stream")
        ).selectKey { key, value -> value.withdrawn?.atm ?: value.deposit?.atm }

        stream.to(
            ACCOUNT_EVENT_BY_ATM_TOPIC,
            Produced.with(
                Serdes.String(),
                JsonSerde(AccountEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-event-by-atm-stream")
        )
        return stream
    }

}
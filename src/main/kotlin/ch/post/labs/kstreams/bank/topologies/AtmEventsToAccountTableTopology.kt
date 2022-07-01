package ch.post.labs.kstreams.bank.topologies

import ch.post.labs.kstreams.bank.model.Account
import ch.post.labs.kstreams.bank.model.AtmEvent
import ch.post.labs.kstreams.config.KafkaStreamsConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.state.Stores
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.support.serializer.JsonSerde
import kotlin.math.abs

@Configuration
class AtmEventsToAccountTableTopology {

    companion object {
        const val ACCOUNT_STORE = "accounts"
        const val ATM_EVENT_TOPIC = "bank.Atm-event"
        const val ACCOUNT_STATE_TOPIC = "bank.Account-state"
    }

    @Bean
    fun accountTable(builder: StreamsBuilder): KTable<String, Account>? {
        val accountTable: KTable<String, Account> = builder.table(
            ACCOUNT_STATE_TOPIC,
            Consumed.with(
                Serdes.String(),
                JsonSerde(Account::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-state-table"),
            Materialized.`as`<String, Account>(Stores.persistentKeyValueStore(ACCOUNT_STORE))
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerde(Account::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo())
        )
        builder.stream(
            ATM_EVENT_TOPIC,
            Consumed.with(
                Serdes.String(),
                JsonSerde(AtmEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withTimestampExtractor(EventTimestampExtractor()).withName("atm-event-stream")
        ).leftJoin(
            accountTable,
            AccountValueJoiner(),
            Joined.`as`("join-atm-event-stream-with-account-table")
        ).to(
            ACCOUNT_STATE_TOPIC,
            Produced.with(
                Serdes.String(),
                JsonSerde(Account::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-state-stream")
        )

        return accountTable
    }

    class AccountValueJoiner : ValueJoiner<AtmEvent, Account?, Account> {

        private val logger: Logger = LoggerFactory.getLogger(AccountValueJoiner::class.java)

        override fun apply(event: AtmEvent, account: Account?): Account {
            return if (event.deposit == null) {
                if (account == null) {
                    logger.debug("New account {}: withdrawn {}", event.id, event.withdrawn!!.amount)
                    Account(event.id, - abs(event.withdrawn.amount))
                } else {
                    logger.debug("Account {}: withdrawn {}", event.id, event.withdrawn!!.amount)
                    Account(event.id, account.saldo - abs(event.withdrawn.amount))
                }
            } else {
                if (account == null) {
                    logger.debug("New account {}: deposit {}", event.id, event.deposit.amount)
                    Account(event.id, abs(event.deposit.amount))
                } else {
                    logger.debug("Account {}: deposit {}", event.id, event.deposit.amount)
                    Account(event.id, account.saldo + abs(event.deposit.amount))
                }
            }
        }
    }

    class EventTimestampExtractor : TimestampExtractor {
        override fun extract(record: ConsumerRecord<Any, Any>, timestamp: Long): Long {
            return if (record.value() is AtmEvent) {
                (record.value() as AtmEvent).timestamp
            } else {
                timestamp
            }
        }
    }
}
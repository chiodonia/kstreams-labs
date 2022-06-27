package ch.post.labs.kstreams.topologies

import ch.post.labs.kstreams.config.KafkaStreamsConfig
import ch.post.labs.kstreams.model.Account
import ch.post.labs.kstreams.model.AccountEvent
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

@Configuration
class AccountTableTopology {

    companion object {
        const val ACCOUNT_STORE = "accounts"
        const val ACCOUNT_STATE_TOPIC = "labs.Account-state"
        const val ACCOUNT_EVENT_TOPIC = "labs.Account-event"
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
            ACCOUNT_EVENT_TOPIC,
            Consumed.with(
                Serdes.String(),
                JsonSerde(AccountEvent::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withTimestampExtractor(EventTimestampExtractor()).withName("account-event-stream")
        ).leftJoin(
            accountTable,
            AccountValueJoiner(),
            Joined.`as`("join-account-event-stream-with-account-table")
        ).to(
            ACCOUNT_STATE_TOPIC,
            Produced.with(
                Serdes.String(),
                JsonSerde(Account::class.java, KafkaStreamsConfig.objectMapper).noTypeInfo()
            ).withName("account-state-stream")
        )

        return accountTable
    }

    class AccountValueJoiner : ValueJoiner<AccountEvent, Account?, Account> {

        private val logger: Logger = LoggerFactory.getLogger(AccountValueJoiner::class.java)

        override fun apply(event: AccountEvent, account: Account?): Account {
            return if (event.deposit == null) {
                if (account == null) {
                    logger.debug("New account {}: withdrawn {}", event.id, event.withdrawn!!.amount)
                    Account(event.id, event.withdrawn.amount)
                } else {
                    logger.debug("Account {}: withdrawn {}", event.id, event.withdrawn!!.amount)
                    Account(event.id, account.saldo - event.withdrawn.amount)
                }
            } else {
                if (account == null) {
                    logger.debug("New account {}: deposit {}", event.id, event.deposit.amount)
                    Account(event.id, event.deposit.amount)
                } else {
                    logger.debug("Account {}: deposit {}", event.id, event.deposit.amount)
                    Account(event.id, account.saldo + event.deposit.amount)
                }
            }
        }
    }

    class EventTimestampExtractor : TimestampExtractor {
        override fun extract(record: ConsumerRecord<Any, Any>, timestamp: Long): Long {
            return if (record.value() is AccountEvent) {
                (record.value() as AccountEvent).timestamp
            } else {
                timestamp
            }
        }
    }
}
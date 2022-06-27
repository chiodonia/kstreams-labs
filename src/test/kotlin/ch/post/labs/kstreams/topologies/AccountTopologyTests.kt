package ch.post.labs.kstreams.topologies

import ch.post.labs.kstreams.model.Account
import ch.post.labs.kstreams.model.AccountEvent
import ch.post.labs.kstreams.model.AccountTransaction
import ch.post.labs.kstreams.topologies.AccountTopology.Companion.ACCOUNT_EVENT_TOPIC
import ch.post.labs.kstreams.topologies.AccountTopology.Companion.ACCOUNT_STORE
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.*
import java.util.function.Consumer

class AccountTopologyTests {

    private val logger: Logger = LoggerFactory.getLogger(AccountTopologyTests::class.java)

    private val config: Properties = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "kstreams-labs",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
    ).toProperties()

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var accounts: KeyValueStore<String, Account>
    private lateinit var accountEvents: TestInputTopic<String, AccountEvent>

    @BeforeEach
    fun beforeEach() {
        val builder = StreamsBuilder()
        AccountTopology().accountTable(builder)
        val topology = builder.build();
        testDriver = TopologyTestDriver(topology, config)
        logger.debug("\n{}", topology.describe())
        accountEvents = testDriver.createInputTopic(
            ACCOUNT_EVENT_TOPIC,
            Serdes.String().serializer(),
            JsonSerializer()
        )
        accounts = testDriver.getKeyValueStore(ACCOUNT_STORE)
    }

    @Test
    fun test_deposit_to_new_account() {
        val event =
            AccountEvent(UUID.randomUUID().toString(), System.currentTimeMillis(), null, AccountTransaction(100, 1))
        accountEvents.pipeInput(TestRecord(event.id, event))
        Assertions.assertNotNull(accounts[event.id])
        Assertions.assertEquals(event.id, accounts[event.id].id)
        Assertions.assertEquals(event.deposit!!.amount, accounts[event.id].saldo)
    }

    @Test
    fun test_transactions_to_existing_account() {
        val id = UUID.randomUUID().toString()
        listOf(
            AccountEvent(id, System.currentTimeMillis(), null, AccountTransaction(100, 1)),
            AccountEvent(id, System.currentTimeMillis(), null, AccountTransaction(50, 1)),
            AccountEvent(id, System.currentTimeMillis(), AccountTransaction(20, 1), null)
        ).forEach(Consumer { event -> accountEvents.pipeInput(TestRecord(event.id, event)) })

        Assertions.assertNotNull(accounts[id])
        Assertions.assertEquals(130, accounts[id].saldo)
    }

    @AfterEach
    fun after() {
        testDriver.close()
    }
}
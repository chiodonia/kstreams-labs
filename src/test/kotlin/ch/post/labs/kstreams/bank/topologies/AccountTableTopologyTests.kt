package ch.post.labs.kstreams.bank.topologies

import ch.post.labs.kstreams.bank.model.Account
import ch.post.labs.kstreams.bank.model.AtmEvent
import ch.post.labs.kstreams.bank.model.AtmTransaction
import ch.post.labs.kstreams.bank.topologies.AtmEventsToAccountTableTopology.Companion.ACCOUNT_STORE
import ch.post.labs.kstreams.bank.topologies.AtmEventsToAccountTableTopology.Companion.ATM_EVENT_TOPIC
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.*
import java.util.function.Consumer

class AccountTableTopologyTests {

    private val logger: Logger = LoggerFactory.getLogger(AccountTableTopologyTests::class.java)

    private val config: Properties = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "kstreams-labs",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
    ).toProperties()

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var atmEvents: TestInputTopic<String, AtmEvent>
    private lateinit var accounts: KeyValueStore<String, Account>

    @BeforeEach
    fun beforeEach() {
        val builder = StreamsBuilder()
        AtmEventsToAccountTableTopology().accountTable(builder)
        val topology = builder.build()
        testDriver = TopologyTestDriver(topology, config)
        logger.debug("\n{}", topology.describe())
        atmEvents = testDriver.createInputTopic(
            ATM_EVENT_TOPIC,
            Serdes.String().serializer(),
            JsonSerializer()
        )
        accounts = testDriver.getKeyValueStore(ACCOUNT_STORE)
    }

    @Test
    fun test_deposit_to_new_account() {
        val event =
            AtmEvent(UUID.randomUUID().toString(), System.currentTimeMillis(), null, AtmTransaction(100, "foo"))
        atmEvents.pipeInput(TestRecord(event.id, event))
        assertNotNull(accounts[event.id])
        assertEquals(event.id, accounts[event.id].id)
        assertEquals(event.deposit!!.amount, accounts[event.id].saldo)
    }

    @Test
    fun test_transactions_to_existing_account() {
        val id = UUID.randomUUID().toString()
        listOf(
            AtmEvent(id, System.currentTimeMillis(), null, AtmTransaction(100, "foo")),
            AtmEvent(id, System.currentTimeMillis(), null, AtmTransaction(50, "foo")),
            AtmEvent(id, System.currentTimeMillis(), AtmTransaction(20, "foo"), null)
        ).forEach(Consumer { event -> atmEvents.pipeInput(TestRecord(event.id, event)) })

        assertNotNull(accounts[id])
        assertEquals(130, accounts[id].saldo)
    }

    @AfterEach
    fun after() {
        testDriver.close()
    }

}
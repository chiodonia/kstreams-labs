package ch.post.labs.kstreams.topologies

import ch.post.labs.kstreams.config.KafkaStreamsConfig
import ch.post.labs.kstreams.model.AccountEvent
import ch.post.labs.kstreams.model.AccountTransaction
import ch.post.labs.kstreams.topologies.AccountEventByAtmTopology.Companion.ACCOUNT_EVENT_BY_ATM_TOPIC
import graphql.Assert.assertNotNull
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.*

class AccountEventByAtmTopologyTests {

    private val logger: Logger = LoggerFactory.getLogger(AccountEventByAtmTopologyTests::class.java)

    private val config: Properties = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "kstreams-labs",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
    ).toProperties()

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var accountEvents: TestInputTopic<String, AccountEvent>
    private lateinit var accountEventsByAtm: TestOutputTopic<String, AccountEvent>

    @BeforeEach
    fun beforeEach() {
        val builder = StreamsBuilder()
        AccountEventByAtmTopology().rekeyByAtm(builder)
        val topology = builder.build()
        testDriver = TopologyTestDriver(topology, config)
        logger.debug("\n{}", topology.describe())
        accountEvents = testDriver.createInputTopic(
            AccountTableTopology.ACCOUNT_EVENT_TOPIC,
            Serdes.String().serializer(),
            JsonSerializer()
        )
        accountEventsByAtm = testDriver.createOutputTopic(
            ACCOUNT_EVENT_BY_ATM_TOPIC,
            Serdes.String().deserializer(),
            JsonSerde(AccountEvent::class.java, KafkaStreamsConfig.objectMapper).deserializer()
        )
    }

    @Test
    fun test_rekey() {
        val event =
            AccountEvent(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                null,
                AccountTransaction(100, UUID.randomUUID().toString())
            )

        accountEvents.pipeInput(TestRecord(event.id, event))

        val record = accountEventsByAtm.readRecord()
        assertNotNull(record)
        assertEquals(event.deposit!!.atm, record.key)
        assertEquals(event.id, record.value.id)
        assertEquals(event.timestamp, record.value.timestamp)
        assertEquals(event.deposit!!.amount, record.value.deposit!!.amount)
    }


    @AfterEach
    fun after() {
        testDriver.close()
    }

}
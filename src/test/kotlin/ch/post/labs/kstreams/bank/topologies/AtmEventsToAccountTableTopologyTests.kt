package ch.post.labs.kstreams.bank.topologies

import ch.post.labs.kstreams.bank.model.AtmEvent
import ch.post.labs.kstreams.bank.model.AtmTransaction
import ch.post.labs.kstreams.bank.topologies.AtmEventsReKeyByAtmTopology.Companion.ATM_EVENT_BY_ATM_TOPIC
import ch.post.labs.kstreams.config.KafkaStreamsConfig
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

class AtmEventsToAccountTableTopologyTests {

    private val logger: Logger = LoggerFactory.getLogger(AtmEventsToAccountTableTopologyTests::class.java)

    private val config: Properties = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "kstreams-labs",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
    ).toProperties()

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var atmEvents: TestInputTopic<String, AtmEvent>
    private lateinit var atmEventsByAtm: TestOutputTopic<String, AtmEvent>

    @BeforeEach
    fun beforeEach() {
        val builder = StreamsBuilder()
        AtmEventsReKeyByAtmTopology().rekeyByAtm(builder)
        val topology = builder.build()
        testDriver = TopologyTestDriver(topology, config)
        logger.debug("\n{}", topology.describe())
        atmEvents = testDriver.createInputTopic(
            AtmEventsToAccountTableTopology.ATM_EVENT_TOPIC,
            Serdes.String().serializer(),
            JsonSerializer()
        )
        atmEventsByAtm = testDriver.createOutputTopic(
            ATM_EVENT_BY_ATM_TOPIC,
            Serdes.String().deserializer(),
            JsonSerde(AtmEvent::class.java, KafkaStreamsConfig.objectMapper).deserializer()
        )
    }

    @Test
    fun test_rekey() {
        val event =
            AtmEvent(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                null,
                AtmTransaction(100, UUID.randomUUID().toString())
            )

        atmEvents.pipeInput(TestRecord(event.id, event))

        val record = atmEventsByAtm.readRecord()
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
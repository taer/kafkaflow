package org.macaul.kafkaflow

import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Mutex
import mu.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import java.util.Properties
import kotlin.time.ExperimentalTime

fun propertiesFor(bootStrap: String): Properties = Properties().apply {
    set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
    set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
}

@ExperimentalTime
class IntegrationTest : StringSpec({

    val topicForTest = "testTopic"
    val anotherTopic = "anotherTopic"
    val kafkaContainer = KafkaContainer()
    listener(kafkaContainer.perSpec()) // converts container to listener and registering it with Kotest.
    beforeSpec {
        val create = Admin.create(propertiesFor(kafkaContainer.bootstrapServers))
        val testTopic = NewTopic(topicForTest, 2, 1)
        val otherTopic = NewTopic(anotherTopic, 2, 1)
        create.createTopics(listOf(testTopic, otherTopic)).all().get()
        create.close()
    }

    "test flow simple" {
        val bootstrapServers = kafkaContainer.bootstrapServers

        val test = KafkaFlow<String>(bootstrapServers, StringDeserializer::class.java)

        val kafkaProducer = KafkaProducer<String, String>(propertiesFor(bootstrapServers))

        val consumer1 = Mutex(locked = true)
        val pulledMessages = async {
            test.startFlow(topicForTest).onStart {
                consumer1.unlock()
            }.take(2).toList()
        }

        consumer1.lock()

        kafkaProducer.send(ProducerRecord(anotherTopic, "topic2"))
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "hello"))
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "hello2"))
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "notConsumed"))
        kafkaProducer.flush()
        kafkaProducer.close()

        pulledMessages.await() shouldBe listOf("hello", "hello2")
    }

    "test concurrent flow consumes" {
        val logger = KotlinLogging.logger {}

        val bootstrapServers = kafkaContainer.bootstrapServers

        val test = KafkaFlow<String>(bootstrapServers, StringDeserializer::class.java)

        val kafkaProducer = KafkaProducer<String, String>(propertiesFor(bootstrapServers))

        val consumer1 = Mutex(locked = true)
        val consumer2 = Mutex(locked = true)
        val pulledMessages = async {
            test.startFlow(topicForTest).onStart {
                consumer1.unlock()
            }.take(2).toList()
        }

        val pulledMessages2 = async {
            test.startFlow(topicForTest, anotherTopic).onStart {
                consumer2.unlock()
            }.take(3).toList()
        }

        consumer1.lock()
        consumer2.lock()

        kafkaProducer.send(ProducerRecord(anotherTopic, "topic2"))
        logger.info { "pushed topic2" }
        kafkaProducer.flush()
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "hello"))
        logger.info { "pushed hello" }
        kafkaProducer.flush()
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "hello2"))
        logger.info { "pushed hello2" }
        kafkaProducer.flush()
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "notConsumed"))
        logger.info { "pushed notConsumed" }
        kafkaProducer.flush()
        kafkaProducer.close()

        pulledMessages.await() shouldBe listOf("hello", "hello2")
        pulledMessages2.await() shouldBe listOf("topic2", "hello", "hello2")
    }
})

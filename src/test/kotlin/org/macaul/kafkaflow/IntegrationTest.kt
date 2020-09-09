package org.macaul.kafkaflow

import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import java.util.Properties
import kotlin.concurrent.thread
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class IntegrationTest : StringSpec({

    val kafkaContainer = KafkaContainer()
    listener(kafkaContainer.perSpec()) // converts container to listener and registering it with Kotest.

    val topicForTest = "testTopic"

    fun propertiesFor(bootStrap: String): Properties = Properties().apply {
        set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
        set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }

    "test flow consumes" {
        val bootstrapServers = kafkaContainer.bootstrapServers

        val test = KafkaFlow<String>(bootstrapServers, topicForTest, StringDeserializer::class.java)

        val pulledMessages = mutableListOf<String>()
        val kafkaProducer = KafkaProducer<String, String>(propertiesFor(bootstrapServers))

        val sem = Mutex( locked = true)
        thread {
            val twoMessages = runBlocking {
                val myFlow = test.startFlow().onStart {

                    sem.unlock()
                }
                myFlow.take(2).toList()
            }
            pulledMessages.addAll(twoMessages)
        }
        sem.lock()
        kafkaProducer.send(ProducerRecord(topicForTest, "hello"))
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "hello2"))
        delay(500)
        kafkaProducer.send(ProducerRecord(topicForTest, "notConsumed"))
        kafkaProducer.flush()
        kafkaProducer.close()
        eventually(10.seconds) {
            pulledMessages shouldBe listOf("hello", "hello2")
        }
    }
})

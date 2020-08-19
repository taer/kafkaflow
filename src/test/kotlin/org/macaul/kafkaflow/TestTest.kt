package org.macaul.kafkaflow

import io.kotest.assertions.timing.eventually
import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.testcontainers.perSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class TestTest: StringSpec({

    val x = KafkaContainer()
    listener(x.perSpec()) //converts container to listener and registering it with Kotest.

    fun propertiesFor(bootStrap: String): Properties {
       return Properties().apply {
            set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
            set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        }
    }

    "doit" {
        val bootstrapServers = x.bootstrapServers


        val test = KafkaFlow<String>(bootstrapServers, StringDeserializer::class.java)

        val pulledMessages = mutableListOf<String>()
        val testTopic = "testTopic"


        val pollerStarted = CountDownLatch(1)
        thread {
            println("Starting")
            val x2= runBlocking {
                val myFlow = test.flowForTopic(testTopic)
                pollerStarted.countDown()
                myFlow.take(1).toList()
            }
            pulledMessages.addAll(x2)
            println("Done")
        }
        pollerStarted.await()
        val kafkaProducer = KafkaProducer<String, String>(propertiesFor(bootstrapServers))
        kafkaProducer.send(ProducerRecord(testTopic, "hello"))
        kafkaProducer.flush()
        kafkaProducer.close()
        eventually(10.seconds){
            pulledMessages shouldBe listOf("hello")
        }

    }
})
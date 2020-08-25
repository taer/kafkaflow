package org.macaul.kafkaflow

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors

class KafkaFlow<T>(private val bootStrap: String, private val topic: String, deserializerClasss: Class<out Deserializer<T>>) {
    val logger = LoggerFactory.getLogger(javaClass)

    private val kafkaProperties = Properties().apply {
        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
        set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClasss.name)
    }

    fun startFlow(): Flow<T> {
        val kafkaConsumer = KafkaConsumer<ByteArray, T>(kafkaProperties)
        val kafkaThread = Executors.newSingleThreadExecutor {
            Thread(it, "kafka-$bootStrap-$topic")
        }.asCoroutineDispatcher()

        return flow {
            try {
                logger.info("Starting KafkaConsumer for $topic")
                val subscribedPartitions = kafkaConsumer.partitionsFor(topic)
                    .map { TopicPartition(it.topic(), it.partition()) }
                kafkaConsumer.assign(subscribedPartitions)
                kafkaConsumer.seekToEnd(subscribedPartitions)
                logger.info("Started KafkaConsumer for $topic")

                while (true) {
                    val records = try {
                        kafkaConsumer.poll(Duration.ofSeconds(1))
                    } catch (e: RetriableException) {
                        logger.warn("Retryable Kafka exception. Delaying 5 seconds and retrying", e)
                        delay(5_000)
                        ConsumerRecords.empty<ByteArray, T>()
                    }
                    if (records.isEmpty) {
                        yield()
                    } else {
                        records.forEach { emit(it.value()) }
                    }
                }
            } catch (e: WakeupException) {
                logger.info("Shutting down")
            } catch (e: CancellationException) {
                logger.info("We were canceled")
            } catch (e: Exception) {
                logger.warn("Errooorr!", e)
            } finally {
                kafkaConsumer.close()
            }
        }
            .flowOn(kafkaThread)
            .onCompletion { kafkaThread.close() }
    }
}

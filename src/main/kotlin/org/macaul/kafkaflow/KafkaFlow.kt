package org.macaul.kafkaflow

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CancellationException

class KafkaFlow<T>(
    private val bootStrap: String,
    deserializerClass: Class<out Deserializer<T>>
) {
    private val logger = KotlinLogging.logger {}

    private val kafkaProperties = Properties().apply {
        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap)
        set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass.name)
    }

    fun startFlow(vararg topic: String): Flow<T> {
        val kafkaConsumer = KafkaConsumer<ByteArray, T>(kafkaProperties)
        return flow {
            try {
                logger.info("Starting KafkaConsumer for $topic")
                val subscribedPartitions = topic.flatMap { kafkaConsumer.partitionsFor(it) }
                    .map { TopicPartition(it.topic(), it.partition()) }

                logger.info("assigning $subscribedPartitions")
                kafkaConsumer.assign(subscribedPartitions)
                kafkaConsumer.seekToEnd(subscribedPartitions)
                logger.info("Started KafkaConsumer for $topic")

                while (true) {
                    try {
                        logger.info("pre-poll")
                        val records = withContext(Dispatchers.IO){
                            kafkaConsumer.poll(Duration.ofSeconds(100)).also {
                                logger.info("post-Poll")
                            }
                        }

                        if (records.isEmpty) {
                            logger.info("no records")
                            yield()
                        } else {
                            logger.info("yay records")
                            records.forEach { emit(it.value()) }
                        }
                    } catch (e: RetriableException) {
                        logger.warn("Retryable Kafka exception. Delaying 5 seconds and retrying", e)
                        delay(5_000)
                    }
                }
//            } catch (e: WakeupException) {
//                logger.info("Shutting down")
            } catch (e: CancellationException) {
                logger.info("We were canceled")
            } finally {
                kafkaConsumer.close()
            }
        }
    }
}

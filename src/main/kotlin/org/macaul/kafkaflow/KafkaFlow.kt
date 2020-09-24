package org.macaul.kafkaflow

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CancellationException

public class KafkaFlow<T>(
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

    public fun startFlow(vararg topic: String): Flow<T> {
        val kafkaConsumer = KafkaConsumer<ByteArray, T>(kafkaProperties)
        val topics = topic.joinToString(",")
        logger.debug("Starting KafkaConsumer for $topics")
        val subscribedPartitions = topic.flatMap { kafkaConsumer.partitionsFor(it) }
            .map { TopicPartition(it.topic(), it.partition()) }

        logger.debug { "assigning $subscribedPartitions" }
        kafkaConsumer.assign(subscribedPartitions)
        kafkaConsumer.seekToEnd(subscribedPartitions)
        logger.debug { "Started KafkaConsumer for $topics" }
        return flow {
            try {

                while (true) {
                    try {
//                        val records = withContext(Dispatchers.IO){
                        val records = kafkaConsumer.poll(Duration.ofMillis(500))
//                        }

                        if (records.isEmpty) {
                            yield()
                        } else {
                            records.forEach { emit(it.value()) }
                        }
                    } catch (e: RetriableException) {
                        logger.warn(e) { "Retryable Kafka exception. Delaying 5 seconds and retrying" }
                        delay(5_000)
                    }
                }
            } catch (e: CancellationException) {
                logger.debug { "We were canceled" }
            } finally {
                kafkaConsumer.close()
            }
        }.flowOn(Dispatchers.IO)
    }
}

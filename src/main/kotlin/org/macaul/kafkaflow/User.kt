package org.macaul.kafkaflow

import org.apache.kafka.common.serialization.StringDeserializer

class User {
    fun go() {
        val test = KafkaFlow<String>("", StringDeserializer::class.java)
        val flowForTopic = test.flowForTopic("testTopic")
    }
}

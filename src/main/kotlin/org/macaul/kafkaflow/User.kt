package org.macaul.kafkaflow

import org.apache.kafka.common.serialization.StringDeserializer

class User {
    fun go() {
        val test = KafkaFlow<String>("", "testTopic", StringDeserializer::class.java)
        val flowForTopic = test.startFlow()
    }
}

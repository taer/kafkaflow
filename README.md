Small utility to retrieve current and future records from Kafka topics in a flow, written in [![Pure Kotlin](https://img.shields.io/badge/100%25-kotlin-blue.svg)](https://kotlinlang.org/).  

#### Attach to a topic and retrieve future messages
```Kotlin
val kafkaFlow = KafkaFlow<String>(bootstrapServers, StringDeserializer::class.java)
val twoMessagesFromTopic = kafkaFlow.startFlow("a.Topic").take(2).toList()
```

You can also simply stream "forever"
```Kotlin
val kafkaFlow = KafkaFlow<String>(bootstrapServers, StringDeserializer::class.java)

kafkaFlow.startFlow("a.Topic").collect {
    logger.info { "Just got this string from the topic: $it"}  
}
```


#### Get messages from the last time(TODO: Stateful)
```Kotlin
```
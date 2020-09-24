import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

/*
 * This file was generated by the Gradle "init" task.
 */

plugins {
    kotlin("jvm") version "1.4.10"
    id("org.jmailen.kotlinter") version "3.0.2"
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "1.8"
}

kotlinter {
    experimentalRules = true
}

kotlin {
    explicitApi()
}

repositories {
    mavenLocal()
    mavenCentral()
}

tasks.withType<Test> {
    useJUnitPlatform()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.9")
    implementation("org.apache.kafka:kafka-clients:2.6.0")
//    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.2")
    implementation("io.github.microutils:kotlin-logging:1.8.3")

    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.2.2")
    testImplementation("io.kotest:kotest-extensions-testcontainers-jvm:4.2.2")

    testImplementation("org.testcontainers:testcontainers:1.14.3")
    testImplementation("org.testcontainers:kafka:1.14.3")

    runtimeOnly("ch.qos.logback:logback-classic:1.2.3")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.2.3")
}

group = "org.macaul.kafka"
version = "1.0-SNAPSHOT"
description = "KafkaFlow"

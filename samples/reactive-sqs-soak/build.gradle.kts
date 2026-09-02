import org.gradle.api.tasks.bundling.Compression
import org.gradle.api.tasks.bundling.Tar

plugins {
    application
}

description = "Long-running AWS soak harness for reactive SQS acknowledgement safety"

dependencies {
    implementation(project(":reactive-sqs-spring-boot-3-starter"))
    implementation(platform(libs.spring.boot3.bom))
    implementation(platform(libs.aws.bom))
    implementation(platform(libs.reactor.bom))
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.aws.dynamodb)
    implementation(libs.jackson2.databind)
    implementation(libs.reactor.core)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("io.github.marcschmidt1999.reactive.sqs.soak.SoakApplication")
    applicationDefaultJvmArgs =
        listOf("-Xms32m", "-Xmx384m", "-XX:MaxDirectMemorySize=128m")
}

tasks.named<Tar>("distTar") {
    compression = Compression.GZIP
    archiveFileName.set("reactive-sqs-soak.tar.gz")
}

tasks.register<JavaExec>("soakProduce") {
    description = "Continuously sends auditable messages to a deployed soak-test stack"
    group = "soak"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("io.github.marcschmidt1999.reactive.sqs.soak.SoakProducerMain")
}

tasks.register<JavaExec>("soakReport") {
    description = "Prints a live, non-final snapshot from the DynamoDB audit ledger"
    group = "soak"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("io.github.marcschmidt1999.reactive.sqs.soak.SoakReportMain")
}

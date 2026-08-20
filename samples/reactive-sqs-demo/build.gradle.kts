plugins {
    application
}

description = "Runnable Spring Boot 3 service demonstrating reactive SQS listeners"

dependencies {
    implementation(project(":reactive-sqs-spring-boot-3-starter"))
    implementation(platform(libs.spring.boot3.bom))
    implementation(platform(libs.aws.bom))
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.boot.starter.webflux)

    runtimeOnly(libs.micrometer.registry.prometheus)
    runtimeOnly(libs.aws.sso)
    runtimeOnly(libs.aws.ssooidc)
    runtimeOnly(libs.aws.sts)

    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("io.github.marcschmidt1999.reactive.sqs.demo.DemoApplication")
}

val benchmarkSend = tasks.register<JavaExec>("benchmarkSend") {
    description = "Sends a batched benchmark workload to the configured SQS demo queue"
    group = "benchmark"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("io.github.marcschmidt1999.reactive.sqs.demo.BenchmarkProducerMain")
}

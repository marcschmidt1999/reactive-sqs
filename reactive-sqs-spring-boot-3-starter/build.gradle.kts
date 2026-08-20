description = "Spring Boot 3 starter for reactive SQS listeners with Jackson 2"

dependencies {
    api(project(":reactive-sqs-spring"))

    implementation(platform(libs.spring.boot3.bom))
    implementation(libs.spring.boot.autoconfigure)
    implementation(libs.jackson2.databind)
    compileOnly(libs.micrometer.core)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.micrometer.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

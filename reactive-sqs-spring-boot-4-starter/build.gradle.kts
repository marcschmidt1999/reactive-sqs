description = "Spring Boot 4 starter for reactive SQS listeners with Jackson 3"

dependencies {
    api(project(":reactive-sqs-spring"))

    implementation(platform(libs.spring.boot4.bom))
    implementation(libs.spring.boot.autoconfigure)
    implementation(libs.jackson3.databind)
    compileOnly(libs.micrometer.core)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.micrometer.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

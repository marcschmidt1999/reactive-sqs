description = "Spring annotation and lifecycle integration for reactive SQS listeners"

dependencies {
    api(project(":reactive-sqs-core"))

    compileOnly(libs.spring.context)
    compileOnly(platform(libs.reactor.bom))
    compileOnly(libs.reactor.core)

    testImplementation(platform(libs.spring.boot3.bom))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

description = "Bounded reactive SQS receive, visibility, settlement, and lifecycle engine"

dependencies {
    compileOnly(platform(libs.reactor.bom))
    compileOnly(libs.reactor.core)

    api(platform(libs.aws.bom))
    api(libs.aws.sqs)

    implementation("org.slf4j:slf4j-api:2.0.18")

    testImplementation(platform(libs.junit5.bom))
    testImplementation(platform(libs.reactor.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.reactor.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

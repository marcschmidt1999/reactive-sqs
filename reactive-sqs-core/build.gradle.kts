description = "Bounded reactive SQS receive, visibility, settlement, and lifecycle engine"

dependencies {
    api(platform(libs.reactor.bom))
    api(libs.reactor.core)

    api(platform(libs.aws.bom))
    api(libs.aws.sqs)

    implementation("org.slf4j:slf4j-api:2.0.18")

    testImplementation(platform(libs.junit5.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

plugins {
    id("io.github.ben-manes.versions.settings") version "0.61.0"
}

rootProject.name = "reactive-sqs"

include(
    "reactive-sqs-core",
    "reactive-sqs-spring",
    "reactive-sqs-spring-boot-3-starter",
    "reactive-sqs-spring-boot-4-starter",
    "reactive-sqs-demo",
    "reactive-sqs-soak",
)

project(":reactive-sqs-demo").projectDir = file("samples/reactive-sqs-demo")
project(":reactive-sqs-soak").projectDir = file("samples/reactive-sqs-soak")

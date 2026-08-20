rootProject.name = "reactive-sqs"

include(
    "reactive-sqs-core",
    "reactive-sqs-spring",
    "reactive-sqs-spring-boot-3-starter",
    "reactive-sqs-spring-boot-4-starter",
    "reactive-sqs-demo",
)

project(":reactive-sqs-demo").projectDir = file("samples/reactive-sqs-demo")

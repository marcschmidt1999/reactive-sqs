import java.util.zip.ZipFile

description = "Spring Boot 3 starter for reactive SQS listeners with Jackson 2"

val java17CompatibleJars =
    listOf(
        tasks.named<Jar>("jar"),
        project(":reactive-sqs-spring").tasks.named<Jar>("jar"),
        project(":reactive-sqs-core").tasks.named<Jar>("jar"),
    )

val verifyJava17Compatibility =
    tasks.register("verifyJava17Compatibility") {
        group = "verification"
        description = "Verifies that the Boot 3 starter and its project dependencies use Java 17 bytecode."
        dependsOn(java17CompatibleJars)
        inputs.files(java17CompatibleJars.map { it.flatMap(Jar::getArchiveFile) })

        doLast {
            val maximumJava17ClassVersion = 61
            inputs.files.files.forEach { archive ->
                ZipFile(archive).use { zip ->
                    zip.entries()
                        .asSequence()
                        .filter { !it.isDirectory && it.name.endsWith(".class") }
                        .forEach { entry ->
                            val header = zip.getInputStream(entry).use { it.readNBytes(8) }
                            check(header.size == 8) { "Invalid class file ${entry.name} in $archive" }
                            val classVersion =
                                ((header[6].toInt() and 0xff) shl 8) or (header[7].toInt() and 0xff)
                            check(classVersion <= maximumJava17ClassVersion) {
                                "${entry.name} in $archive requires class-file version $classVersion; Java 17 supports up to $maximumJava17ClassVersion"
                            }
                        }
                }
            }
        }
    }

tasks.named("check") {
    dependsOn(verifyJava17Compatibility)
}

dependencies {
    api(project(":reactive-sqs-spring"))

    implementation(platform(libs.spring.boot3.bom))
    implementation(libs.spring.boot.autoconfigure)
    implementation(libs.reactor.core)
    implementation(libs.jackson2.databind)
    compileOnly(libs.micrometer.core)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.micrometer.core)
    testImplementation(libs.reactor.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

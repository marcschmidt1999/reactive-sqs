import com.diffplug.gradle.spotless.SpotlessExtension
import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsExtension
import com.github.spotbugs.snom.SpotBugsTask
import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import java.math.BigDecimal
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.repositories.PasswordCredentials
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.jvm.tasks.Jar
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    base
    id("com.diffplug.spotless") version "8.10.0"
    id("com.github.spotbugs") version "6.5.11" apply false
}

group = "io.github.marcschmidt1999"
version = providers.gradleProperty("version").orElse("0.1.0-SNAPSHOT").get()

val mockitoCore = libs.mockito.core
val jacocoToolVersion = libs.versions.jacoco.get()
val unstableVersion =
    Regex("(?i)(?:^|[.-])(alpha|beta|rc|cr|m|milestone|preview|snapshot|ea)\\d*(?:$|[.-])")
val publishedLibraryPaths =
    setOf(
        ":reactive-sqs-core",
        ":reactive-sqs-spring",
        ":reactive-sqs-spring-boot-3-starter",
        ":reactive-sqs-spring-boot-4-starter",
    )

tasks.named<DependencyUpdatesTask>("dependencyUpdates") {
    revision = "release"
    rejectVersionIf {
        unstableVersion.containsMatchIn(candidate.version)
    }
}

spotless {
    format("buildFiles") {
        target(
            "*.gradle.kts",
            "*.md",
            "docs/**/*.md",
            ".github/**/*.yml",
            ".github/**/*.yaml",
            "reactive-sqs-*/build.gradle.kts",
            "reactive-sqs-*/src/**/*.md",
            "reactive-sqs-*/src/**/*.yml",
            "reactive-sqs-*/src/**/*.yaml",
            "samples/*/build.gradle.kts",
            "samples/*/README.md",
            "samples/*/src/**/*.md",
            "samples/*/src/**/*.yml",
            "samples/*/src/**/*.yaml",
        )
        trimTrailingWhitespace()
        endWithNewline()
    }
}

allprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }
}

subprojects {
    val publishedLibrary = path in publishedLibraryPaths
    val java17CompatibleLibrary =
        path in
            setOf(
                ":reactive-sqs-core",
                ":reactive-sqs-spring",
                ":reactive-sqs-spring-boot-3-starter",
            )

    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")
    if (publishedLibrary) {
        apply(plugin = "maven-publish")
    }

    dependencyLocking {
        lockAllConfigurations()
    }

    val mockitoAgent = configurations.create("mockitoAgent") {
        isCanBeConsumed = false
        isCanBeResolved = true
    }
    val publishedDescription = providers.provider {
        project.description ?: "Reactive SQS listener for Spring applications"
    }

    dependencies {
        add("testImplementation", mockitoCore)
        val agentDependency = create(mockitoCore.get()) as ModuleDependency
        agentDependency.isTransitive = false
        add(mockitoAgent.name, agentDependency)
    }

    extensions.configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(if (java17CompatibleLibrary) 17 else 21)
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror", "-parameters"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("-javaagent:${mockitoAgent.singleFile.absolutePath}")
    }

    extensions.configure<JacocoPluginExtension> {
        toolVersion = jacocoToolVersion
    }

    val jacocoTestReport = tasks.named<JacocoReport>("jacocoTestReport") {
        dependsOn(tasks.named("test"))
        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
    }

    val jacocoTestCoverageVerification =
        tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
            dependsOn(tasks.named("test"))
            violationRules {
                rule {
                    limit {
                        counter = "LINE"
                        value = "COVEREDRATIO"
                        minimum = BigDecimal("0.75")
                    }
                }
            }
        }

    tasks.named("check") {
        dependsOn(jacocoTestReport, jacocoTestCoverageVerification)
    }

    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
    }

    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }

    tasks.named<Jar>("jar") {
        manifest.attributes(
            "Implementation-Title" to project.name,
            "Implementation-Vendor" to "Marc Schmidt",
            "Implementation-Version" to project.version,
        )
    }

    extensions.configure<SpotlessExtension> {
        java {
            googleJavaFormat("1.30.0").aosp()
            formatAnnotations()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    extensions.configure<SpotBugsExtension> {
        ignoreFailures.set(false)
        effort.set(Effort.MAX)
        reportLevel.set(Confidence.MEDIUM)
    }

    tasks.withType<SpotBugsTask>().configureEach {
        reports.create("html") {
            required.set(true)
        }
    }

    if (publishedLibrary) {
        extensions.configure<PublishingExtension> {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])
                    pom {
                        name.set(project.name)
                        description.set(publishedDescription)
                        url.set("https://github.com/marcschmidt1999/reactive-sqs")
                        licenses {
                            license {
                                name.set("Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                                distribution.set("repo")
                            }
                        }
                        developers {
                            developer {
                                id.set("marcschmidt1999")
                                name.set("Marc Schmidt")
                                organization.set("Marc Schmidt")
                                organizationUrl.set("https://github.com/marcschmidt1999")
                            }
                        }
                        scm {
                            connection.set(
                                "scm:git:https://github.com/marcschmidt1999/reactive-sqs.git",
                            )
                            developerConnection.set(
                                "scm:git:ssh://git@github.com/marcschmidt1999/reactive-sqs.git",
                            )
                            url.set("https://github.com/marcschmidt1999/reactive-sqs")
                        }
                    }
                }
            }
            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/marcschmidt1999/reactive-sqs")
                    credentials(PasswordCredentials::class) {
                        username = providers.environmentVariable("GITHUB_ACTOR").orNull
                        password = providers.environmentVariable("GITHUB_TOKEN").orNull
                    }
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(subprojects.map { "${it.path}:check" })
}

tasks.named("assemble") {
    dependsOn(subprojects.map { "${it.path}:assemble" })
}

tasks.register("publishGithubPackages") {
    group = "publishing"
    description = "Publishes library artifacts to GitHub Packages."
    dependsOn(
        publishedLibraryPaths.map {
            "$it:publishAllPublicationsToGitHubPackagesRepository"
        },
    )
}

# Major-version migration plan

This project supports two Spring Boot generations through separate starter artifacts:

| Area | Current support | Migration constraint |
| --- | --- | --- |
| `reactive-sqs-core` | Reactor BOM 2024.0.18 (Reactor Core 3.7.x) | It is framework-free but exposes Reactor types. |
| `reactive-sqs-spring` | Compiled against Spring Framework 6.2.19 | It is shared by both starters, publishes no Spring version, and must stay binary-compatible with the supported Spring lines. |
| `reactive-sqs-spring-boot-3-starter` | Boot 3.5.16, Jackson 2 | Must remain on Spring Framework 6.2 and Reactor 3.7. |
| `reactive-sqs-spring-boot-4-starter` | Boot 4.1.1, Jackson 3 | Uses Spring Framework 7 and Reactor 3.8. |
| Demo | Boot 3.5.16 | Keep it as the Boot 3 integration test; add a separate Boot 4 demo only if a runnable example is needed. |

The core and Spring modules are published libraries. Do not let either export an exact Spring or Reactor version that overrides an application's Boot BOM. The application BOM should select its matching Spring and Reactor versions.

## Recommended order

1. Keep the Boot 4 starter on the current 4.1.x maintenance release, and test it independently.
2. Make the shared Spring integration explicitly compatible with both Spring Framework 6.2 and 7.0, with one test path for each starter.
3. Upgrade Reactor for the Boot 4 path to 2025.0.x. Keep the Boot 3 path on the Reactor version managed by Boot 3.5.
4. Upgrade the standalone test toolchain to JUnit 6 only after confirming that the Boot 3 and Boot 4 test BOMs resolve a compatible version. Spring Framework 7 itself uses JUnit 6, but this does not require Boot 3 consumers to do so.

Every migration needs a full build, both starter integration test suites, dependency locking, and dependency-verification metadata refresh. The SQS demo should also be exercised against a real queue for receive, visibility renewal, failed handler redelivery, and batched deletion.

## Spring Framework 6.2 to 7.0

**Scope:** the shared `reactive-sqs-spring` module and the Boot 4 starter, not the Boot 3 starter.

Spring Framework 7 requires Java 17 and moves to a Jakarta EE 11 baseline (including Servlet 6.1). It removes `javax.annotation` and `javax.inject` support, removes `spring-jcl`, and changes its primary JSON support to Jackson 3. This project already targets Java 21 and uses neither removed `javax.*` namespace, so the direct source risk is low. The important risk is binary and dependency compatibility between the shared registrar and both framework lines. [Spring Framework 7 release notes](https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-7.0-Release-Notes)

Work required:

- Do not change the single `spring-framework` catalog version from 6.2 to 7.0 while Boot 3 is supported. That would make the Boot 3 starter resolve against an unsupported framework generation.
- Compile the shared Spring module against the oldest supported Spring line (6.2) and test it through both starters, or split framework-specific code if a Spring 7-only API becomes necessary.
- Do not publish Spring Framework as an exact transitive version from the shared module. It uses a consumer-managed `compileOnly` dependency, while each starter's Boot BOM supplies the runtime version.
- Keep the existing two payload converters: Boot 3 accepts Jackson 2 `ObjectMapper`; Boot 4 accepts Jackson 3 `JsonMapper`. Spring Framework 7's Jackson 2 support is deprecated and planned for removal after 7.1, so the Boot 4 starter must not regress to Jackson 2. [Spring Framework 7 Jackson notes](https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-7.0-Release-Notes#jackson-3x-support)
- Add a compatibility matrix in CI: Boot 3 starter tests on Spring 6.2/Jackson 2 and Boot 4 starter tests on Spring 7/Jackson 3. Do not put both starters on one application classpath: both export auto-configuration for the same feature but deliberately use different JSON mapper types.

## Spring Boot 3.5 to 4.1, while retaining Boot 3 support

**Scope:** `reactive-sqs-spring-boot-4-starter` runs Boot 4.1.1. Boot 3 remains a separately released supported artifact.

Boot 4.1 requires Java 17, Spring Framework 7.0.8 or later, and Gradle 8.14+ or 9.x; the project already uses Java 21 and Gradle 9.7.1. [Boot 4.1 system requirements](https://docs.spring.io/spring-boot/system-requirements.html) Boot 4 is based on Jakarta EE 11, removes Boot 3 deprecations, and has a more modular dependency layout. [Boot 4 migration guide](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide)

Work required:

- Update only `spring-boot4` for Boot 4 maintenance releases. Keep `spring-boot3` at the maintained 3.5.x release.
- Compare Boot 4.0 and 4.1 managed dependencies before removing any explicit dependency. In particular, the Boot 4.1 BOM manages Reactor Core 3.8.6. [Boot 4.1 managed coordinates](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html)
- Run the Boot 4 starter's auto-configuration and Micrometer tests after the upgrade. The configuration imports file format remains the current `AutoConfiguration.imports` mechanism, so no discovery migration is expected.
- Check the demo's `application.yml` with Boot's properties migrator during a temporary test run if it is moved to Boot 4. Remove the migrator afterward; it is a migration aid, not a runtime dependency. [Boot migration guide: properties migrator](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#configuration-properties-migration)
- Test Jackson mapping semantics, not merely context startup. Boot 4 defaults to Jackson 3, detects all mapper modules by default, and has changed property names/defaults. Boot can temporarily provide a deprecated Jackson 2 bridge, but this library already has the better long-term split: one starter per mapper generation. [Boot migration guide: Jackson 2 compatibility](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#jackson-2-compatibility)

## Reactor BOM 2024.0 to 2025.0

**Scope:** core runtime behavior and both starters' resolved dependency graph.

Reactor 2024.0.18 resolves Reactor Core 3.7.x. Reactor 2025.0.6 resolves Reactor Core 3.8.6, and Reactor states that 3.7.19 was the last 3.7 release because the 2024.0 train is out of OSS support. [Reactor Core releases](https://github.com/reactor/reactor-core/releases) The BOM is designed to select a coherent Reactor release train and should be imported as a Gradle platform. [Reactor BOM usage](https://github.com/reactor/reactor#using-the-bom-with-gradle)

Work required:

- Do not set `reactor=2025.0.x` globally while promising Boot 3.5 support. Boot 3.5 manages the 2024.0/3.7 line, while Boot 4.1 manages Reactor Core 3.8.6. Let each starter's Boot BOM select the version that it supports.
- If `reactive-sqs-core` is also published for non-Boot use, document the supported Reactor ranges and avoid forcing its BOM onto consumers. Test the oldest supported 3.7 line and the current 3.8 line.
- Review source and public API for `reactor.util.annotation.*`. Reactor 3.8 deprecates those annotations in favor of JSpecify; ordinary Reactor use is source-compatible, but nullness annotations need type-use migration. This project currently has no such imports. [Reactor null-safety documentation](https://projectreactor.io/docs/core/release/reference/advancedFeatures/null-safety.html)
- Treat this as a behavioral migration: run the listener lifecycle, bounded in-flight/backpressure, cancellation, visibility extension, retry, telemetry, and batch-delete tests. The main risk is changed scheduling/backpressure behavior rather than a broad source rewrite.

## JUnit 5 to 6

**Scope:** test dependencies and developer tooling only; no production artifact should expose JUnit.

JUnit 6 requires Java 17 and aligns Platform, Jupiter, and Vintage modules on the same version number. It removes long-deprecated APIs and modules such as `junit-platform-runner` and `junit-platform-jfr`; JFR functionality is in the launcher. [JUnit 6 release notes](https://docs.junit.org/6.0.0/release-notes.html) The project already compiles and tests on Java 21, uses Jupiter plus `junit-platform-launcher`, and has no custom JUnit extension or removed module usage.

Work required:

- Change the JUnit BOM version to a chosen stable 6.x release, then refresh locks and verification metadata.
- Confirm each module's resolved test graph. Boot starter test dependencies may manage JUnit themselves, so do not force a conflicting version ahead of the Boot 3/4 compatibility checks.
- Re-run all tests. Pay attention to test order: JUnit 6 makes nested-class ordering deterministic and changes default ordering behavior. CSV parameterized tests should also be reviewed if added later. [JUnit 6 release notes: breaking changes](https://docs.junit.org/6.0.0/release-notes.html#deprecations-and-breaking-changes)
- Keep any future custom extension code free of removed/deprecated APIs and account for JSpecify nullness declarations in JUnit 6.

## Release decision

Publish the Boot 4.1/Reactor 3.8 support as a minor library release only after the compatibility matrix passes. Do not remove the Boot 3 starter until its declared support window ends. Removal of Boot 3 support, or changing the shared Spring module so it requires Spring 7, is a breaking change and requires the next major library release.

# Contributing

## Requirements

- JDK 21+ for the full project build
- JDK 17 for the Boot 3 compatibility check
- Use the checked-in Gradle wrapper
- Do not commit `build/` files

## Make changes with tests

Use this loop for behavior changes:

1. Add one focused test through a public API or a clear internal test seam.
2. Run it and confirm that it fails for the expected reason.
3. Make the smallest change that passes the test.
4. Clean up only while tests stay green.
5. Run `./gradlew check` before opening a pull request.

The Boot 3 compatibility check is:

```shell
./gradlew java17CompatibilityCheck
```

Mock only external systems such as `SqsAsyncClient`. Use Reactor virtual time for retries,
visibility, timeouts, and shutdown. Do not use sleeps in tests.

## Compatibility

Changing the annotation, `SqsMessage<T>`, artifact names, defaults, acknowledgement behavior, or
lifecycle order is a public change. Document it in [CHANGELOG.md](CHANGELOG.md). Add Boot 3 and
Boot 4 tests when the change affects both starters.

## Pull requests

- Explain the SQS behavior you changed.
- Include the failing and passing test evidence.
- Do not put AWS credentials, queue URLs, receipt handles, or payloads in logs or test data.
- Do not add FIFO support without tests for FIFO ordering and standard fair queues.
- Do not claim exactly-once processing.

See [SECURITY.md](SECURITY.md) for security reports.

package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Duration;

@FunctionalInterface
interface RetryDelay {

    void pause(Duration duration);
}

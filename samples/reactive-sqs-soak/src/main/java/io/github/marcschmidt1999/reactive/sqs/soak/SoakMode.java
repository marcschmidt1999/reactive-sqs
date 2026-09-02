package io.github.marcschmidt1999.reactive.sqs.soak;

public enum SoakMode {
    NORMAL,
    RETRY_ONCE,
    POISON,
    BOUNDARY
}

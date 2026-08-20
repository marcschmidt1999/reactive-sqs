package io.github.marcschmidt1999.reactive.sqs.demo;

import java.util.Objects;

/** JSON payload used by the real-queue demo. */
public record DemoMessage(String id, String text, boolean shouldFail) {

    public DemoMessage {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(text, "text must not be null");
        if (id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (text.isBlank()) {
            throw new IllegalArgumentException("text must not be blank");
        }
    }
}

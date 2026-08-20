package io.github.marcschmidt1999.reactive.sqs.internal;

/** Internal marker used to classify payload conversion failures without metric error tags. */
public final class SqsMessageMappingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SqsMessageMappingException(Throwable cause) {
        super("Failed to map the SQS message body", cause);
    }
}

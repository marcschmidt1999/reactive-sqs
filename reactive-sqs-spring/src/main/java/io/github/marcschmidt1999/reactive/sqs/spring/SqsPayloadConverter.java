package io.github.marcschmidt1999.reactive.sqs.spring;

import java.lang.reflect.Type;

/** Converts an SQS message body to an annotated listener's declared parameter type. */
@FunctionalInterface
public interface SqsPayloadConverter {

    /**
     * Convert a message body.
     *
     * @param body raw SQS message body
     * @param targetType listener parameter type
     * @return converted payload
     * @throws Exception when conversion fails
     */
    Object convert(String body, Type targetType) throws Exception;
}

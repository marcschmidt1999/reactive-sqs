package io.github.marcschmidt1999.reactive.sqs.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a reactive method as a listener for one Amazon SQS queue. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ReactiveSqsListener {

    /**
     * Queue URL, or a Spring property placeholder resolving to a queue URL.
     *
     * @return configured queue URL
     */
    String queue();

    /**
     * SQS visibility timeout applied on receive and renewed while processing is active.
     *
     * @return visibility timeout in seconds
     */
    int visibilityTimeoutSeconds() default 60;

    /**
     * Maximum number of receive reservations and unsettled messages owned by this listener.
     *
     * @return bounded in-flight capacity
     */
    int maxInFlight() default 1;

    /**
     * Time Spring shutdown waits for handlers and settlement before cancelling unfinished work.
     *
     * @return graceful shutdown period in seconds
     */
    int shutdownGraceSeconds() default 30;

    /**
     * Maximum time allowed for payload conversion and handler completion. A timeout is not
     * acknowledged and is therefore eligible for SQS redelivery.
     *
     * @return processing limit in seconds
     */
    int maxProcessingDurationSeconds() default 3_600;
}

package io.github.marcschmidt1999.reactive.sqs.boot4.autoconfigure;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/** Optional Micrometer instrumentation for reactive SQS listeners. */
@AutoConfiguration(before = ReactiveSqsAutoConfiguration.class)
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(
        prefix = "reactive-sqs.metrics",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class ReactiveSqsMetricsAutoConfiguration {

    @Bean("reactiveSqsMicrometerTelemetryFactory")
    @ConditionalOnMissingBean(name = "micrometerSqsListenerTelemetryFactory")
    SqsListenerTelemetryFactory reactiveSqsListenerTelemetryFactory(
            ObjectProvider<MeterRegistry> meterRegistry) {
        var registry = meterRegistry.getIfAvailable();
        return registry == null
                ? SqsListenerTelemetryFactory.noOp()
                : new MicrometerSqsListenerTelemetryFactory(registry);
    }
}

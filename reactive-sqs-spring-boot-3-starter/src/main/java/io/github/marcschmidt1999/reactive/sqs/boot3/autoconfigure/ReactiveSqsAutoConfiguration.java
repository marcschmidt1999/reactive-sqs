package io.github.marcschmidt1999.reactive.sqs.boot3.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import io.github.marcschmidt1999.reactive.sqs.spring.ReactiveSqsListenerRegistrar;
import io.github.marcschmidt1999.reactive.sqs.spring.SqsPayloadConverter;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/** Spring Boot 3 auto-configuration for reactive SQS listeners. */
@AutoConfiguration
@ConditionalOnClass({SqsAsyncClient.class, Mono.class, ObjectMapper.class})
@ConditionalOnBean(SqsAsyncClient.class)
@ConditionalOnProperty(
        prefix = "reactive-sqs",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class ReactiveSqsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    SqsPayloadConverter reactiveSqsPayloadConverter(ObjectMapper objectMapper) {
        return (body, targetType) ->
                objectMapper.readValue(body, objectMapper.constructType(targetType));
    }

    @Bean
    ReactiveSqsListenerRegistrar reactiveSqsListenerRegistrar(
            ListableBeanFactory beanFactory,
            Environment environment,
            SqsAsyncClient sqsClient,
            SqsPayloadConverter payloadConverter,
            ObjectProvider<SqsListenerTelemetryFactory> telemetryFactories) {
        return new ReactiveSqsListenerRegistrar(
                beanFactory,
                environment,
                sqsClient,
                payloadConverter,
                SqsListenerTelemetryFactory.composite(telemetryFactories.orderedStream().toList()));
    }
}

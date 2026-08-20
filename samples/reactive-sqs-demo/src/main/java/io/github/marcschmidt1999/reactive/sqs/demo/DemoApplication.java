package io.github.marcschmidt1999.reactive.sqs.demo;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    SqsAsyncClient sqsAsyncClient(@Value("${demo.aws-region}") String awsRegion) {
        return createSqsClient(awsRegion);
    }

    @Bean
    @ConditionalOnMissingBean
    DemoPerformanceMeasurementWindow demoPerformanceMeasurementWindow() {
        return new DemoPerformanceMeasurementWindow();
    }

    @Bean("demoPerformanceMeasurementTelemetry")
    @ConditionalOnMissingBean(name = "demoPerformanceMeasurementWindowTelemetry")
    SqsListenerTelemetryFactory demoPerformanceMeasurementWindowTelemetry(
            DemoPerformanceMeasurementWindow measurementWindow) {
        return measurementWindow::create;
    }

    static SqsAsyncClient createSqsClient(String awsRegion) {
        var retryStrategy = StandardRetryStrategy.builder().maxAttempts(3).build();
        return SqsAsyncClient.builder()
                .region(Region.of(awsRegion))
                .overrideConfiguration(
                        configuration ->
                                configuration
                                        .apiCallAttemptTimeout(Duration.ofSeconds(25))
                                        .apiCallTimeout(Duration.ofSeconds(30))
                                        .retryStrategy(retryStrategy))
                .build();
    }
}

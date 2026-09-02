package io.github.marcschmidt1999.reactive.sqs.soak;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@SpringBootApplication
public class SoakApplication {

    public static void main(String[] args) {
        SpringApplication.run(SoakApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    SqsAsyncClient sqsAsyncClient(@Value("${soak.region}") String region) {
        return sqsClient(region);
    }

    @Bean(destroyMethod = "close")
    DynamoDbAsyncClient dynamoDbAsyncClient(@Value("${soak.region}") String region) {
        return dynamoClient(region);
    }

    @Bean
    AuditStore auditStore(
            DynamoDbAsyncClient client, @Value("${soak.table-name}") String tableName) {
        return new DynamoAuditStore(client, tableName);
    }

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }

    @Bean
    CpuOperation cpuOperation() {
        return new BusyCpuOperation();
    }

    @Bean(destroyMethod = "dispose")
    Scheduler soakCpuScheduler() {
        return Schedulers.newParallel("soak-cpu", 2, true);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    static SqsAsyncClient sqsClient(String region) {
        return SqsAsyncClient.builder()
                .region(Region.of(region))
                .overrideConfiguration(
                        configuration ->
                                configuration
                                        .apiCallAttemptTimeout(Duration.ofSeconds(25))
                                        .apiCallTimeout(Duration.ofSeconds(30))
                                        .retryStrategy(
                                                StandardRetryStrategy.builder()
                                                        .maxAttempts(3)
                                                        .build()))
                .build();
    }

    static DynamoDbAsyncClient dynamoClient(String region) {
        return DynamoDbAsyncClient.builder()
                .region(Region.of(region))
                .overrideConfiguration(
                        configuration ->
                                configuration
                                        .apiCallAttemptTimeout(Duration.ofSeconds(10))
                                        .apiCallTimeout(Duration.ofSeconds(15))
                                        .retryStrategy(
                                                StandardRetryStrategy.builder()
                                                        .maxAttempts(3)
                                                        .build()))
                .build();
    }
}

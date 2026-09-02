package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.ClassPathResource;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

class SoakApplicationTest {

    @Test
    void applicationBuildsRuntimeComponentsWithoutStaticCredentials() {
        var application = new SoakApplication();
        try (var sqs = application.sqsAsyncClient("eu-central-1");
                var dynamo = application.dynamoDbAsyncClient("eu-central-1")) {
            assertThat(sqs.serviceName()).isEqualTo("sqs");
            assertThat(dynamo.serviceName()).isEqualTo("dynamodb");
        }
        assertThat(application.auditStore(mock(DynamoDbAsyncClient.class), "ledger"))
                .isInstanceOf(DynamoAuditStore.class);
        assertThat(application.objectMapper()).isNotNull();
        assertThat(application.cpuOperation()).isInstanceOf(BusyCpuOperation.class);
        assertThat(application.clock().instant()).isBeforeOrEqualTo(Instant.now());
        var scheduler = application.soakCpuScheduler();
        assertThat(scheduler.isDisposed()).isFalse();
        scheduler.dispose();
    }

    @Test
    void cpuOperationDoesRealBoundedWorkAndHonorsCancellation() {
        var operation = new BusyCpuOperation();

        assertThat(operation.run(1, 42)).isNotBlank();
        assertThatThrownBy(() -> operation.run(0, 42)).isInstanceOf(IllegalArgumentException.class);

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> operation.run(10, 42))
                    .isInstanceOf(CancellationException.class);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void nonWebConsumerIsConfiguredToStayAliveAfterStartup() throws Exception {
        var properties =
                new YamlPropertySourceLoader()
                        .load("soak-test", new ClassPathResource("application.yml"))
                        .get(0);

        assertThat(properties.getProperty("spring.main.keep-alive")).isEqualTo(true);
    }
}

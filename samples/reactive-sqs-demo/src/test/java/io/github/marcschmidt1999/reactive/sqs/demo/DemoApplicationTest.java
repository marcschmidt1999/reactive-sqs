package io.github.marcschmidt1999.reactive.sqs.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@ExtendWith(OutputCaptureExtension.class)
class DemoApplicationTest {

    private static final String QUEUE_URL = "https://queue.example.test/reactive-sqs-demo-test";

    @Test
    void applicationCreatesTheCallerOwnedAwsClientWithoutContactingAws() {
        new ApplicationContextRunner()
                .withUserConfiguration(DemoApplication.class)
                .withPropertyValues("reactive-sqs.enabled=false", "demo.aws-region=eu-central-1")
                .run(
                        context ->
                                assertThat(context)
                                        .hasNotFailed()
                                        .hasSingleBean(SqsAsyncClient.class));
    }

    @Test
    void requiresAnExplicitDemoQueueUrl() {
        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void typedAnnotationConsumesAndAcknowledgesAQueueMessage(CapturedOutput output) {
        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("demo.queue-url=" + QUEUE_URL, "demo.aws-region=eu-central-1")
                .run(
                        context -> {
                            var sqs = context.getBean(SqsProbe.class);
                            var delivery =
                                    context.getBean(DemoPerformanceMeasurementWindow.class)
                                            .nextDelivery();

                            sqs.deliver(
                                    "{\"id\":\"order-42\",\"text\":\"hello from aws-cli\",\"shouldFail\":false}");

                            assertThat(sqs.receiveRequest)
                                    .succeedsWithin(Duration.ofSeconds(2))
                                    .satisfies(
                                            request ->
                                                    assertThat(request.queueUrl())
                                                            .isEqualTo(QUEUE_URL));
                            assertThat(sqs.deleteRequest)
                                    .succeedsWithin(Duration.ofSeconds(2))
                                    .satisfies(
                                            request ->
                                                    assertThat(request.entries())
                                                            .singleElement()
                                                            .extracting(
                                                                    entry -> entry.receiptHandle())
                                                            .isEqualTo("receipt-42"));
                            assertThat(sqs.nextReceiveRequest)
                                    .succeedsWithin(Duration.ofSeconds(2));
                            assertThat(delivery).succeedsWithin(Duration.ofSeconds(2));
                            assertThat(output)
                                    .contains("Processed demo message message-42")
                                    .doesNotContain("order-42", "hello from aws-cli");
                            assertThat(
                                            context.getBean(DemoPerformanceMeasurementWindow.class)
                                                    .snapshot()
                                                    .deliveries()
                                                    .get("acknowledged"))
                                    .isEqualTo(1);
                        });
    }

    @Test
    void requestedFailureLeavesTheMessageForRedelivery(CapturedOutput output) {
        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("demo.queue-url=" + QUEUE_URL, "demo.aws-region=eu-central-1")
                .run(
                        context -> {
                            var sqs = context.getBean(SqsProbe.class);

                            sqs.deliver(
                                    "{\"id\":\"order-13\",\"text\":\"please fail\",\"shouldFail\":true}");

                            assertThat(sqs.nextReceiveRequest)
                                    .succeedsWithin(Duration.ofSeconds(2));
                            assertThat(sqs.deleteRequest).isNotCompleted();
                            assertThat(output)
                                    .contains(
                                            "Demo handler deliberately failing message message-42")
                                    .doesNotContain("order-13", "please fail");
                        });
    }

    @Test
    void missingRequiredPayloadFieldLeavesTheMessageForRedelivery() {
        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("demo.queue-url=" + QUEUE_URL, "demo.aws-region=eu-central-1")
                .run(
                        context -> {
                            var sqs = context.getBean(SqsProbe.class);

                            sqs.deliver("{\"text\":\"missing id\",\"shouldFail\":false}");

                            assertThat(sqs.nextReceiveRequest)
                                    .succeedsWithin(Duration.ofSeconds(2));
                            assertThat(sqs.deleteRequest).isNotCompleted();
                        });
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import({
        DemoMessageListener.class,
        FakeSqsConfiguration.class,
        DemoPerformanceConfiguration.class
    })
    static class TestApplication {}

    @TestConfiguration(proxyBeanMethods = false)
    static class DemoPerformanceConfiguration {

        @Bean
        DemoPerformanceMeasurementWindow demoPerformanceMeasurementWindow() {
            return new DemoPerformanceMeasurementWindow();
        }

        @Bean
        SqsListenerTelemetryFactory demoPerformanceMeasurementWindowTelemetry(
                DemoPerformanceMeasurementWindow measurementWindow) {
            return measurementWindow::create;
        }

        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    static class FakeSqsConfiguration {

        @Bean
        SqsProbe sqsProbe() {
            return new SqsProbe();
        }

        @Bean
        SqsAsyncClient sqsAsyncClient(SqsProbe probe) {
            return probe.client;
        }
    }

    static final class SqsProbe {

        private final SqsAsyncClient client = mock(SqsAsyncClient.class);
        private final CompletableFuture<ReceiveMessageResponse> firstReceive =
                new CompletableFuture<>();
        private final CompletableFuture<ReceiveMessageResponse> pendingReceive =
                new CompletableFuture<>();
        private final CompletableFuture<ReceiveMessageRequest> receiveRequest =
                new CompletableFuture<>();
        private final CompletableFuture<ReceiveMessageRequest> nextReceiveRequest =
                new CompletableFuture<>();
        private final CompletableFuture<DeleteMessageBatchRequest> deleteRequest =
                new CompletableFuture<>();

        private SqsProbe() {
            when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                    .thenAnswer(
                            invocation -> {
                                if (receiveRequest.complete(invocation.getArgument(0))) {
                                    return firstReceive;
                                }
                                nextReceiveRequest.complete(invocation.getArgument(0));
                                return pendingReceive;
                            });
            when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                    .thenAnswer(
                            invocation -> {
                                var request =
                                        invocation.getArgument(0, DeleteMessageBatchRequest.class);
                                deleteRequest.complete(request);
                                return CompletableFuture.completedFuture(
                                        successfulDeleteResponse(request));
                            });
        }

        private void deliver(String body) {
            firstReceive.complete(
                    ReceiveMessageResponse.builder()
                            .messages(
                                    Message.builder()
                                            .messageId("message-42")
                                            .receiptHandle("receipt-42")
                                            .body(body)
                                            .build())
                            .build());
        }

        private static DeleteMessageBatchResponse successfulDeleteResponse(
                DeleteMessageBatchRequest request) {
            return DeleteMessageBatchResponse.builder()
                    .successful(
                            request.entries().stream()
                                    .map(
                                            entry ->
                                                    DeleteMessageBatchResultEntry.builder()
                                                            .id(entry.id())
                                                            .build())
                                    .toList())
                    .build();
        }
    }
}

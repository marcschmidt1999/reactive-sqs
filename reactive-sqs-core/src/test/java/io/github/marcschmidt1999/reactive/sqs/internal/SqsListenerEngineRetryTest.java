package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class SqsListenerEngineRetryTest {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    private SqsListenerEngine engine;

    @AfterEach
    void stopEngine() {
        if (engine != null) {
            engine.stop().subscribe();
        }
        scheduler.dispose();
    }

    @Test
    void firstReceiveRetryUsesLowerEqualJitterBound() {
        assertFirstRetryDelay(() -> 0.0, Duration.ofMillis(500));
    }

    @Test
    void firstReceiveRetryUsesUpperEqualJitterBound() {
        assertFirstRetryDelay(() -> 1.0, Duration.ofSeconds(1));
    }

    private void assertFirstRetryDelay(DoubleSupplier jitter, Duration expectedDelay) {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.failedFuture(
                                                new IllegalStateException("transient failure"))
                                        : pendingReceive);
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 30, 3_600),
                        scheduler,
                        jitter);

        engine.start(ignored -> Mono.empty());

        assertThat(receiveCalls).hasValue(1);
        scheduler.advanceTimeBy(expectedDelay.minusMillis(1));
        assertThat(receiveCalls).hasValue(1);

        scheduler.advanceTimeBy(Duration.ofMillis(1));
        assertThat(receiveCalls).hasValue(2);
    }
}

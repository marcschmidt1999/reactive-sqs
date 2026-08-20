package io.github.marcschmidt1999.reactive.sqs.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class ReactiveSqsListenerLifecycleTest {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    private ReactiveSqsListenerRegistrar registrar;

    @AfterEach
    void stopRegistrar() {
        if (registrar != null) {
            registrar.destroy();
        }
        scheduler.dispose();
    }

    @Test
    void pollingStartsInSmartLifecycleStartPhase() {
        var receiveCalls = new AtomicInteger();
        var client = receivingClient(receiveCalls);
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "validListener", new RootBeanDefinition(ValidListener.class));
        registrar = registrar(beanFactory, client);

        registrar.afterSingletonsInstantiated();

        assertThat(receiveCalls).hasValue(0);
        registrar.start();
        assertThat(receiveCalls).hasValue(1);
    }

    @Test
    void validatesEveryEndpointBeforeAnyListenerStarts() {
        var receiveCalls = new AtomicInteger();
        var client = receivingClient(receiveCalls);
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "validListener", new RootBeanDefinition(ValidListener.class));
        beanFactory.registerBeanDefinition(
                "fifoListener", new RootBeanDefinition(FifoListener.class));
        registrar = registrar(beanFactory, client);

        assertThatThrownBy(registrar::afterSingletonsInstantiated)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIFO queues are not supported yet: orders.fifo");
        assertThat(receiveCalls).hasValue(0);
    }

    @Test
    void stopRequestedDuringStartupStopsEveryListenerAfterStartupCompletes() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var firstReceive = new CompletableFuture<ReceiveMessageResponse>();
        var secondReceive = new CompletableFuture<ReceiveMessageResponse>();
        var firstReceiveEntered = new CountDownLatch(1);
        var releaseFirstReceive = new CountDownLatch(1);
        var receiveCalls = new AtomicInteger();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            var call = receiveCalls.getAndIncrement();
                            if (call == 0) {
                                firstReceiveEntered.countDown();
                                if (!releaseFirstReceive.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException(
                                            "test did not release first receive");
                                }
                                return firstReceive;
                            }
                            return secondReceive;
                        });
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "firstListener", new RootBeanDefinition(ValidListener.class));
        beanFactory.registerBeanDefinition(
                "secondListener", new RootBeanDefinition(SecondValidListener.class));
        registrar = registrar(beanFactory, client);
        registrar.afterSingletonsInstantiated();

        var start = CompletableFuture.runAsync(registrar::start);
        assertThat(firstReceiveEntered.await(5, TimeUnit.SECONDS)).isTrue();
        var stopped = new CompletableFuture<Void>();
        registrar.stop(() -> stopped.complete(null));
        assertThat(receiveCalls).hasValue(1);
        assertThat(stopped).isNotCompleted();

        try {
            releaseFirstReceive.countDown();
            start.join();

            assertThat(stopped).isCompleted();
            assertThat(firstReceive).isCancelled();
            assertThat(receiveCalls).hasValue(1);
            assertThat(registrar.isRunning()).isFalse();
        } finally {
            releaseFirstReceive.countDown();
            firstReceive.cancel(true);
            secondReceive.cancel(true);
        }
    }

    @Test
    void terminalPollFailureRemainsLifecycleRunningUntilCoordinatedStop() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        var forbidden = SqsException.builder().statusCode(403).message("forbidden").build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : CompletableFuture.failedFuture(forbidden));
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "failingPollListener",
                new RootBeanDefinition(ActiveDuringPollFailureListener.class));
        registrar = registrar(beanFactory, client);
        registrar.afterSingletonsInstantiated();

        registrar.start();

        assertThat(receiveCalls).hasValue(2);
        assertThat(registrar.isRunning()).isTrue();

        var stopped = new CompletableFuture<Void>();
        registrar.stop(() -> stopped.complete(null));
        scheduler.advanceTimeBy(Duration.ZERO);
        assertThat(stopped).isCompleted();
        assertThat(registrar.isRunning()).isFalse();
    }

    private ReactiveSqsListenerRegistrar registrar(
            DefaultListableBeanFactory beanFactory, SqsAsyncClient client) {
        return new ReactiveSqsListenerRegistrar(
                beanFactory,
                new MockEnvironment()
                        .withProperty("valid.queue", "queue-url")
                        .withProperty(
                                "fifo.queue",
                                "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.fifo"),
                client,
                (body, type) -> body,
                scheduler);
    }

    private SqsAsyncClient receivingClient(AtomicInteger receiveCalls) {
        var client = mock(SqsAsyncClient.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveCalls.incrementAndGet();
                            return new CompletableFuture<ReceiveMessageResponse>();
                        });
        return client;
    }

    static final class ValidListener {

        @ReactiveSqsListener(queue = "${valid.queue}")
        Mono<Void> handle(String body) {
            return Mono.empty();
        }
    }

    static final class FifoListener {

        @ReactiveSqsListener(queue = "${fifo.queue}")
        Mono<Void> handle(String body) {
            return Mono.empty();
        }
    }

    static final class SecondValidListener {

        @ReactiveSqsListener(queue = "${valid.queue}")
        Mono<Void> handle(String body) {
            return Mono.empty();
        }
    }

    static final class ActiveDuringPollFailureListener {

        @ReactiveSqsListener(queue = "${valid.queue}", maxInFlight = 2, shutdownGraceSeconds = 0)
        Mono<Void> handle(String body) {
            return Mono.never();
        }
    }
}

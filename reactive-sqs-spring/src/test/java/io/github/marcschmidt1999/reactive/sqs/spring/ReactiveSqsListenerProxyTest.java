package io.github.marcschmidt1999.reactive.sqs.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class ReactiveSqsListenerProxyTest {

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
    void invokesAnnotationDeclaredOnImplementationThroughJdkProxy() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var handled = new CompletableFuture<OrderCreated>();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        successfulDeletes(client);

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.addBeanPostProcessor(
                new BeanPostProcessor() {
                    @Override
                    public Object postProcessAfterInitialization(Object bean, String beanName) {
                        if (!beanName.equals("orderListener")) {
                            return bean;
                        }
                        var proxyFactory = new ProxyFactory(bean);
                        proxyFactory.setInterfaces(OrderHandler.class);
                        return proxyFactory.getProxy();
                    }
                });
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        ProxiedOrderListener.class, () -> new ProxiedOrderListener(handled)));
        beanFactory.preInstantiateSingletons();
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);

        registrar.afterSingletonsInstantiated();
        registrar.start();

        scheduler.advanceTimeBy(java.time.Duration.ofMillis(5));

        assertThat(handled).isCompletedWithValue(new OrderCreated("order-42"));
        assertThat(receiveCalls).hasValue(2);
    }

    private interface OrderHandler {
        Mono<Void> handle(OrderCreated event);
    }

    private static final class ProxiedOrderListener implements OrderHandler {
        private final CompletableFuture<OrderCreated> handled;

        private ProxiedOrderListener(CompletableFuture<OrderCreated> handled) {
            this.handled = handled;
        }

        @Override
        @ReactiveSqsListener(queue = "${test.queue-url}")
        public Mono<Void> handle(OrderCreated event) {
            handled.complete(event);
            return Mono.empty();
        }
    }

    private record OrderCreated(String orderId) {}

    private static void successfulDeletes(SqsAsyncClient client) {
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var request =
                                    invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            return CompletableFuture.completedFuture(
                                    DeleteMessageBatchResponse.builder()
                                            .successful(
                                                    request.entries().stream()
                                                            .map(
                                                                    entry ->
                                                                            DeleteMessageBatchResultEntry
                                                                                    .builder()
                                                                                    .id(entry.id())
                                                                                    .build())
                                                            .toList())
                                            .build());
                        });
    }
}

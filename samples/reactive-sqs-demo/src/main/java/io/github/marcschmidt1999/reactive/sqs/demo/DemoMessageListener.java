package io.github.marcschmidt1999.reactive.sqs.demo;

import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
final class DemoMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(DemoMessageListener.class);

    @ReactiveSqsListener(
            queue = "${demo.queue-url}",
            maxInFlight = 8,
            visibilityTimeoutSeconds = 60,
            shutdownGraceSeconds = 10)
    Mono<Void> consume(SqsMessage<DemoMessage> message) {
        return Mono.fromRunnable(
                () -> {
                    var payload = message.payload();
                    if (payload.shouldFail()) {
                        LOG.warn(
                                "Demo handler deliberately failing message {}",
                                message.messageId());
                        throw new IllegalStateException(
                                "Demo handler deliberately requested failure");
                    }
                    LOG.info("Processed demo message {}", message.messageId());
                });
    }
}

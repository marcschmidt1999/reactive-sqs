package io.github.marcschmidt1999.reactive.sqs.internal;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.KmsThrottledException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;

/**
 * Internal single-queue engine. Spring adapters are intentionally the supported consumer interface.
 */
public final class SqsListenerEngine {

    private static final int MAX_MESSAGES_PER_RECEIVE = 10;
    private static final int MAX_RECEIVE_BACKOFF_SECONDS = 20;
    private static final int MAX_VISIBILITY_SECONDS = 43_200;
    private static final int VISIBILITY_REQUEST_SAFETY_SECONDS = 5;
    private static final long INITIAL_LEASE_SAFETY_MILLIS = 1_000;
    private static final long VISIBILITY_RETRY_SAFETY_MILLIS = 100;
    private static final AtomicLong STATE_SEQUENCE = new AtomicLong();
    private static final Logger LOG = LoggerFactory.getLogger(SqsListenerEngine.class);

    private final SqsAsyncClient sqsClient;
    private final Configuration configuration;
    private final Scheduler scheduler;
    private final SqsListenerTelemetry telemetry;
    private final DoubleSupplier retryJitter;
    private final Consumer<Runnable> deliveryStarter;
    private final UnaryOperator<Mono<Void>> deleteGate;
    private final SqsDeleteBatcher deleteBatcher;
    private final AtomicReference<Function<Message, Mono<Void>>> handler = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicInteger receiveFailures = new AtomicInteger();
    private final AtomicReference<ReceiveRetry> scheduledRetry = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<ReceiveMessageResponse>> pendingReceive =
            new AtomicReference<>();
    private final AtomicInteger ownedCapacity = new AtomicInteger();
    private final AtomicBoolean receiveInProgress = new AtomicBoolean();
    private final AtomicInteger activeDeliveries = new AtomicInteger();
    private final Set<ActiveDelivery> deliveryTasks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Disposable> scheduledForcedStop = new AtomicReference<>();
    private final Object capacityMonitor = new Object();
    private final Object lifecycleMonitor = new Object();
    private final Sinks.Empty<Void> termination = Sinks.empty();

    public SqsListenerEngine(
            SqsAsyncClient sqsClient, Configuration configuration, Scheduler scheduler) {
        this(sqsClient, configuration, scheduler, SqsListenerTelemetry.noOp());
    }

    public SqsListenerEngine(
            SqsAsyncClient sqsClient,
            Configuration configuration,
            Scheduler scheduler,
            SqsListenerTelemetry telemetry) {
        this(
                sqsClient,
                configuration,
                scheduler,
                telemetry,
                () -> ThreadLocalRandom.current().nextDouble(),
                Runnable::run,
                UnaryOperator.identity());
    }

    SqsListenerEngine(
            SqsAsyncClient sqsClient,
            Configuration configuration,
            Scheduler scheduler,
            DoubleSupplier retryJitter) {
        this(
                sqsClient,
                configuration,
                scheduler,
                SqsListenerTelemetry.noOp(),
                retryJitter,
                Runnable::run,
                UnaryOperator.identity());
    }

    SqsListenerEngine(
            SqsAsyncClient sqsClient,
            Configuration configuration,
            Scheduler scheduler,
            DoubleSupplier retryJitter,
            Consumer<Runnable> deliveryStarter) {
        this(
                sqsClient,
                configuration,
                scheduler,
                SqsListenerTelemetry.noOp(),
                retryJitter,
                deliveryStarter,
                UnaryOperator.identity());
    }

    SqsListenerEngine(
            SqsAsyncClient sqsClient,
            Configuration configuration,
            Scheduler scheduler,
            DoubleSupplier retryJitter,
            Consumer<Runnable> deliveryStarter,
            UnaryOperator<Mono<Void>> deleteGate) {
        this(
                sqsClient,
                configuration,
                scheduler,
                SqsListenerTelemetry.noOp(),
                retryJitter,
                deliveryStarter,
                deleteGate);
    }

    SqsListenerEngine(
            SqsAsyncClient sqsClient,
            Configuration configuration,
            Scheduler scheduler,
            SqsListenerTelemetry telemetry,
            DoubleSupplier retryJitter,
            Consumer<Runnable> deliveryStarter,
            UnaryOperator<Mono<Void>> deleteGate) {
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
        this.retryJitter = Objects.requireNonNull(retryJitter, "retryJitter");
        this.deliveryStarter = Objects.requireNonNull(deliveryStarter, "deliveryStarter");
        this.deleteGate = Objects.requireNonNull(deleteGate, "deleteGate");
        this.deleteBatcher =
                new SqsDeleteBatcher(
                        sqsClient,
                        configuration.queueUrl(),
                        scheduler,
                        event -> observe(value -> value.deleteBatchCompleted(event)));
    }

    public void start(Function<Message, Mono<Void>> messageHandler) {
        Objects.requireNonNull(messageHandler, "messageHandler");
        synchronized (lifecycleMonitor) {
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("SQS listener engine may only be started once");
            }
            if (stopRequested.get()) {
                return;
            }
            handler.set(messageHandler);
            running.set(true);
        }
        publishState();
        poll();
    }

    public Mono<Void> stop() {
        synchronized (lifecycleMonitor) {
            stopRequested.set(true);
            running.set(false);
        }
        publishState();
        cancelRetry();
        var receive = pendingReceive.get();
        if (receive != null) {
            receive.cancel(true);
        }
        deleteBatcher.flushNow();
        scheduleForcedStopIfNeeded();
        completeTerminationIfDrained();
        return termination.asMono();
    }

    public boolean isRunning() {
        return running.get();
    }

    private void poll() {
        var reservation = reserveReceive();
        if (reservation == 0) {
            return;
        }
        publishState();
        var request =
                ReceiveMessageRequest.builder()
                        .queueUrl(configuration.queueUrl())
                        .waitTimeSeconds(configuration.longPollWaitSeconds())
                        .maxNumberOfMessages(reservation)
                        .visibilityTimeout(configuration.visibilityTimeoutSeconds())
                        .messageAttributeNames("All")
                        .messageSystemAttributeNamesWithStrings("All")
                        .build();
        var receiveStartedNanos = scheduler.now(TimeUnit.NANOSECONDS);
        CompletableFuture<ReceiveMessageResponse> receive;
        try {
            receive =
                    Objects.requireNonNull(
                            sqsClient.receiveMessage(request),
                            "SqsAsyncClient returned null from receiveMessage");
        } catch (RuntimeException error) {
            handleReceiveFailure(
                    error, reservation, receiveStartedNanos, scheduler.now(TimeUnit.NANOSECONDS));
            return;
        }
        pendingReceive.set(receive);
        receive.whenComplete(
                (response, error) -> {
                    var receiveCompletedNanos = scheduler.now(TimeUnit.NANOSECONDS);
                    pendingReceive.compareAndSet(receive, null);
                    if (!running.get()) {
                        completeReceive(reservation, 0);
                        publishState();
                        receiveCompleted(
                                receiveStartedNanos,
                                receiveCompletedNanos,
                                SqsListenerTelemetry.ReceiveOutcome.CANCELLED,
                                0);
                        completeTerminationIfDrained();
                        return;
                    }
                    if (error != null) {
                        handleReceiveFailure(
                                error, reservation, receiveStartedNanos, receiveCompletedNanos);
                        return;
                    }
                    handleReceiveSuccess(
                            reservation, response, receiveStartedNanos, receiveCompletedNanos);
                });
        if (!running.get()) {
            receive.cancel(true);
        }
    }

    private void handleReceiveSuccess(
            int reservation,
            ReceiveMessageResponse response,
            long receiveStartedNanos,
            long receiveCompletedNanos) {
        if (response == null) {
            handleReceiveFailure(
                    new NullPointerException("SQS returned a null receive response"),
                    reservation,
                    receiveStartedNanos,
                    receiveCompletedNanos);
            return;
        }
        receiveFailures.set(0);
        List<Message> messages = response.messages();
        if (messages.size() > reservation) {
            fail(
                    new IllegalStateException(
                            "SQS returned %d messages for a receive reservation of %d"
                                    .formatted(messages.size(), reservation)));
            completeReceive(reservation, 0);
            publishState();
            receiveCompleted(
                    receiveStartedNanos,
                    receiveCompletedNanos,
                    SqsListenerTelemetry.ReceiveOutcome.TERMINAL_ERROR,
                    0);
            completeTerminationIfDrained();
            return;
        }
        List<ActiveDelivery> deliveries;
        var cancelled = false;
        synchronized (lifecycleMonitor) {
            if (!running.get()) {
                cancelled = true;
                deliveries = List.of();
            } else {
                deliveries =
                        messages.stream()
                                .map(
                                        message ->
                                                new ActiveDelivery(
                                                        message,
                                                        receiveStartedNanos,
                                                        receiveCompletedNanos))
                                .toList();
                deliveryTasks.addAll(deliveries);
                activeDeliveries.addAndGet(deliveries.size());
                completeReceive(reservation, messages.size());
            }
        }
        if (cancelled) {
            completeReceive(reservation, 0);
            publishState();
            receiveCompleted(
                    receiveStartedNanos,
                    receiveCompletedNanos,
                    SqsListenerTelemetry.ReceiveOutcome.CANCELLED,
                    0);
            completeTerminationIfDrained();
            return;
        }
        publishState();
        deliveries.forEach(delivery -> deliveryStarter.accept(delivery::start));
        poll();
        receiveCompleted(
                receiveStartedNanos,
                receiveCompletedNanos,
                messages.isEmpty()
                        ? SqsListenerTelemetry.ReceiveOutcome.EMPTY
                        : SqsListenerTelemetry.ReceiveOutcome.MESSAGES,
                messages.size());
    }

    private void handleReceiveFailure(
            Throwable error,
            int reservation,
            long receiveStartedNanos,
            long receiveCompletedNanos) {
        var cause = unwrap(error);
        ReceiveRetry retry = null;
        try {
            if (isRetryable(cause)) {
                LOG.warn(
                        "Transient SQS receive failure for listener {}",
                        configuration.listenerId(),
                        cause);
                retry = prepareReceiveRetry();
            } else {
                fail(cause);
            }
        } catch (RuntimeException retrySchedulingFailure) {
            fail(retrySchedulingFailure);
        } finally {
            completeReceive(reservation, 0);
            if (retry != null) {
                var retryScheduled = armReceiveRetry(retry);
                if (retryScheduled && !failed.get()) {
                    observe(
                            value ->
                                    value.retryScheduled(
                                            SqsListenerTelemetry.RetryOperation.RECEIVE));
                }
            }
            publishState();
            receiveCompleted(
                    receiveStartedNanos,
                    receiveCompletedNanos,
                    failed.get()
                            ? SqsListenerTelemetry.ReceiveOutcome.TERMINAL_ERROR
                            : !running.get()
                                    ? SqsListenerTelemetry.ReceiveOutcome.CANCELLED
                                    : SqsListenerTelemetry.ReceiveOutcome.RETRYABLE_ERROR,
                    0);
            completeTerminationIfDrained();
        }
    }

    private ReceiveRetry prepareReceiveRetry() {
        if (!running.get()) {
            return null;
        }
        var failure = receiveFailures.incrementAndGet();
        var exponent = Math.min(failure - 1, 5);
        var delaySeconds = Math.min(1 << exponent, MAX_RECEIVE_BACKOFF_SECONDS);
        var delayMillis = equalJitterDelayMillis(delaySeconds);
        var retry = new ReceiveRetry(delayMillis);
        var previous = scheduledRetry.getAndSet(retry);
        if (previous != null) {
            previous.cancel();
        }
        if (!running.get() && scheduledRetry.compareAndSet(retry, null)) {
            retry.cancel();
        }
        return retry;
    }

    private boolean armReceiveRetry(ReceiveRetry retry) {
        try {
            return retry.arm();
        } catch (RuntimeException schedulingFailure) {
            scheduledRetry.compareAndSet(retry, null);
            retry.cancel();
            fail(schedulingFailure);
            return false;
        }
    }

    private long equalJitterDelayMillis(int maximumDelaySeconds) {
        var sample = retryJitter.getAsDouble();
        if (!Double.isFinite(sample) || sample < 0.0 || sample > 1.0) {
            throw new IllegalStateException("Retry jitter must return a value between 0.0 and 1.0");
        }
        var maximumDelayMillis = TimeUnit.SECONDS.toMillis(maximumDelaySeconds);
        var minimumDelayMillis = maximumDelayMillis / 2;
        return minimumDelayMillis + Math.round((maximumDelayMillis - minimumDelayMillis) * sample);
    }

    private void cancelRetry() {
        var retry = scheduledRetry.getAndSet(null);
        if (retry != null) {
            retry.cancel();
        }
    }

    private final class ReceiveRetry {
        private final long delayMillis;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean fired = new AtomicBoolean();
        private final AtomicReference<Disposable> task = new AtomicReference<>();

        private ReceiveRetry(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        private boolean arm() {
            if (cancelled.get()) {
                return false;
            }
            var scheduled = scheduler.schedule(this::fire, delayMillis, TimeUnit.MILLISECONDS);
            if (!task.compareAndSet(null, scheduled)) {
                scheduled.dispose();
                return false;
            }
            if ((cancelled.get() || fired.get()) && task.compareAndSet(scheduled, null)) {
                scheduled.dispose();
            }
            return true;
        }

        private void fire() {
            fired.set(true);
            task.set(null);
            if (!cancelled.get() && scheduledRetry.compareAndSet(this, null)) {
                poll();
            }
        }

        private void cancel() {
            cancelled.set(true);
            var scheduled = task.getAndSet(null);
            if (scheduled != null) {
                scheduled.dispose();
            }
        }
    }

    private void fail(Throwable error) {
        failed.set(true);
        synchronized (lifecycleMonitor) {
            running.set(false);
        }
        cancelRetry();
        LOG.error(
                "SQS listener {} stopped after a permanent failure",
                configuration.listenerId(),
                error);
        completeTerminationIfDrained();
    }

    private Throwable unwrap(Throwable error) {
        var cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private boolean isRetryable(Throwable error) {
        if (error instanceof IllegalArgumentException || error instanceof NullPointerException) {
            return false;
        }
        if (!(error instanceof SdkServiceException serviceException)) {
            if (error instanceof SdkException sdkException) {
                return sdkException.retryable();
            }
            return true;
        }
        var statusCode = serviceException.statusCode();
        return serviceException.isThrottlingException()
                || serviceException.isRetryableException()
                || error instanceof RequestThrottledException
                || error instanceof KmsThrottledException
                || statusCode == 408
                || statusCode == 429
                || statusCode >= 500;
    }

    private int reserveReceive() {
        synchronized (lifecycleMonitor) {
            synchronized (capacityMonitor) {
                if (!running.get()
                        || failed.get()
                        || receiveInProgress.get()
                        || scheduledRetry.get() != null) {
                    return 0;
                }
                var freeCapacity = configuration.maxInFlight() - ownedCapacity.get();
                if (freeCapacity == 0) {
                    return 0;
                }
                var reservation = Math.min(MAX_MESSAGES_PER_RECEIVE, freeCapacity);
                ownedCapacity.addAndGet(reservation);
                receiveInProgress.set(true);
                return reservation;
            }
        }
    }

    private void completeReceive(int reservation, int returnedMessages) {
        synchronized (capacityMonitor) {
            ownedCapacity.addAndGet(returnedMessages - reservation);
            receiveInProgress.set(false);
        }
    }

    private void finishDelivery(ActiveDelivery delivery) {
        deliveryTasks.remove(delivery);
        releaseDelivery();
        synchronized (lifecycleMonitor) {
            activeDeliveries.decrementAndGet();
        }
        publishState();
        if (running.get()) {
            poll();
        }
        completeTerminationIfDrained();
    }

    private void releaseDelivery() {
        synchronized (capacityMonitor) {
            ownedCapacity.decrementAndGet();
        }
    }

    private void completeTerminationIfDrained() {
        synchronized (lifecycleMonitor) {
            if (!running.get()
                    && activeDeliveries.get() == 0
                    && !hasReceiveInProgress()
                    && !hasOwnedCapacity()) {
                cancelForcedStop();
                termination.tryEmitEmpty();
            }
        }
    }

    private void scheduleForcedStopIfNeeded() {
        if (activeDeliveries.get() == 0 || scheduledForcedStop.get() != null) {
            return;
        }
        var forcedStop =
                scheduler.schedule(
                        () -> {
                            scheduledForcedStop.set(null);
                            deliveryTasks.forEach(ActiveDelivery::cancel);
                        },
                        configuration.shutdownGraceSeconds(),
                        TimeUnit.SECONDS);
        if (!scheduledForcedStop.compareAndSet(null, forcedStop)) {
            forcedStop.dispose();
        }
    }

    private void cancelForcedStop() {
        var forcedStop = scheduledForcedStop.getAndSet(null);
        if (forcedStop != null) {
            forcedStop.dispose();
        }
    }

    private boolean hasReceiveInProgress() {
        synchronized (capacityMonitor) {
            return receiveInProgress.get();
        }
    }

    private boolean hasOwnedCapacity() {
        synchronized (capacityMonitor) {
            return ownedCapacity.get() != 0;
        }
    }

    private void receiveCompleted(
            long startedNanos,
            long completedNanos,
            SqsListenerTelemetry.ReceiveOutcome outcome,
            int messageCount) {
        var event =
                new SqsListenerTelemetry.ReceiveCompleted(
                        elapsedBetween(startedNanos, completedNanos), outcome, messageCount);
        observe(value -> value.receiveCompleted(event));
    }

    private Duration elapsedBetween(long startedNanos, long completedNanos) {
        return Duration.ofNanos(Math.max(0L, completedNanos - startedNanos));
    }

    private void publishState() {
        var state =
                new SqsListenerTelemetry.ListenerState(
                        STATE_SEQUENCE.incrementAndGet(),
                        activeDeliveries.get(),
                        ownedCapacity.get(),
                        configuration.maxInFlight(),
                        running.get(),
                        failed.get());
        observe(value -> value.stateChanged(state));
    }

    private void observe(Consumer<SqsListenerTelemetry> observation) {
        try {
            observation.accept(telemetry);
        } catch (Throwable telemetryFailure) {
            Exceptions.throwIfFatal(telemetryFailure);
            LOG.warn(
                    "SQS listener telemetry failed for listener {}",
                    configuration.listenerId(),
                    telemetryFailure);
        }
    }

    private Mono<Void> delete(Message message) {
        return deleteBatcher.delete(message);
    }

    private final class ActiveDelivery {
        private final Message message;
        private final VisibilityHeartbeat heartbeat;
        private final long visibilityBudgetDeadlineNanos;
        private final AtomicReference<Disposable> subscription = new AtomicReference<>();
        private final AtomicReference<DeliveryState> state =
                new AtomicReference<>(DeliveryState.NEW);
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicLong processingStartedNanos = new AtomicLong(-1L);
        private final AtomicReference<ProcessingCompleted> processingCompleted =
                new AtomicReference<>();
        private final AtomicLong deleteStartedNanos = new AtomicLong(-1L);
        private final AtomicReference<SqsListenerTelemetry.DeleteCompleted> deleteCompleted =
                new AtomicReference<>();
        private final AtomicBoolean visibilityTimedOut = new AtomicBoolean();

        private ActiveDelivery(
                Message message, long receiveStartedNanos, long responseReceivedNanos) {
            this.message = message;
            this.heartbeat =
                    new VisibilityHeartbeat(message, receiveStartedNanos, responseReceivedNanos);
            this.visibilityBudgetDeadlineNanos =
                    receiveStartedNanos
                            + TimeUnit.SECONDS.toNanos(
                                    MAX_VISIBILITY_SECONDS - VISIBILITY_REQUEST_SAFETY_SECONDS);
        }

        private void start() {
            if (!state.compareAndSet(DeliveryState.NEW, DeliveryState.STARTING)) {
                return;
            }
            heartbeat.start();
            var processing =
                    processingPhase()
                            .then(
                                    Mono.defer(
                                            () ->
                                                    state.get() == DeliveryState.SETTLING
                                                            ? Objects.requireNonNull(
                                                                    deleteGate.apply(
                                                                            deleteAtBoundary()),
                                                                    "Delete gate returned null")
                                                            : Mono.empty()))
                            .timeout(
                                    remainingVisibilityBudget(),
                                    Mono.error(new VisibilityTimeoutException()),
                                    scheduler)
                            .doOnError(
                                    error -> {
                                        if (error instanceof VisibilityTimeoutException) {
                                            visibilityTimedOut.set(true);
                                            if (processingCompleted.get() == null) {
                                                completeProcessing(
                                                        SqsListenerTelemetry.ProcessingOutcome
                                                                .VISIBILITY_TIMEOUT);
                                            }
                                        }
                                    })
                            .doFinally(this::finish)
                            .subscribe(
                                    ignored -> {},
                                    error ->
                                            LOG.warn(
                                                    "SQS message {} was not acknowledged by listener {}",
                                                    message.messageId(),
                                                    configuration.listenerId(),
                                                    error));
            subscription.set(processing);
            if (state.get() == DeliveryState.CANCELLED || state.get() == DeliveryState.FINISHED) {
                processing.dispose();
            }
        }

        private Mono<Void> processingPhase() {
            return Mono.defer(
                            () -> {
                                if (!state.compareAndSet(
                                        DeliveryState.STARTING, DeliveryState.PROCESSING)) {
                                    return Mono.<Void>empty();
                                }
                                processingStartedNanos.set(scheduler.now(TimeUnit.NANOSECONDS));
                                return Objects.requireNonNull(
                                        handler.get().apply(message),
                                        "SQS listener handler returned null");
                            })
                    .timeout(
                            Duration.ofSeconds(configuration.maxProcessingDurationSeconds()),
                            Mono.error(new ProcessingTimeoutException()),
                            scheduler)
                    .doOnSuccess(
                            ignored -> {
                                if (state.compareAndSet(
                                        DeliveryState.PROCESSING, DeliveryState.SETTLING)) {
                                    completeProcessing(
                                            SqsListenerTelemetry.ProcessingOutcome.SUCCESS);
                                }
                            })
                    .doOnError(
                            error ->
                                    completeProcessing(
                                            error instanceof ProcessingTimeoutException
                                                    ? SqsListenerTelemetry.ProcessingOutcome
                                                            .PROCESSING_TIMEOUT
                                                    : error instanceof SqsMessageMappingException
                                                            ? SqsListenerTelemetry.ProcessingOutcome
                                                                    .MAPPING_ERROR
                                                            : SqsListenerTelemetry.ProcessingOutcome
                                                                    .HANDLER_ERROR));
        }

        private void cancel() {
            var previous = moveToCancelled();
            if (previous == DeliveryState.CANCELLED || previous == DeliveryState.FINISHED) {
                return;
            }
            var processing = subscription.get();
            if (processing != null) {
                processing.dispose();
            } else {
                finish(SignalType.CANCEL);
            }
        }

        private DeliveryState moveToCancelled() {
            while (true) {
                var current = state.get();
                if (current == DeliveryState.CANCELLED || current == DeliveryState.FINISHED) {
                    return current;
                }
                if (state.compareAndSet(current, DeliveryState.CANCELLED)) {
                    return current;
                }
            }
        }

        private Mono<Void> deleteAtBoundary() {
            return Mono.defer(
                    () -> {
                        if (!state.compareAndSet(
                                DeliveryState.SETTLING, DeliveryState.DELETE_STARTED)) {
                            return Mono.empty();
                        }
                        deleteStartedNanos.set(scheduler.now(TimeUnit.NANOSECONDS));
                        return delete(message)
                                .doOnSuccess(
                                        ignored ->
                                                completeDelete(
                                                        SqsListenerTelemetry.DeleteOutcome.SUCCESS))
                                .doOnError(
                                        ignored ->
                                                completeDelete(
                                                        SqsListenerTelemetry.DeleteOutcome.ERROR))
                                .doFinally(
                                        signal -> {
                                            if (signal == SignalType.CANCEL) {
                                                completeDelete(
                                                        SqsListenerTelemetry.DeleteOutcome
                                                                .CANCELLED);
                                            }
                                        });
                    });
        }

        private Duration remainingVisibilityBudget() {
            return Duration.ofNanos(
                    Math.max(
                            0L,
                            visibilityBudgetDeadlineNanos - scheduler.now(TimeUnit.NANOSECONDS)));
        }

        private void finish(SignalType terminalSignal) {
            if (finished.compareAndSet(false, true)) {
                var terminalState = state.getAndSet(DeliveryState.FINISHED);
                heartbeat.stop();
                if (processingCompleted.get() == null) {
                    completeProcessing(
                            visibilityTimedOut.get()
                                    ? SqsListenerTelemetry.ProcessingOutcome.VISIBILITY_TIMEOUT
                                    : SqsListenerTelemetry.ProcessingOutcome.SHUTDOWN_CANCELLED);
                }
                if (deleteStartedNanos.get() >= 0L && deleteCompleted.get() == null) {
                    completeDelete(SqsListenerTelemetry.DeleteOutcome.CANCELLED);
                }
                var completedProcessing = Objects.requireNonNull(processingCompleted.get());
                var completedDelete = deleteCompleted.get();
                finishDelivery(this);
                var event =
                        new SqsListenerTelemetry.DeliveryCompleted(
                                deliveryOutcome(
                                        completedProcessing,
                                        completedDelete,
                                        terminalSignal,
                                        terminalState),
                                completedProcessing.duration(),
                                completedProcessing.outcome(),
                                completedDelete == null
                                        ? SqsListenerTelemetry.DeleteCompleted.notAttempted()
                                        : completedDelete);
                observe(value -> value.deliveryCompleted(event));
            }
        }

        private SqsListenerTelemetry.DeliveryOutcome deliveryOutcome(
                ProcessingCompleted processing,
                SqsListenerTelemetry.DeleteCompleted completedDelete,
                SignalType terminalSignal,
                DeliveryState terminalState) {
            if (completedDelete != null) {
                if (completedDelete.outcome() == SqsListenerTelemetry.DeleteOutcome.SUCCESS) {
                    return SqsListenerTelemetry.DeliveryOutcome.ACKNOWLEDGED;
                }
                if (completedDelete.outcome() == SqsListenerTelemetry.DeleteOutcome.ERROR) {
                    return SqsListenerTelemetry.DeliveryOutcome.DELETE_ERROR;
                }
            }
            if (processing.outcome() == SqsListenerTelemetry.ProcessingOutcome.MAPPING_ERROR) {
                return SqsListenerTelemetry.DeliveryOutcome.MAPPING_ERROR;
            }
            if (processing.outcome() == SqsListenerTelemetry.ProcessingOutcome.HANDLER_ERROR) {
                return SqsListenerTelemetry.DeliveryOutcome.HANDLER_ERROR;
            }
            if (processing.outcome() == SqsListenerTelemetry.ProcessingOutcome.PROCESSING_TIMEOUT) {
                return SqsListenerTelemetry.DeliveryOutcome.PROCESSING_TIMEOUT;
            }
            if (visibilityTimedOut.get()
                    || processing.outcome()
                            == SqsListenerTelemetry.ProcessingOutcome.VISIBILITY_TIMEOUT) {
                return SqsListenerTelemetry.DeliveryOutcome.VISIBILITY_TIMEOUT;
            }
            if (terminalSignal == SignalType.CANCEL
                    || terminalState == DeliveryState.CANCELLED
                    || processing.outcome()
                            == SqsListenerTelemetry.ProcessingOutcome.SHUTDOWN_CANCELLED) {
                return SqsListenerTelemetry.DeliveryOutcome.SHUTDOWN_CANCELLED;
            }
            return SqsListenerTelemetry.DeliveryOutcome.DELETE_ERROR;
        }

        private void completeProcessing(SqsListenerTelemetry.ProcessingOutcome outcome) {
            var completed =
                    new ProcessingCompleted(
                            Duration.ofNanos(elapsedNanos(processingStartedNanos.get())), outcome);
            processingCompleted.compareAndSet(null, completed);
        }

        private void completeDelete(SqsListenerTelemetry.DeleteOutcome outcome) {
            var completed =
                    new SqsListenerTelemetry.DeleteCompleted(
                            Duration.ofNanos(elapsedNanos(deleteStartedNanos.get())), outcome);
            deleteCompleted.compareAndSet(null, completed);
        }

        private long elapsedNanos(long startedNanos) {
            return startedNanos < 0L
                    ? 0L
                    : Math.max(0L, scheduler.now(TimeUnit.NANOSECONDS) - startedNanos);
        }
    }

    private enum DeliveryState {
        NEW,
        STARTING,
        PROCESSING,
        SETTLING,
        DELETE_STARTED,
        CANCELLED,
        FINISHED
    }

    private record ProcessingCompleted(
            Duration duration, SqsListenerTelemetry.ProcessingOutcome outcome) {}

    private static final class ProcessingTimeoutException extends RuntimeException {

        private static final long serialVersionUID = 1L;
    }

    private static final class VisibilityTimeoutException extends RuntimeException {

        private static final long serialVersionUID = 1L;
    }

    private final class VisibilityHeartbeat {
        private final Message message;
        private final long receiveStartedNanos;
        private final AtomicLong leaseDeadlineNanos;
        private final AtomicInteger renewalFailures = new AtomicInteger();
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicReference<RenewalTask> scheduledRenewal = new AtomicReference<>();

        private VisibilityHeartbeat(
                Message message, long receiveStartedNanos, long responseReceivedNanos) {
            this.message = message;
            this.receiveStartedNanos = receiveStartedNanos;
            this.leaseDeadlineNanos =
                    new AtomicLong(
                            responseReceivedNanos
                                    + TimeUnit.SECONDS.toNanos(
                                            configuration.visibilityTimeoutSeconds())
                                    - TimeUnit.MILLISECONDS.toNanos(INITIAL_LEASE_SAFETY_MILLIS));
        }

        private void start() {
            scheduleNormalRenewal();
        }

        private void stop() {
            active.set(false);
            var renewal = scheduledRenewal.getAndSet(null);
            if (renewal != null) {
                renewal.cancel();
            }
        }

        private boolean scheduleRenewal(long delay, TimeUnit timeUnit) {
            if (!active.get()) {
                return false;
            }
            var renewal = new RenewalTask(delay, timeUnit);
            var previous = scheduledRenewal.getAndSet(renewal);
            if (previous != null) {
                previous.cancel();
            }
            if (!active.get() && scheduledRenewal.compareAndSet(renewal, null)) {
                renewal.cancel();
                return false;
            }
            try {
                return renewal.arm();
            } catch (RuntimeException schedulingFailure) {
                scheduledRenewal.compareAndSet(renewal, null);
                renewal.cancel();
                LOG.warn(
                        "Failed to schedule SQS visibility renewal for message {} in listener {}",
                        message.messageId(),
                        configuration.listenerId(),
                        schedulingFailure);
                observe(
                        value ->
                                value.visibilityRenewalSkipped(
                                        SqsListenerTelemetry.VisibilitySkipReason.SCHEDULER_ERROR));
                return false;
            }
        }

        private void scheduleNormalRenewal() {
            var remainingLeaseNanos =
                    Math.max(0L, leaseDeadlineNanos.get() - scheduler.now(TimeUnit.NANOSECONDS));
            scheduleRenewal(remainingLeaseNanos / 2, TimeUnit.NANOSECONDS);
        }

        private boolean scheduleFailureRetry() {
            var remainingLeaseMillis =
                    TimeUnit.NANOSECONDS.toMillis(
                            Math.max(
                                    0L,
                                    leaseDeadlineNanos.get()
                                            - scheduler.now(TimeUnit.NANOSECONDS)));
            var latestSafeRetryMillis = remainingLeaseMillis - VISIBILITY_RETRY_SAFETY_MILLIS;
            if (latestSafeRetryMillis <= 0) {
                LOG.warn(
                        "SQS visibility lease is too close to expiry to retry message {} in listener {}",
                        message.messageId(),
                        configuration.listenerId());
                observe(
                        value ->
                                value.visibilityRenewalSkipped(
                                        SqsListenerTelemetry.VisibilitySkipReason.LEASE_EXPIRING));
                return false;
            }
            var failure = renewalFailures.incrementAndGet();
            var exponent = Math.min(failure - 1, 5);
            var maximumBackoffSeconds =
                    Math.max(
                            1,
                            Math.min(
                                    MAX_RECEIVE_BACKOFF_SECONDS,
                                    configuration.visibilityTimeoutSeconds() / 4));
            var backoffSeconds = Math.min(1 << exponent, maximumBackoffSeconds);
            var retryDelayMillis =
                    Math.min(
                            equalJitterDelayMillis(backoffSeconds),
                            Math.max(1L, latestSafeRetryMillis / 2));
            var scheduled = scheduleRenewal(retryDelayMillis, TimeUnit.MILLISECONDS);
            if (scheduled) {
                observe(
                        value ->
                                value.retryScheduled(
                                        SqsListenerTelemetry.RetryOperation.VISIBILITY));
            }
            return scheduled;
        }

        private void renew() {
            if (!active.get()) {
                return;
            }
            var renewedVisibilitySeconds = remainingVisibilitySeconds();
            if (renewedVisibilitySeconds == 0) {
                LOG.warn(
                        "SQS visibility budget exhausted for message {} in listener {}",
                        message.messageId(),
                        configuration.listenerId());
                observe(
                        value ->
                                value.visibilityRenewalSkipped(
                                        SqsListenerTelemetry.VisibilitySkipReason
                                                .BUDGET_EXHAUSTED));
                return;
            }
            var request =
                    ChangeMessageVisibilityRequest.builder()
                            .queueUrl(configuration.queueUrl())
                            .receiptHandle(message.receiptHandle())
                            .visibilityTimeout(renewedVisibilitySeconds)
                            .build();
            var renewalStartedNanos = scheduler.now(TimeUnit.NANOSECONDS);
            try {
                sqsClient
                        .changeMessageVisibility(request)
                        .whenComplete(
                                (ignored, error) -> {
                                    var renewalCompletedNanos = scheduler.now(TimeUnit.NANOSECONDS);
                                    if (error != null) {
                                        var cause = unwrap(error);
                                        LOG.warn(
                                                "Failed to renew SQS visibility for message {} in listener {}",
                                                message.messageId(),
                                                configuration.listenerId(),
                                                cause);
                                        var cancelled = cause instanceof CancellationException;
                                        var retryable = !cancelled && isRetryable(cause);
                                        if (retryable) {
                                            scheduleFailureRetry();
                                        }
                                        visibilityRenewalCompleted(
                                                renewalStartedNanos,
                                                renewalCompletedNanos,
                                                cancelled
                                                        ? SqsListenerTelemetry.VisibilityOutcome
                                                                .CANCELLED
                                                        : retryable
                                                                ? SqsListenerTelemetry
                                                                        .VisibilityOutcome
                                                                        .RETRYABLE_ERROR
                                                                : SqsListenerTelemetry
                                                                        .VisibilityOutcome
                                                                        .TERMINAL_ERROR);
                                    } else {
                                        renewalFailures.set(0);
                                        leaseDeadlineNanos.set(
                                                renewalStartedNanos
                                                        + TimeUnit.SECONDS.toNanos(
                                                                renewedVisibilitySeconds));
                                        scheduleNormalRenewal();
                                        visibilityRenewalCompleted(
                                                renewalStartedNanos,
                                                renewalCompletedNanos,
                                                SqsListenerTelemetry.VisibilityOutcome.SUCCESS);
                                    }
                                });
            } catch (RuntimeException error) {
                var renewalCompletedNanos = scheduler.now(TimeUnit.NANOSECONDS);
                var cause = unwrap(error);
                LOG.warn(
                        "Failed to renew SQS visibility for message {} in listener {}",
                        message.messageId(),
                        configuration.listenerId(),
                        cause);
                var cancelled = cause instanceof CancellationException;
                var retryable = !cancelled && isRetryable(cause);
                if (retryable) {
                    scheduleFailureRetry();
                }
                visibilityRenewalCompleted(
                        renewalStartedNanos,
                        renewalCompletedNanos,
                        cancelled
                                ? SqsListenerTelemetry.VisibilityOutcome.CANCELLED
                                : retryable
                                        ? SqsListenerTelemetry.VisibilityOutcome.RETRYABLE_ERROR
                                        : SqsListenerTelemetry.VisibilityOutcome.TERMINAL_ERROR);
            }
        }

        private void visibilityRenewalCompleted(
                long startedNanos,
                long completedNanos,
                SqsListenerTelemetry.VisibilityOutcome outcome) {
            var event =
                    new SqsListenerTelemetry.VisibilityRenewalCompleted(
                            elapsedBetween(startedNanos, completedNanos), outcome);
            observe(value -> value.visibilityRenewalCompleted(event));
        }

        private int remainingVisibilitySeconds() {
            var elapsedNanos =
                    Math.max(0L, scheduler.now(TimeUnit.NANOSECONDS) - receiveStartedNanos);
            var elapsedSeconds =
                    Math.floorDiv(
                            elapsedNanos + TimeUnit.SECONDS.toNanos(1) - 1,
                            TimeUnit.SECONDS.toNanos(1));
            var remainingSeconds =
                    Math.max(
                            0L,
                            MAX_VISIBILITY_SECONDS
                                    - VISIBILITY_REQUEST_SAFETY_SECONDS
                                    - elapsedSeconds);
            return (int) Math.min(configuration.visibilityTimeoutSeconds(), remainingSeconds);
        }

        private final class RenewalTask {
            private final long delay;
            private final TimeUnit timeUnit;
            private final AtomicBoolean cancelled = new AtomicBoolean();
            private final AtomicBoolean fired = new AtomicBoolean();
            private final AtomicReference<Disposable> task = new AtomicReference<>();

            private RenewalTask(long delay, TimeUnit timeUnit) {
                this.delay = delay;
                this.timeUnit = timeUnit;
            }

            private boolean arm() {
                if (cancelled.get()) {
                    return false;
                }
                var scheduled = scheduler.schedule(this::fire, delay, timeUnit);
                if (!task.compareAndSet(null, scheduled)) {
                    scheduled.dispose();
                    return false;
                }
                if ((cancelled.get() || fired.get()) && task.compareAndSet(scheduled, null)) {
                    scheduled.dispose();
                }
                return true;
            }

            private void fire() {
                fired.set(true);
                task.set(null);
                if (!cancelled.get()
                        && active.get()
                        && scheduledRenewal.compareAndSet(this, null)) {
                    renew();
                }
            }

            private void cancel() {
                cancelled.set(true);
                var scheduled = task.getAndSet(null);
                if (scheduled != null) {
                    scheduled.dispose();
                }
            }
        }
    }

    public record Configuration(
            String listenerId,
            String queueUrl,
            int maxInFlight,
            int visibilityTimeoutSeconds,
            int longPollWaitSeconds,
            int shutdownGraceSeconds,
            int maxProcessingDurationSeconds) {

        public Configuration {
            if (listenerId == null || listenerId.isBlank()) {
                throw new IllegalArgumentException("listenerId must not be blank");
            }
            if (queueUrl == null || queueUrl.isBlank()) {
                throw new IllegalArgumentException("queueUrl must not be blank");
            }
            if (queueUrl.endsWith(".fifo")) {
                var separator = queueUrl.lastIndexOf('/');
                var queueName = separator < 0 ? queueUrl : queueUrl.substring(separator + 1);
                throw new IllegalArgumentException(
                        "FIFO queues are not supported yet: " + queueName);
            }
            if (maxInFlight < 1) {
                throw new IllegalArgumentException("maxInFlight must be at least 1");
            }
            if (visibilityTimeoutSeconds < 2 || visibilityTimeoutSeconds > 43_200) {
                throw new IllegalArgumentException(
                        "visibilityTimeoutSeconds must be between 2 and 43200");
            }
            if (longPollWaitSeconds < 0 || longPollWaitSeconds > 20) {
                throw new IllegalArgumentException("longPollWaitSeconds must be between 0 and 20");
            }
            if (shutdownGraceSeconds < 0) {
                throw new IllegalArgumentException("shutdownGraceSeconds must not be negative");
            }
            if (maxProcessingDurationSeconds < 1 || maxProcessingDurationSeconds > 43_200) {
                throw new IllegalArgumentException(
                        "maxProcessingDurationSeconds must be between 1 and 43200");
            }
        }
    }
}

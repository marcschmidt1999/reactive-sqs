package io.github.marcschmidt1999.reactive.sqs.spring;

import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerEngine;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsMessageMappingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;

/** Discovers annotated Spring methods and adapts them to the shared SQS listener engine. */
public final class ReactiveSqsListenerRegistrar
        implements SmartInitializingSingleton, SmartLifecycle, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(ReactiveSqsListenerRegistrar.class);

    private final ListableBeanFactory beanFactory;
    private final Environment environment;
    private final SqsAsyncClient sqsClient;
    private final SqsPayloadConverter payloadConverter;
    private final SqsListenerTelemetryFactory telemetryFactory;
    private final Scheduler scheduler;
    private final Object lifecycleMonitor = new Object();
    private final List<ListenerDefinition> listenerDefinitions = new ArrayList<>();
    private final List<ListenerRuntime> listenerRuntimes = new ArrayList<>();
    private final List<Runnable> stopCallbacks = new ArrayList<>();
    private LifecycleState lifecycleState = LifecycleState.STOPPED;

    public ReactiveSqsListenerRegistrar(
            ListableBeanFactory beanFactory,
            Environment environment,
            SqsAsyncClient sqsClient,
            SqsPayloadConverter payloadConverter) {
        this(
                beanFactory,
                environment,
                sqsClient,
                payloadConverter,
                SqsListenerTelemetryFactory.noOp(),
                Schedulers.parallel());
    }

    public ReactiveSqsListenerRegistrar(
            ListableBeanFactory beanFactory,
            Environment environment,
            SqsAsyncClient sqsClient,
            SqsPayloadConverter payloadConverter,
            SqsListenerTelemetryFactory telemetryFactory) {
        this(
                beanFactory,
                environment,
                sqsClient,
                payloadConverter,
                telemetryFactory,
                Schedulers.parallel());
    }

    ReactiveSqsListenerRegistrar(
            ListableBeanFactory beanFactory,
            Environment environment,
            SqsAsyncClient sqsClient,
            SqsPayloadConverter payloadConverter,
            Scheduler scheduler) {
        this(
                beanFactory,
                environment,
                sqsClient,
                payloadConverter,
                SqsListenerTelemetryFactory.noOp(),
                scheduler);
    }

    ReactiveSqsListenerRegistrar(
            ListableBeanFactory beanFactory,
            Environment environment,
            SqsAsyncClient sqsClient,
            SqsPayloadConverter payloadConverter,
            SqsListenerTelemetryFactory telemetryFactory,
            Scheduler scheduler) {
        this.beanFactory = beanFactory;
        this.environment = environment;
        this.sqsClient = sqsClient;
        this.payloadConverter = payloadConverter;
        this.telemetryFactory = telemetryFactory;
        this.scheduler = scheduler;
    }

    @Override
    public void afterSingletonsInstantiated() {
        var endpoints = discoverEndpoints();
        validateUniqueListenerIds(endpoints);
        var definitions = endpoints.stream().map(this::definition).toList();
        synchronized (lifecycleMonitor) {
            listenerDefinitions.clear();
            listenerDefinitions.addAll(definitions);
        }
    }

    @Override
    public void start() {
        List<ListenerRuntime> runtimes;
        synchronized (lifecycleMonitor) {
            if (lifecycleState == LifecycleState.RUNNING
                    || lifecycleState == LifecycleState.STARTING) {
                return;
            }
            if (lifecycleState == LifecycleState.STOPPING
                    || lifecycleState == LifecycleState.STOPPING_DURING_START) {
                throw new IllegalStateException("Reactive SQS listeners are still stopping");
            }
            runtimes =
                    listenerDefinitions.stream()
                            .map(
                                    definition ->
                                            new ListenerRuntime(
                                                    definition.endpoint(),
                                                    new SqsListenerEngine(
                                                            sqsClient,
                                                            definition.configuration(),
                                                            scheduler,
                                                            definition.telemetry())))
                            .toList();
            if (runtimes.isEmpty()) {
                return;
            }
            listenerRuntimes.addAll(runtimes);
            lifecycleState = LifecycleState.STARTING;
        }
        try {
            for (var runtime : runtimes) {
                synchronized (lifecycleMonitor) {
                    if (lifecycleState == LifecycleState.STOPPING_DURING_START) {
                        break;
                    }
                }
                runtime.engine().start(message -> invoke(runtime.endpoint(), message));
            }
        } catch (RuntimeException | Error error) {
            stop(() -> {});
            throw error;
        } finally {
            started();
        }
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    @Override
    public void stop(Runnable callback) {
        List<ListenerRuntime> runtimes = List.of();
        var alreadyStopped = false;
        synchronized (lifecycleMonitor) {
            if (lifecycleState == LifecycleState.STOPPED) {
                alreadyStopped = true;
            } else {
                stopCallbacks.add(callback);
                if (lifecycleState == LifecycleState.STOPPING
                        || lifecycleState == LifecycleState.STOPPING_DURING_START) {
                    return;
                }
                lifecycleState =
                        lifecycleState == LifecycleState.STARTING
                                ? LifecycleState.STOPPING_DURING_START
                                : LifecycleState.STOPPING;
                runtimes = List.copyOf(listenerRuntimes);
            }
        }
        if (alreadyStopped) {
            callback.run();
            return;
        }
        var runtimesToStop = runtimes;
        var terminations =
                runtimesToStop.stream()
                        .map(ListenerRuntime::engine)
                        .map(SqsListenerEngine::stop)
                        .toList();
        if (terminations.isEmpty()) {
            stopped(runtimesToStop);
            return;
        }
        Mono.whenDelayError(terminations)
                .subscribe(
                        ignored -> {},
                        error -> {
                            LOG.error("Failed while stopping reactive SQS listeners", error);
                            stopped(runtimesToStop);
                        },
                        () -> stopped(runtimesToStop));
    }

    private void started() {
        List<Runnable> callbacks = List.of();
        synchronized (lifecycleMonitor) {
            if (lifecycleState == LifecycleState.STARTING) {
                lifecycleState = LifecycleState.RUNNING;
            } else if (lifecycleState == LifecycleState.STOPPING_DURING_START) {
                lifecycleState = LifecycleState.STOPPING;
                if (listenerRuntimes.isEmpty()) {
                    lifecycleState = LifecycleState.STOPPED;
                    callbacks = drainStopCallbacks();
                }
            }
        }
        runStopCallbacks(callbacks);
    }

    private void stopped(List<ListenerRuntime> runtimes) {
        List<Runnable> callbacks = List.of();
        synchronized (lifecycleMonitor) {
            listenerRuntimes.removeAll(runtimes);
            if (listenerRuntimes.isEmpty() && lifecycleState == LifecycleState.STOPPING) {
                lifecycleState = LifecycleState.STOPPED;
                callbacks = drainStopCallbacks();
            }
        }
        runStopCallbacks(callbacks);
    }

    private List<Runnable> drainStopCallbacks() {
        var callbacks = List.copyOf(stopCallbacks);
        stopCallbacks.clear();
        return callbacks;
    }

    private void runStopCallbacks(List<Runnable> callbacks) {
        callbacks.forEach(
                callback -> {
                    try {
                        callback.run();
                    } catch (RuntimeException error) {
                        LOG.error("Reactive SQS stop callback failed", error);
                    }
                });
    }

    @Override
    public boolean isRunning() {
        synchronized (lifecycleMonitor) {
            return lifecycleState == LifecycleState.RUNNING;
        }
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public void destroy() {
        stop(() -> {});
    }

    private List<ListenerEndpoint> discoverEndpoints() {
        var discovered = new ArrayList<ListenerEndpoint>();
        for (var beanName : beanFactory.getBeanDefinitionNames()) {
            var beanType = beanFactory.getType(beanName, false);
            if (beanType == null) {
                continue;
            }
            Object bean = null;
            var listenerType = ClassUtils.getUserClass(beanType);
            if (Proxy.isProxyClass(beanType)) {
                bean = beanFactory.getBean(beanName);
                listenerType = AopProxyUtils.ultimateTargetClass(bean);
            }
            var methods = listenerMethods(listenerType);
            if (methods.isEmpty()) {
                continue;
            }
            var listenerBean = bean == null ? beanFactory.getBean(beanName) : bean;
            methods.forEach(
                    (method, annotation) ->
                            discovered.add(endpoint(beanName, listenerBean, method, annotation)));
        }
        return List.copyOf(discovered);
    }

    private void validateUniqueListenerIds(List<ListenerEndpoint> endpoints) {
        var listenerIds = new HashSet<String>();
        for (var endpoint : endpoints) {
            if (!listenerIds.add(endpoint.listenerId())) {
                throw new IllegalStateException(
                        "Duplicate reactive SQS listener id: "
                                + endpoint.listenerId()
                                + ". Listener bean names and method names must form a unique id.");
            }
        }
    }

    private ListenerDefinition definition(ListenerEndpoint endpoint) {
        var configuration =
                new SqsListenerEngine.Configuration(
                        endpoint.listenerId(),
                        endpoint.queueUrl(),
                        endpoint.maxInFlight(),
                        endpoint.visibilityTimeoutSeconds(),
                        20,
                        endpoint.shutdownGraceSeconds(),
                        endpoint.maxProcessingDurationSeconds());
        var telemetry =
                SqsListenerTelemetryFactory.requireTelemetry(telemetryFactory, configuration);
        return new ListenerDefinition(endpoint, configuration, telemetry);
    }

    private Map<Method, ReactiveSqsListener> listenerMethods(Class<?> listenerType) {
        return MethodIntrospector.selectMethods(
                listenerType,
                (MethodIntrospector.MetadataLookup<ReactiveSqsListener>)
                        method ->
                                AnnotatedElementUtils.findMergedAnnotation(
                                        method, ReactiveSqsListener.class));
    }

    private ListenerEndpoint endpoint(
            String beanName, Object bean, Method method, ReactiveSqsListener annotation) {
        if (method.getParameterCount() != 1 || !returnsMonoVoid(method)) {
            throw new IllegalStateException(
                    "@ReactiveSqsListener method must have one parameter and return Mono<Void>: "
                            + method);
        }
        var invocableMethod = MethodIntrospector.selectInvocableMethod(method, bean.getClass());
        if (!invocableMethod.trySetAccessible()) {
            throw new IllegalStateException(
                    "@ReactiveSqsListener method is not accessible: " + invocableMethod);
        }
        var handlerParameter = method.getGenericParameterTypes()[0];
        var receivesEnvelope = false;
        var payloadType = handlerParameter;
        if (handlerParameter instanceof ParameterizedType parameterizedType
                && parameterizedType.getRawType().equals(SqsMessage.class)) {
            payloadType = parameterizedType.getActualTypeArguments()[0];
            receivesEnvelope = true;
        } else if (handlerParameter.equals(SqsMessage.class)) {
            throw new IllegalStateException(
                    "@ReactiveSqsListener SqsMessage parameter must declare its payload type: "
                            + method);
        }
        return new ListenerEndpoint(
                beanName + "." + method.getName(),
                bean,
                invocableMethod,
                payloadType,
                environment.resolveRequiredPlaceholders(annotation.queue()),
                receivesEnvelope,
                annotation.visibilityTimeoutSeconds(),
                annotation.maxInFlight(),
                annotation.shutdownGraceSeconds(),
                annotation.maxProcessingDurationSeconds());
    }

    private boolean returnsMonoVoid(Method method) {
        if (!(method.getGenericReturnType() instanceof ParameterizedType returnType)
                || !(returnType.getRawType() instanceof Class<?> rawType)
                || !Mono.class.isAssignableFrom(rawType)) {
            return false;
        }
        var typeArguments = returnType.getActualTypeArguments();
        return typeArguments.length == 1 && typeArguments[0].equals(Void.class);
    }

    private Mono<Void> invoke(ListenerEndpoint endpoint, Message message) {
        return Mono.defer(
                () -> {
                    Object payload;
                    try {
                        payload = payloadConverter.convert(message.body(), endpoint.payloadType());
                    } catch (Exception error) {
                        return Mono.error(new SqsMessageMappingException(error));
                    }
                    try {
                        var argument =
                                endpoint.receivesEnvelope()
                                        ? new SqsMessage<>(payload, endpoint.queueUrl(), message)
                                        : payload;
                        var result = endpoint.method().invoke(endpoint.bean(), argument);
                        if (result == null) {
                            return Mono.error(
                                    new IllegalStateException(
                                            "@ReactiveSqsListener method returned null: "
                                                    + endpoint.method()));
                        }
                        @SuppressWarnings("unchecked")
                        var processing = (Mono<Void>) result;
                        return processing;
                    } catch (InvocationTargetException error) {
                        var cause = error.getCause() == null ? error : error.getCause();
                        return Mono.error(cause);
                    } catch (ReflectiveOperationException | RuntimeException error) {
                        return Mono.error(error);
                    }
                });
    }

    private record ListenerEndpoint(
            String listenerId,
            Object bean,
            Method method,
            Type payloadType,
            String queueUrl,
            boolean receivesEnvelope,
            int visibilityTimeoutSeconds,
            int maxInFlight,
            int shutdownGraceSeconds,
            int maxProcessingDurationSeconds) {}

    private record ListenerDefinition(
            ListenerEndpoint endpoint,
            SqsListenerEngine.Configuration configuration,
            SqsListenerTelemetry telemetry) {}

    private record ListenerRuntime(ListenerEndpoint endpoint, SqsListenerEngine engine) {}

    private enum LifecycleState {
        STOPPED,
        STARTING,
        RUNNING,
        STOPPING_DURING_START,
        STOPPING
    }
}

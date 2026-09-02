package io.github.marcschmidt1999.reactive.sqs.soak;

@FunctionalInterface
interface CpuOperation {

    String run(int durationMillis, long seed);
}

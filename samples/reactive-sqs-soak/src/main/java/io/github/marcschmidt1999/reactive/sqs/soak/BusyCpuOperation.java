package io.github.marcschmidt1999.reactive.sqs.soak;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

final class BusyCpuOperation implements CpuOperation {

    @Override
    public String run(int durationMillis, long seed) {
        if (durationMillis < 1) {
            throw new IllegalArgumentException("durationMillis must be positive");
        }
        var deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMillis);
        var value = seed;
        do {
            for (var index = 0; index < 2_048; index++) {
                value ^= value << 13;
                value ^= value >>> 7;
                value ^= value << 17;
                value *= 0x9e3779b97f4a7c15L;
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("CPU operation interrupted");
            }
        } while (System.nanoTime() < deadline);
        return Long.toUnsignedString(value, 16);
    }
}

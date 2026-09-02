package io.github.marcschmidt1999.reactive.sqs.soak;

import java.util.Objects;

final class SoakWorkload {

    private static final int MIN_SHORT_CPU_MILLIS = 5;
    private static final int SHORT_CPU_RANGE = 71;
    private static final int MIN_BOUNDARY_CPU_MILLIS = 4_500;
    private static final int BOUNDARY_CPU_RANGE = 1_001;

    private SoakWorkload() {}

    static SoakMessage message(String runId, long sequence, long seed) {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be positive");
        }

        var mode = mode(sequence);
        var mixed = mix(seed ^ Long.rotateLeft(sequence, 23));
        var cpuMillis =
                mode == SoakMode.BOUNDARY
                        ? MIN_BOUNDARY_CPU_MILLIS + Math.floorMod(mixed, BOUNDARY_CPU_RANGE)
                        : MIN_SHORT_CPU_MILLIS + Math.floorMod(mixed, SHORT_CPU_RANGE);
        var eventId = "%020d-%016x".formatted(sequence, mix(mixed ^ runId.hashCode()));
        return new SoakMessage(runId, eventId, sequence, mode, cpuMillis, seed);
    }

    private static SoakMode mode(long sequence) {
        if (sequence % 200 == 0) {
            return SoakMode.POISON;
        }
        if (sequence % 100 == 0) {
            return SoakMode.BOUNDARY;
        }
        if (sequence % 50 == 0) {
            return SoakMode.RETRY_ONCE;
        }
        return SoakMode.NORMAL;
    }

    private static long mix(long value) {
        var mixed = value;
        mixed = (mixed ^ (mixed >>> 30)) * 0xbf58476d1ce4e5b9L;
        mixed = (mixed ^ (mixed >>> 27)) * 0x94d049bb133111ebL;
        return mixed ^ (mixed >>> 31);
    }
}

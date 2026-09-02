package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SoakWorkloadTest {

    @Test
    void createsARepeatableMixWithBoundedCpuWork() {
        var normal = SoakWorkload.message("run-1", 1, 42);
        var retry = SoakWorkload.message("run-1", 50, 42);
        var boundary = SoakWorkload.message("run-1", 100, 42);
        var poison = SoakWorkload.message("run-1", 200, 42);

        assertThat(
                        new WorkloadSummary(
                                normal.mode(),
                                retry.mode(),
                                boundary.mode(),
                                poison.mode(),
                                normal.cpuMillis() >= 5 && normal.cpuMillis() <= 75,
                                retry.cpuMillis() >= 5 && retry.cpuMillis() <= 75,
                                boundary.cpuMillis() >= 4_500 && boundary.cpuMillis() <= 5_500,
                                poison.cpuMillis() >= 5 && poison.cpuMillis() <= 75,
                                normal.equals(SoakWorkload.message("run-1", 1, 42)),
                                !normal.equals(SoakWorkload.message("run-1", 1, 43))))
                .isEqualTo(
                        new WorkloadSummary(
                                SoakMode.NORMAL,
                                SoakMode.RETRY_ONCE,
                                SoakMode.BOUNDARY,
                                SoakMode.POISON,
                                true,
                                true,
                                true,
                                true,
                                true,
                                true));
    }

    private record WorkloadSummary(
            SoakMode normal,
            SoakMode retry,
            SoakMode boundary,
            SoakMode poison,
            boolean normalCpuBounded,
            boolean retryCpuBounded,
            boolean boundaryCpuBounded,
            boolean poisonCpuBounded,
            boolean repeatable,
            boolean seedAffectsMessage) {}
}

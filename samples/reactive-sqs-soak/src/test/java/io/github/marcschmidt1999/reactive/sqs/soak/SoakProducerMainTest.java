package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SoakProducerMainTest {

    @Test
    void operationalFailureDoesNotFinalizeTheManifestAsACompletedRun() {
        var producer = mock(SoakProducer.class);
        var options =
                new SoakProducerMain.ProducerOptions(
                        "eu-central-1",
                        "queue-url",
                        "table-name",
                        "run-1",
                        1,
                        Duration.ofSeconds(1),
                        3,
                        42);
        doThrow(new IllegalStateException("SQS unavailable"))
                .when(producer)
                .publish(any(), any(), any());

        assertThatThrownBy(() -> SoakProducerMain.run(options, producer))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SQS unavailable");

        verify(producer).checkpointRun(eq("run-1"), eq(0L), anyLong(), any(), eq(false));
    }
}

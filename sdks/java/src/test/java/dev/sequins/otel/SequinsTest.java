package dev.sequins.otel;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SequinsTest {

    @Test
    void initReturnsHandle() {
        Sequins sequins = Sequins.init("test-service");
        assertNotNull(sequins);
        sequins.close();
    }

    @Test
    void tracerProviderIsNotNull() {
        Sequins sequins = Sequins.init("test-tracer");
        assertNotNull(sequins.getTracerProvider());
        sequins.close();
    }

    @Test
    void meterProviderIsNotNull() {
        Sequins sequins = Sequins.init("test-meter");
        assertNotNull(sequins.getMeterProvider());
        sequins.close();
    }

    @Test
    void loggerProviderIsNotNull() {
        Sequins sequins = Sequins.init("test-logger");
        assertNotNull(sequins.getLoggerProvider());
        sequins.close();
    }

    @Test
    void canGetTracerFromProvider() {
        Sequins sequins = Sequins.init("test-get-tracer");
        Tracer tracer = sequins.getTracerProvider().tracerBuilder("my-lib").build();
        assertNotNull(tracer);
        sequins.close();
    }

    @Test
    void canGetMeterFromProvider() {
        Sequins sequins = Sequins.init("test-get-meter");
        Meter meter = sequins.getMeterProvider().meterBuilder("my-lib").build();
        assertNotNull(meter);
        sequins.close();
    }

    @Test
    void closeDoesNotThrow() {
        Sequins sequins = Sequins.init("test-close");
        assertDoesNotThrow(sequins::close);
    }

    @Test
    void customConfig() {
        SequinsConfig config = SequinsConfig.builder()
                .serviceName("custom-service")
                .endpoint("http://localhost:4317")
                .build();
        Sequins sequins = Sequins.init(config);
        assertNotNull(sequins);
        sequins.close();
    }
}

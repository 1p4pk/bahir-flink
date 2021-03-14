package org.apache.flink.streaming.connectors.influxdb.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class InfluxDBSourceBuilderTest {
    @Test
    void shouldNotBuildSourceWhenSchemaDeserializerIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class, () -> InfluxDBSource.<Long>builder().build());
        assertEquals(
                exception.getMessage(), "Deserialization schema is required but not provided.");
    }
}

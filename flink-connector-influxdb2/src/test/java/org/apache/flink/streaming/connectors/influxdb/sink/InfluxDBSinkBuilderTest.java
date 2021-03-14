package org.apache.flink.streaming.connectors.influxdb.sink;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.streaming.connectors.influxdb.util.InfluxDBContainer;
import org.apache.flink.streaming.connectors.influxdb.util.InfluxDBTestSerializer;
import org.junit.jupiter.api.Test;

class InfluxDBSinkBuilderTest {

    @Test
    void shouldNotBuildSinkWhenURLIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                                        .setInfluxDBOrganization(
                                                InfluxDBContainer.getOrganization())
                                        .build());
        assertEquals(exception.getMessage(), "The InfluxDB URL is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenUsernameIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                                        .setInfluxDBOrganization(
                                                InfluxDBContainer.getOrganization())
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The InfluxDB username is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenPasswordIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                                        .setInfluxDBOrganization(
                                                InfluxDBContainer.getOrganization())
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The InfluxDB password is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenBucketIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                                        .setInfluxDBOrganization(
                                                InfluxDBContainer.getOrganization())
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The Bucket name is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenOrganizationIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The Organization name is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenSchemaSerializerIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.<Long>builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                                        .setInfluxDBOrganization(
                                                InfluxDBContainer.getOrganization())
                                        .build());
        assertEquals(exception.getMessage(), "Serialization schema is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenBufferSizeIsZero() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> InfluxDBSink.<Long>builder().setWriteBufferSize(0));
        assertEquals(exception.getMessage(), "The buffer size should be greater than 0.");
    }
}

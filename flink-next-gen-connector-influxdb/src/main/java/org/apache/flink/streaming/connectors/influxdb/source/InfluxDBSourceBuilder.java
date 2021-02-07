package org.apache.flink.streaming.connectors.influxdb.source;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Properties;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;

public class InfluxDBSourceBuilder<OUT> {

    private InfluxDBDataPointDeserializer<OUT> deserializationSchema;
    // Configurations
    protected Properties properties;

    InfluxDBSourceBuilder() {
        this.deserializationSchema = null;
        this.properties = new Properties();
    }

    /**
     * Sets the {@link InfluxDBDataPointDeserializer deserializer} of the {@link
     * org.apache.flink.streaming.connectors.influxdb.common.DataPoint DataPoint} for the
     * InfluxDBSource.
     *
     * @param dataPointDeserializer the deserializer for InfluxDB {@link
     *     org.apache.flink.streaming.connectors.influxdb.common.DataPoint DataPoint}.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setDeserializer(
            final InfluxDBDataPointDeserializer<OUT> dataPointDeserializer) {
        this.deserializationSchema = dataPointDeserializer;
        return this;
    }

    /**
     * Sets the enqueue wait time, i.e., the time out of this InfluxDBSource.
     *
     * @param timeOut the enqueue wait time to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setEnqueueWaitTime(final long timeOut) {
        return this.setProperty(
                InfluxDBSourceOptions.ENQUEUE_WAIT_TIME.key(), String.valueOf(timeOut));
    }

    /**
     * Sets the ingest queue capacity of this InfluxDBSource.
     *
     * @param capacity the capacity to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setIngestQueueCapacity(final int capacity) {
        return this.setProperty(
                InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY.key(), String.valueOf(capacity));
    }

    /**
     * Sets the maximum number of lines that should be parsed per HTTP request for this
     * InfluxDBSource.
     *
     * @param max the maximum number of lines to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setMaximumLinesPerRequest(final int max) {
        return this.setProperty(
                InfluxDBSourceOptions.MAXIMUM_LINES_PER_REQUEST.key(), String.valueOf(max));
    }

    /**
     * Sets the TCP port on which the splitreader's HTTP server of this InfluxDBSource is running
     * on.
     *
     * @param port the port to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setPort(final int port) {
        return this.setProperty(InfluxDBSourceOptions.PORT.key(), String.valueOf(port));
    }

    /**
     * Set an arbitrary property for the InfluxDBSource. The valid keys can be found in {@link
     * InfluxDBSourceOptions}.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setProperty(final String key, final String value) {
        this.properties.setProperty(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the InfluxDBSource. The valid keys can be found in {@link
     * InfluxDBSourceOptions}.
     *
     * @param properties the properties to set for the InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setProperties(final Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    /**
     * Build the {@link InfluxDBSource}.
     *
     * @return a InfluxDBSource with the settings made for this builder.
     */
    public InfluxDBSource<OUT> build() {
        this.sanityCheck();
        return new InfluxDBSource<>(this.properties, this.deserializationSchema);
    }

    // ------------- private helpers  --------------
    private void sanityCheck() {
        // Check required settings.
        checkNotNull(
                this.deserializationSchema, "Deserialization schema is required but not provided.");
    }
}

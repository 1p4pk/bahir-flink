/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;
import org.apache.flink.util.Preconditions;

public final class InfluxDBSourceBuilder<OUT> {

    private InfluxDBDataPointDeserializer<OUT> deserializationSchema;
    // Configurations
    private final Configuration configuration;

    InfluxDBSourceBuilder() {
        this.deserializationSchema = null;
        this.configuration = new Configuration();
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
        this.configuration.setLong(InfluxDBSourceOptions.ENQUEUE_WAIT_TIME, timeOut);
        return this;
    }

    /**
     * Sets the ingest queue capacity of this InfluxDBSource.
     *
     * @param capacity the capacity to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setIngestQueueCapacity(final int capacity) {
        this.configuration.setInteger(InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY, capacity);
        return this;
    }

    /**
     * Sets the maximum number of lines that should be parsed per HTTP request for this
     * InfluxDBSource.
     *
     * @param max the maximum number of lines to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setMaximumLinesPerRequest(final int max) {
        this.configuration.setInteger(InfluxDBSourceOptions.MAXIMUM_LINES_PER_REQUEST, max);
        return this;
    }

    /**
     * Sets the TCP port on which the split reader's HTTP server of this InfluxDBSource is running
     * on.
     *
     * @param port the port to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setPort(final int port) {
        this.configuration.setInteger(InfluxDBSourceOptions.PORT, port);
        return this;
    }

    /**
     * Build the {@link InfluxDBSource}.
     *
     * @return a InfluxDBSource with the settings made for this builder.
     */
    public InfluxDBSource<OUT> build() {
        this.sanityCheck();
        return new InfluxDBSource<>(this.configuration, this.deserializationSchema);
    }

    // ------------- private helpers  --------------

    private void sanityCheck() {
        Preconditions.checkNotNull(
                this.deserializationSchema, "Deserialization schema is required but not provided.");
    }
}

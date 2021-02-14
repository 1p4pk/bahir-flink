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
package org.apache.flink.streaming.connectors.influxdb.sink;

import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.INFLUXDB_BUCKET;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.INFLUXDB_ORGANIZATION;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.INFLUXDB_PASSWORD;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.INFLUXDB_URL;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.INFLUXDB_USERNAME;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.WRITE_BUFFER_SIZE;
import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.WRITE_DATA_POINT_CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Properties;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;

public class InfluxDBSinkBuilder<IN> {
    private InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private final Properties properties;

    public InfluxDBSinkBuilder() {
        this.influxDBSchemaSerializer = null;
        this.properties = new Properties();
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBUrl(final String influxDBUrl) {
        return this.setProperty(INFLUXDB_URL.key(), influxDBUrl);
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBUsername(final String influxDBUrl) {
        return this.setProperty(INFLUXDB_USERNAME.key(), influxDBUrl);
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBPassword(final String influxDBUrl) {
        return this.setProperty(INFLUXDB_PASSWORD.key(), influxDBUrl);
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBBucket(final String influxDBUrl) {
        return this.setProperty(INFLUXDB_BUCKET.key(), influxDBUrl);
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBOrganization(final String influxDBUrl) {
        return this.setProperty(INFLUXDB_ORGANIZATION.key(), influxDBUrl);
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBSchemaSerializer(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        return this;
    }

    public InfluxDBSinkBuilder<IN> setDataPointCheckpoint(final boolean shouldWrite) {
        return this.setProperty(WRITE_DATA_POINT_CHECKPOINT.key(), String.valueOf(shouldWrite));
    }

    public InfluxDBSinkBuilder<IN> setWriteBufferSize(final int bufferSize) {
        return this.setProperty(WRITE_BUFFER_SIZE.key(), String.valueOf(bufferSize));
    }

    public InfluxDBSink<IN> build() {
        this.sanityCheck();
        return new InfluxDBSink<>(this.influxDBSchemaSerializer, this.properties);
    }

    // ------------- private helpers  --------------
    /**
     * Set an arbitrary property for the InfluxDBSink. The valid keys can be found in {@link
     * InfluxDBSinkOptions}.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this InfluxDBSinkBuilder.
     */
    private InfluxDBSinkBuilder<IN> setProperty(final String key, final String value) {
        this.properties.setProperty(key, value);
        return this;
    }

    /** Checks if the SchemaSerializer and the influxDBConfig are not null and set. */
    private void sanityCheck() {
        // Check required settings.
        checkNotNull(
                this.influxDBSchemaSerializer,
                "Deserialization schema is required but not provided.");
    }
}

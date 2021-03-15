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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;

public final class InfluxDBSinkBuilder<IN> {
    private InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private String influxDBUrl;
    private String influxDBUsername;
    private String influxDBPassword;
    private String bucketName;
    private String organizationName;
    private final Configuration configuration;

    public InfluxDBSinkBuilder() {
        this.influxDBUrl = null;
        this.influxDBUsername = null;
        this.influxDBPassword = null;
        this.bucketName = null;
        this.organizationName = null;
        this.influxDBSchemaSerializer = null;
        this.configuration = new Configuration();
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBUrl(final String influxDBUrl) {
        this.influxDBUrl = influxDBUrl;
        this.configuration.setString(INFLUXDB_URL, influxDBUrl);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBUsername(final String influxDBUsername) {
        this.influxDBUsername = influxDBUsername;
        this.configuration.setString(INFLUXDB_USERNAME, influxDBUsername);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBPassword(final String influxDBPassword) {
        this.influxDBPassword = influxDBPassword;
        this.configuration.setString(INFLUXDB_PASSWORD, influxDBPassword);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBBucket(final String bucketName) {
        this.bucketName = bucketName;
        this.configuration.setString(INFLUXDB_BUCKET, bucketName);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBOrganization(final String organizationName) {
        this.organizationName = organizationName;
        this.configuration.setString(INFLUXDB_ORGANIZATION, organizationName);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setInfluxDBSchemaSerializer(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        return this;
    }

    public InfluxDBSinkBuilder<IN> addCheckpointDataPoint(final boolean shouldWrite) {
        this.configuration.setBoolean(WRITE_DATA_POINT_CHECKPOINT, shouldWrite);
        return this;
    }

    public InfluxDBSinkBuilder<IN> setWriteBufferSize(final int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("The buffer size should be greater than 0.");
        }
        this.configuration.setInteger(WRITE_BUFFER_SIZE, bufferSize);
        return this;
    }

    public InfluxDBSink<IN> build() {
        this.sanityCheck();
        return new InfluxDBSink<>(this.influxDBSchemaSerializer, this.configuration);
    }

    // ------------- private helpers  --------------

    /** Checks if the SchemaSerializer and the influxDBConfig are not null and set. */
    private void sanityCheck() {
        // Check required settings.
        checkNotNull(this.influxDBUrl, "The InfluxDB URL is required but not provided.");
        checkNotNull(this.influxDBUsername, "The InfluxDB username is required but not provided.");
        checkNotNull(this.influxDBPassword, "The InfluxDB password is required but not provided.");
        checkNotNull(this.bucketName, "The Bucket name is required but not provided.");
        checkNotNull(this.organizationName, "The Organization name is required but not provided.");
        checkNotNull(
                this.influxDBSchemaSerializer,
                "Serialization schema is required but not provided.");
    }
}

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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import java.util.Properties;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.connectors.influxdb.common.InfluxDBOptionsBase;

public final class InfluxDBSinkOptions extends InfluxDBOptionsBase {

    private InfluxDBSinkOptions() {}

    public static final ConfigOption<Boolean> WRITE_DATA_POINT_CHECKPOINT =
            ConfigOptions.key("sink.influxDB.write.data_point.checkpoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Determines if the checkpoint data point should be written to InfluxDB or not.");

    public static final ConfigOption<Integer> WRITE_BUFFER_SIZE =
            ConfigOptions.key("sink.influxDB.write.buffer.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of the buffer to store the data before writing to InfluxDB.");

    public static final ConfigOption<String> INFLUXDB_URL =
            ConfigOptions.key("sink.influxDB.client.URL")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB Connection URL.");

    public static final ConfigOption<String> INFLUXDB_USERNAME =
            ConfigOptions.key("sink.influxDB.client.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB username.");

    public static final ConfigOption<String> INFLUXDB_PASSWORD =
            ConfigOptions.key("sink.influxDB.client.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB password.");

    public static final ConfigOption<String> INFLUXDB_BUCKET =
            ConfigOptions.key("sink.influxDB.client.bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB bucket name.");

    public static final ConfigOption<String> INFLUXDB_ORGANIZATION =
            ConfigOptions.key("sink.influxDB.client.organization")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB organization name.");

    public static boolean writeDataPointCheckpoint(final Properties properties) {
        return getOption(properties, WRITE_DATA_POINT_CHECKPOINT, Boolean::parseBoolean);
    }

    public static int getBufferSizeCapacity(final Properties properties) {
        return getOption(properties, WRITE_BUFFER_SIZE, Integer::parseInt);
    }

    public static InfluxDBClient getInfluxDBClient(final Properties properties) {
        final String url = getOption(properties, INFLUXDB_URL, String::toString);
        final String username = getOption(properties, INFLUXDB_USERNAME, String::toString);
        final String password = getOption(properties, INFLUXDB_PASSWORD, String::toString);
        final String bucket = getOption(properties, INFLUXDB_BUCKET, String::toString);
        final String organization = getOption(properties, INFLUXDB_ORGANIZATION, String::toString);
        final InfluxDBClientOptions influxDBClientOptions =
                InfluxDBClientOptions.builder()
                        .url(url)
                        .authenticate(username, password.toCharArray())
                        .bucket(bucket)
                        .org(organization)
                        .build();
        return InfluxDBClientFactory.create(influxDBClientOptions);
    }
}

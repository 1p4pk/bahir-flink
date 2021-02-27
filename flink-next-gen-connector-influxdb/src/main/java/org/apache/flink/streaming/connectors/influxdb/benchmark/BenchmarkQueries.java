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
package org.apache.flink.streaming.connectors.influxdb.benchmark;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.connectors.influxdb.benchmark.influxDBConfig.InfluxDBClientConfig;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.jetbrains.annotations.NotNull;

public final class BenchmarkQueries {

    private BenchmarkQueries() {}

    public enum Queries {
        SourceThroughput,
        SourceLatency,
        SinkThroughput,
        SinkLatency
    }

    @SneakyThrows
    public static JobClient startDiscardingQueryAsync() {
        final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        final InfluxDBSource<DataPoint> influxDBSource =
                InfluxDBSource.<DataPoint>builder()
                        .setDeserializer(new InfluxDBBenchmarkDeserializer())
                        .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .addSink(new DiscardingSink<>());

        return env.executeAsync();
    }

    @NotNull
    private static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().enableObjectReuse();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        return env;
    }

    @SneakyThrows
    public static JobClient startFileQueryAsync(final String path, final int writeEveryX) {
        final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        final InfluxDBSource<DataPoint> influxDBSource =
                InfluxDBSource.<DataPoint>builder()
                        .setDeserializer(new InfluxDBBenchmarkDeserializer())
                        .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .filter(new FilterDataPoints(writeEveryX))
                .map(new AddTimestamp()) // build filter to not write all data points but every x
                .sinkTo(createFileSink(path));
        return env.executeAsync();
    }

    @SneakyThrows
    public static void startSinkThroughputQuery(
            final long numberOfItemsToSink, final int writeBufferSize) {
        final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        final InfluxDBSink<Long> influxDBSink =
                InfluxDBSink.<Long>builder()
                        .setWriteBufferSize(writeBufferSize)
                        .setInfluxDBUrl(InfluxDBClientConfig.getUrl())
                        .setInfluxDBUsername(InfluxDBClientConfig.getUsername())
                        .setInfluxDBPassword(InfluxDBClientConfig.getPassword())
                        .setInfluxDBBucket(InfluxDBClientConfig.getBucket())
                        .setInfluxDBOrganization(InfluxDBClientConfig.getOrganization())
                        .setInfluxDBSchemaSerializer(new InfluxDBLongBenchmarkSerializer())
                        .build();

        env.fromSequence(0L, numberOfItemsToSink).sinkTo(influxDBSink);
        env.execute();
    }

    @SneakyThrows
    public static void startSinkLatencyQuery(
            final long numberOfItemsToSink, final int writeBufferSize) {
        final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        final InfluxDBSink<Tuple2<Long, Long>> influxDBSink =
                InfluxDBSink.<Tuple2<Long, Long>>builder()
                        .setWriteBufferSize(writeBufferSize)
                        .setInfluxDBUrl(InfluxDBClientConfig.getUrl())
                        .setInfluxDBUsername(InfluxDBClientConfig.getUsername())
                        .setInfluxDBPassword(InfluxDBClientConfig.getPassword())
                        .setInfluxDBBucket(InfluxDBClientConfig.getBucket())
                        .setInfluxDBOrganization(InfluxDBClientConfig.getOrganization())
                        .setInfluxDBSchemaSerializer(new InfluxDBTupleBenchmarkSerializer())
                        .build();

        env.fromSequence(0L, numberOfItemsToSink)
                .map(new AddTimestampToSequence())
                .sinkTo(influxDBSink);
        env.execute();
    }

    private static final class FilterDataPoints implements FilterFunction<DataPoint> {
        private long counter = -1;
        private final int writeEveryX;

        private FilterDataPoints(final int writeEveryX) {
            this.writeEveryX = writeEveryX;
        }

        @Override
        public boolean filter(final DataPoint dataPoint) {
            this.counter++;
            return this.counter % this.writeEveryX == 0;
        }
    }

    private static class AddTimestamp implements MapFunction<DataPoint, String> {
        @Override
        public String map(final DataPoint dataPoint) {
            return String.format("%s,%s", dataPoint.getTimestamp(), System.currentTimeMillis());
        }
    }

    private static class AddTimestampToSequence implements MapFunction<Long, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> map(final Long value) {
            return new Tuple2<>(value, System.currentTimeMillis());
        }
    }

    private static FileSink<String> createFileSink(final String path) {
        final OutputFileConfig config = OutputFileConfig.builder().withPartSuffix(".csv").build();
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(config)
                .build();
    }
}

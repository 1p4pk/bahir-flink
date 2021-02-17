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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.influxdb.benchmark.testContainer.InfluxDBContainer;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;

public final class BenchmarkQueries {

    private BenchmarkQueries() {}

    public enum Queries {
        DiscardingSource,
        FileSource,
        Sink
    }

    @SneakyThrows
    static JobClient startDiscardingQueryAsync() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        final InfluxDBSource<DataPoint> influxDBSource =
                InfluxDBSource.<DataPoint>builder()
                        .setDeserializer(new InfluxDBBenchmarkDeserializer())
                        .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .addSink(new DiscardingSink<>());
        return env.executeAsync();
    }

    @SneakyThrows
    static JobClient startFileQueryAsync(final String path) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        final InfluxDBSource<DataPoint> influxDBSource =
                InfluxDBSource.<DataPoint>builder()
                        .setDeserializer(new InfluxDBBenchmarkDeserializer())
                        .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .filter(new FilterDataPoints(10000))
                .map(new AddTimestamp()) // build filter to not write all data points but every x
                .sinkTo(createFileSink(path));
        return env.executeAsync();
    }

    @SneakyThrows
    static JobClient startSinkQuery(final InfluxDBContainer<?> influxDBContainer) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        final InfluxDBSink<Long> influxDBSink =
                InfluxDBSink.<Long>builder()
                        .setInfluxDBUrl(influxDBContainer.getUrl())
                        .setInfluxDBUsername(InfluxDBContainer.getUsername())
                        .setInfluxDBPassword(InfluxDBContainer.getPassword())
                        .setInfluxDBBucket(InfluxDBContainer.getBucket())
                        .setInfluxDBOrganization(InfluxDBContainer.getOrganization())
                        .setInfluxDBSchemaSerializer(new InfluxDBBenchmarkSerializer())
                        .build();

        // TODO: change this to an infinite source
        env.fromElements(1L, 2L, 3L, 4L, 5L).sinkTo(influxDBSink);
        return env.executeAsync();
    }

    private static final class FilterDataPoints implements FilterFunction<DataPoint> {
        private long counter = -1;
        private final int writeEveryX;

        private FilterDataPoints(final int writeEveryX) {
            this.writeEveryX = writeEveryX;
        }

        @Override
        public boolean filter(final DataPoint dataPoint) throws Exception {
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

    private static FileSink<String> createFileSink(final String path) {
        final OutputFileConfig config = OutputFileConfig.builder().withPartSuffix(".csv").build();
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(config)
                .build();
    }

    private static final class InfiniteSourceFunction extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = -8758033916372648233L;

        private boolean running = true;

        @Override
        public void run(final SourceContext<Integer> ctx) throws Exception {
            while (this.running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(0);
                }

                Thread.sleep(5L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}

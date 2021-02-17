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
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;

public class BenchmarkQueries {

    public enum Queries {
        DiscardingSource,
        FileSource
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

    private static class FilterDataPoints implements FilterFunction<DataPoint> {
        private long counter = -1;
        private final int writeEveryX;

        FilterDataPoints(final int writeEveryX) {
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
            return String.format(
                    "%s,%s", dataPoint.getTimestamp().toString(), System.currentTimeMillis());
        }
    }

    private static FileSink<String> createFileSink(final String path) {
        final OutputFileConfig config = OutputFileConfig.builder().withPartSuffix(".csv").build();
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(config)
                .build();
    }
}

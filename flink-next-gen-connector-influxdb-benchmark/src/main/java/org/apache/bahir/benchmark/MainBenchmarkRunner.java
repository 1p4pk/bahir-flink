package org.apache.bahir.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bahir.benchmark.generator.BlockingOffer;
import org.apache.bahir.benchmark.generator.SimpleLineProtocolGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainBenchmarkRunner implements Runnable {

    @Option(
            names = {"--eventsPerSecond", "-eps"},
            defaultValue = "10000000")
    private int eventsPerSecond;

    @Option(
            names = {"--eventsPerRequest", "-eps"},
            defaultValue = "1000")
    private int eventsPerRequest;

    @Option(
            names = {"--timeInSeconds", "-tis"},
            defaultValue = "30")
    private int timeInSeconds;

    @Option(
            names = {"--host", "-h"},
            defaultValue = "localhost")
    private String host;

    @Option(
            names = {"--port", "-p"},
            defaultValue = "8000")
    private int port;

    @Option(
            names = {"--outputPath"},
            defaultValue = "")
    private String outputPath;

    public static void main(final String[] args) {
        for (final String s : args) {
            log.info(s);
        }
        new CommandLine(new MainBenchmarkRunner()).execute(args);
    }

    @SneakyThrows
    @Override
    public void run() {
        final JobClient jobClient = this.startEngineQueryAsync();

        final SimpleLineProtocolGenerator generator =
                new SimpleLineProtocolGenerator(
                        this.eventsPerSecond, this.eventsPerRequest, this.timeInSeconds);
        final BlockingOffer offer =
                new BlockingOffer(
                        this.host,
                        this.port,
                        this.timeInSeconds,
                        this.eventsPerSecond,
                        this.eventsPerRequest,
                        this.outputPath + "result_");

        offer.waitForConnection();
        final long startTime = System.nanoTime();
        generator.generate(offer).get();
        final long endTime = System.nanoTime();
        offer.writeFile();
        log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
        jobClient.cancel();
    }

    @SneakyThrows
    private JobClient startEngineQueryAsync() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CollectSink.VALUES.clear();

        final InfluxDBSource<DataPoint> influxDBSource =
                InfluxDBSource.<DataPoint>builder()
                        .setDeserializer(new InfluxDBBenchmarkDeserializer())
                        .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .addSink(new CollectSink());
        return env.executeAsync();
    }

    private static class CollectSink implements SinkFunction<DataPoint> {

        public static final List<DataPoint> VALUES =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(final DataPoint value) throws Exception {
            VALUES.add(value);
        }
    }
}

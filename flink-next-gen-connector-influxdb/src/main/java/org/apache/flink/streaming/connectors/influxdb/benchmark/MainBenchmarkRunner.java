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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.BlockingOffer;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.SimpleLineProtocolGenerator;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainBenchmarkRunner implements Runnable {

    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Option(
            names = {"--eventsPerSecond", "-eps"},
            defaultValue = "1000000")
    private int eventsPerSecond;

    @Option(
            names = {"--eventsPerRequest", "-epr"},
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
        log.info("Start waiting for connection.");
        offer.waitForConnection();
        final long startTime = System.nanoTime();
        generator.generate(offer).get();
        final long endTime = System.nanoTime();
        offer.writeFile();
        log.info("Finished after {} seconds.", (endTime - startTime) / 1_000_000_000);
        jobClient.cancel();
        System.exit(0);
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

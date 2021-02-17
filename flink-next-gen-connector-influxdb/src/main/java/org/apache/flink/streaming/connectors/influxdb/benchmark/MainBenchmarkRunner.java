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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.BlockingOffer;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.SimpleLineProtocolGenerator;
import org.apache.flink.streaming.connectors.influxdb.benchmark.testContainer.InfluxDBContainer;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainBenchmarkRunner implements Runnable {

    @Option(
            names = {"--eventsPerSecond", "-eps"},
            defaultValue = "10000")
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
            names = {"--query"},
            defaultValue = "Sink",
            description = "Enum values: ${COMPLETION-CANDIDATES}")
    private BenchmarkQueries.Queries query;

    @Option(names = {"--outputPath"})
    private String outputPath;

    public static void main(final String[] args) {
        log.info("Start benchmark.");
        for (final String s : args) {
            log.info(s);
        }
        new CommandLine(new MainBenchmarkRunner()).execute(args);
    }

    @SneakyThrows
    @Override
    public void run() {
        if (this.outputPath == null) {
            this.outputPath = System.getProperty("user.dir");
            log.info("Output path: {}", this.outputPath);
        }
        JobClient jobClient = null;
        switch (this.query) {
            case DiscardingSource:
                jobClient = BenchmarkQueries.startDiscardingQueryAsync();
                this.runSourceBenchmark();
                break;
            case FileSource:
                final String filePath = String.format("%s/file_source_latency", this.outputPath);
                jobClient = BenchmarkQueries.startFileQueryAsync(filePath);
                this.runSourceBenchmark();
                break;
            case Sink:
                final InfluxDBContainer<?> influxDBContainer =
                        InfluxDBContainer.createWithDefaultTag();
                jobClient = BenchmarkQueries.startSinkQuery(influxDBContainer);
                this.runSinkBenchmark(influxDBContainer.getClient());
                influxDBContainer.close();
                break;
            default:
                log.error("Query {} not known", this.query);
                System.exit(1);
        }
        jobClient.cancel();
        System.exit(0);
    }

    @SneakyThrows
    private void runSourceBenchmark() {
        final SimpleLineProtocolGenerator generator =
                new SimpleLineProtocolGenerator(
                        this.eventsPerSecond, this.eventsPerRequest, this.timeInSeconds);
        final BlockingOffer offer =
                BlockingOffer.builder()
                        .host(this.host)
                        .port(this.port)
                        .engineRuntime(this.timeInSeconds)
                        .eventsPerSecond(this.eventsPerSecond)
                        .eventsPerRequest(this.eventsPerRequest)
                        .filePath(this.outputPath)
                        .build();
        final long startTime = System.nanoTime();
        generator.generate(offer).get();
        final long endTime = System.nanoTime();
        offer.writeFile();
        log.info("Finished after {} seconds.", (endTime - startTime) / 1_000_000_000);
    }

    @SneakyThrows
    private void runSinkBenchmark(final InfluxDBClient influxDBClient) {
        // TODO: Read Data from InfluxDB and write to file
        final List<String> data = queryWrittenData(influxDBClient);
        log.info("Getting data.");
    }

    private static List<String> queryWrittenData(final InfluxDBClient influxDBClient) {
        final List<String> dataPoints = new ArrayList<>();

        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"testSink\")",
                        InfluxDBContainer.getBucket());
        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                dataPoints.add(recordToDataPoint(record).toLineProtocol());
            }
        }
        return dataPoints;
    }

    private static Point recordToDataPoint(final FluxRecord record) {
        final String tagKey = "simpleTag";
        final Point point = new Point(record.getMeasurement());
        point.addTag(tagKey, String.valueOf(record.getValueByKey(tagKey)));
        point.addField(
                Objects.requireNonNull(record.getField()), String.valueOf(record.getValue()));
        point.time(record.getTime(), WritePrecision.NS);
        return point;
    }
}

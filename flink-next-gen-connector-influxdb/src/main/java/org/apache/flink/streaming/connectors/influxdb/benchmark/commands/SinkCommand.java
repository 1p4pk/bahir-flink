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
package org.apache.flink.streaming.connectors.influxdb.benchmark.commands;

import static org.apache.flink.streaming.connectors.influxdb.benchmark.InfluxDBTupleBenchmarkSerializer.queryWrittenData;
import static org.junit.Assert.assertTrue;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.query.FluxRecord;
import java.io.File;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries.Queries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.testcontainer.InfluxDBContainer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sink", description = "Command to start sink benchmarking")
@Slf4j
public class SinkCommand implements Runnable {

    @Option(names = {"--outputPath"})
    private String outputPath;

    private static final String CSV_FILE_NAME = "sink-output.csv";

    @Option(
            names = {"-n", "--numberOfItemsToSink"},
            description = "Amount of values to get ingested in InfluxDB",
            required = true)
    private long numberOfItemsToSink;

    @Option(
            names = {"-b", "--batchSize"},
            description = "Number of data points to be batched by sink",
            required = true)
    private int batchSize;

    @Option(
            names = {"--query"},
            defaultValue = "SinkThroughput",
            description = "Enum values: ${COMPLETION-CANDIDATES}")
    private BenchmarkQueries.Queries query;

    @Override
    public void run() {
        final InfluxDBContainer<?> influxDBContainer = InfluxDBContainer.createWithDefaultTag();
        long startTime = 0, endTime = 0;
        switch (this.query) {
            case SinkThroughput:
                startTime = System.currentTimeMillis();
                BenchmarkQueries.startSinkThroughputQuery(
                        influxDBContainer, this.numberOfItemsToSink - 1, this.batchSize);
                endTime = System.currentTimeMillis();
                break;
            case SinkLatency:
                startTime = System.currentTimeMillis();
                BenchmarkQueries.startSinkLatencyQuery(
                        influxDBContainer, this.numberOfItemsToSink - 1, this.batchSize);
                endTime = System.currentTimeMillis();
                break;
            default:
                log.error("Query {} not known in source", this.query);
                System.exit(1);
        }
        final long duration = endTime - startTime;
        final long throughput = (this.numberOfItemsToSink / duration) * 1_000_000;
        System.out.println("Throughput: " + throughput + " records/seconds");
        System.out.println("Runtime: " + duration);
        log.info("Finished after {} seconds.", duration / 1_000_000_000);
        if (this.query == Queries.SinkLatency) {
            queryResultFromInlfuxDB(influxDBContainer.getClient(), duration);
        }
        influxDBContainer.close();
    }

    @SneakyThrows
    private static void queryResultFromInlfuxDB(
            final InfluxDBClient influxDBClient, final long duration) {
        // TODO: Read Data from InfluxDB and write to file
        Thread.sleep(1000);
        final List<FluxRecord> records = queryWrittenData(influxDBClient);
        final Collection<String[]> dataLines = new ArrayList<>();
        for (final FluxRecord record : records) {
            final Object processingTime = record.getValue();
            final Instant ingestionTime = record.getTime();
            dataLines.add(
                    new String[] {
                        processingTime.toString(), String.valueOf(ingestionTime.toEpochMilli())
                    });
        }
        System.out.println("length: " + records.size());
        writeFile(dataLines);
        log.info("Getting data.\n {} of records written.\n duration {}", records.size(), duration);
    }

    @SneakyThrows
    private static void writeFile(final Collection<String[]> dataLines) {
        final File csvOutputFile = new File(CSV_FILE_NAME);
        try (final PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println("processingTime,influxIngestionTime");
            dataLines.stream().map(SinkCommand::convertToCSV).forEach(pw::println);
        }
        assertTrue(csvOutputFile.exists());
        log.info("Wrote result to file.");
    }

    private static String convertToCSV(final String[] data) {
        return String.join(",", data);
    }
}

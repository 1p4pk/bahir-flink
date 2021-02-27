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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.BlockingOffer;
import org.apache.flink.streaming.connectors.influxdb.benchmark.generator.SimpleLineProtocolGenerator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "source", description = "Command to start source benchmarking")
@Slf4j
public class SourceCommand implements Runnable {
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
            names = {"--writeEveryX", "-wex"},
            defaultValue = "10000")
    private int writeEveryX;

    @Option(
            names = {"--host", "-h"},
            defaultValue = "localhost",
            description = "Host address of HTTP server (defaults to localhost)")
    private String host;

    @Option(
            names = {"--port", "-p"},
            defaultValue = "8000",
            description = "Port number of HTTP server (defaults to 8000)")
    private int port;

    @Option(
            names = {"--query"},
            defaultValue = "SourceThroughput",
            description = "Enum values: ${COMPLETION-CANDIDATES}")
    private BenchmarkQueries.Queries query;

    @Option(names = {"--outputPath"})
    private String outputPath;

    @SneakyThrows
    @Override
    public void run() {
        if (this.outputPath == null) {
            this.outputPath = System.getProperty("user.dir");
            log.info("Output path: {}", this.outputPath);
        }
        JobClient jobClient = null;
        switch (this.query) {
            case SourceThroughput:
                jobClient = BenchmarkQueries.startDiscardingQueryAsync();
                this.runSourceBenchmark();
                break;
            case SourceLatency:
                final String filePath = String.format("%s/file_source_latency", this.outputPath);
                jobClient = BenchmarkQueries.startFileQueryAsync(filePath, this.writeEveryX);
                this.runSourceBenchmark();
                break;
            default:
                log.error("Query {} not known in source", this.query);
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
}

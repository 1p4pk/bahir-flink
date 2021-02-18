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

import static org.apache.flink.streaming.connectors.influxdb.benchmark.InfluxDBBenchmarkSerializer.queryWrittenData;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.query.FluxRecord;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.testcontainer.InfluxDBContainer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sink", description = "Command to start sink benchmarking")
@Slf4j
public class SinkCommand implements Runnable {

    @Option(
            names = {"-n", "--numberOfItemsToSink"},
            description = "Amount of values to get ingested in InfluxDB",
            required = true)
    private long numberOfItemsToSink;

    @Override
    public void run() {
        final InfluxDBContainer<?> influxDBContainer = InfluxDBContainer.createWithDefaultTag();

        final long startTime = System.nanoTime();
        BenchmarkQueries.startSinkQuery(influxDBContainer, this.numberOfItemsToSink - 1);
        final long endTime = System.nanoTime();

        final long duration = endTime - startTime;
        log.info("Finished after {} seconds.", duration / 1_000_000_000);
        runSinkBenchmark(influxDBContainer.getClient(), duration);
        influxDBContainer.close();
    }

    @SneakyThrows
    private static void runSinkBenchmark(final InfluxDBClient influxDBClient, final long duration) {
        // TODO: Read Data from InfluxDB and write to file
        final List<FluxRecord> records = queryWrittenData(influxDBClient);
        log.info("Getting data.\n {} of records written.\n duration {}", records.size(), duration);
    }
}

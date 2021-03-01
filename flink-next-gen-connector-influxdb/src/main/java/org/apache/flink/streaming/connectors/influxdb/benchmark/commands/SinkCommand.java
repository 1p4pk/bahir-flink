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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.query.FluxRecord;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.BenchmarkQueries.Queries;
import org.apache.flink.streaming.connectors.influxdb.benchmark.influxDBConfig.InfluxDBClientConfig;
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
        final InfluxDBClient influxDBClient = InfluxDBClientConfig.getClient();
        long startTime = 0, endTime = 0;
        switch (this.query) {
            case SinkThroughput:
                startTime = System.currentTimeMillis();
                BenchmarkQueries.startSinkThroughputQuery(
                        this.numberOfItemsToSink - 1, this.batchSize);
                endTime = System.currentTimeMillis();
                break;
            case SinkLatency:
                startTime = System.currentTimeMillis();
                BenchmarkQueries.startSinkLatencyQuery(
                        this.numberOfItemsToSink - 1, this.batchSize);
                endTime = System.currentTimeMillis();
                break;
            default:
                log.error("Query {} not known in source", this.query);
                System.exit(1);
        }
        final long duration = endTime - startTime;
        final long throughput = (long) (((double) this.numberOfItemsToSink / duration) * 1000);
        log.info("Throughput: {} records/seconds", throughput);
        log.info("Runtime: {} milliseconds", duration);
        if (this.query == Queries.SinkLatency) {
            this.queryResultFromInfluxDB(influxDBClient);
        }
        InfluxDBClientConfig.clearData(influxDBClient);
        influxDBClient.close();
    }

    @SneakyThrows
    private void queryResultFromInfluxDB(final InfluxDBClient influxDBClient) {
        Thread.sleep(1000);
        final List<FluxRecord> records = queryWrittenData(influxDBClient);
        for (final FluxRecord record : records) {
            final Double latency = (Double) record.getValueByKey("latency");
            log.info(
                    "latency for batch size {} and number of elements {} is {} nanoseconds",
                    this.batchSize,
                    this.numberOfItemsToSink,
                    latency.longValue());
        }
    }
}

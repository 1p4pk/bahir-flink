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
package org.apache.flink.streaming.connectors.influxdb.sink.writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;

@Slf4j
public class InfluxDBWriter<IN> implements SinkWriter<IN, Long, Point> {

    private static final int BUFFER_SIZE = 1000;

    private long lastTimestamp = 0;
    private final List<Point> elements;
    private ProcessingTimeService processingTimerService;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer, final InfluxDBConfig config) {
        this.schemaSerializer = schemaSerializer;
        this.elements = new ArrayList<>(BUFFER_SIZE);
        this.influxDBClient = config.getClient();
    }

    @Override
    public void write(final IN in, final Context context) {
        try {
            if (this.elements.size() == BUFFER_SIZE) {
                log.info("Buffer size reached preparing to write the elements.");
                this.writeCurrentElements();
                this.elements.clear();
            } else {
                log.debug("Adding elements to buffer. Buffer size: {}", this.elements.size());
                this.elements.add(this.schemaSerializer.serialize(in, context));
                if (context.timestamp() != null)
                    this.lastTimestamp = Math.max(this.lastTimestamp, context.timestamp());
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Long> prepareCommit(final boolean flush) {
        final List<Long> lastTimestamp = new ArrayList<>();
        lastTimestamp.add(this.lastTimestamp);
        return lastTimestamp;
    }

    @Override
    public List<Point> snapshotState() {
        return this.elements;
    }

    @Override
    public void close() throws Exception {
        log.debug("Preparing to write the elements in close.");
        this.writeCurrentElements();
        log.debug("Closing the writer.");
        this.elements.clear();
    }

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }

    private void writeCurrentElements() throws Exception {
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            writeApi.writePoints(this.elements);
            log.debug("Wrote {} data points", this.elements.size());
        }
    }
}

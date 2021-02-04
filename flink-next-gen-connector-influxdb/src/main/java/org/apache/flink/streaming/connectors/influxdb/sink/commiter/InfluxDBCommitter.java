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
package org.apache.flink.streaming.connectors.influxdb.sink.commiter;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.streaming.connectors.influxdb.common.InfluxDBConfig;

@Slf4j
public class InfluxDBCommitter implements Committer<Long> {

    private final InfluxDBClient influxDBClient;

    public InfluxDBCommitter(final InfluxDBConfig config) {
        this.influxDBClient = config.getClient();
    }

    // This method is called only when a checkpoint is set
    @SneakyThrows
    @Override
    public List<Long> commit(final List<Long> committables) {
        log.info("A checkpoint is set.");
        Optional<Long> lastTimestamp = Optional.empty();
        if (committables.size() >= 1) {
            lastTimestamp = Optional.ofNullable(committables.get(committables.size() - 1));
        }
        this.writeCheckpointDataPoint(lastTimestamp);
        return Collections.emptyList();
    }

    @Override
    public void close() {
        this.influxDBClient.close();
        log.debug("Closing the committer.");
    }

    private void writeCheckpointDataPoint(final Optional<Long> timestamp) {
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            final Point point = new Point("checkpoint");
            point.addField("checkpoint", "flink");
            timestamp.ifPresent(aTime -> point.time(aTime, WritePrecision.NS));
            writeApi.writePoint(point);
            log.debug("Checkpoint data point write at {}", point.toLineProtocol());
        }
    }
}

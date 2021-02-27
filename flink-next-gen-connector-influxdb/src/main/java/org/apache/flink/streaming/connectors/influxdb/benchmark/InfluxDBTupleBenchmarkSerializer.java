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
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.sink.SinkWriter.Context;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.influxdb.benchmark.testcontainer.InfluxDBContainer;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;

public class InfluxDBTupleBenchmarkSerializer
        implements InfluxDBSchemaSerializer<Tuple2<Long, Long>> {

    private static final String MEASUREMENT = "test";
    private static final String TAG_KEY = "longValue";
    private static final String FIELD_KEY = "serializationTime";

    @Override
    public Point serialize(final Tuple2<Long, Long> element, final Context context)
            throws Exception {
        final Point dataPoint = new Point(MEASUREMENT);
        dataPoint.addTag(TAG_KEY, String.valueOf(element.f0));
        dataPoint.addField(FIELD_KEY, element.f1);
        return dataPoint;
    }

    public static List<FluxRecord> queryWrittenData(final InfluxDBClient influxDBClient) {
        final List<FluxRecord> fluxRecords = new ArrayList<>();
        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"%s\")",
                        InfluxDBContainer.getBucket(), MEASUREMENT);
        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            fluxRecords.addAll(table.getRecords());
        }
        return fluxRecords;
    }
}

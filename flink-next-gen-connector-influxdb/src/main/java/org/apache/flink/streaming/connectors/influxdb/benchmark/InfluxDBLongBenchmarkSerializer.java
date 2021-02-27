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

import com.influxdb.client.write.Point;
import org.apache.flink.api.connector.sink.SinkWriter.Context;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;

public class InfluxDBLongBenchmarkSerializer implements InfluxDBSchemaSerializer<Long> {

    private static final String MEASUREMENT = "test";
    private static final String FIELD_KEY = "longValue";

    @Override
    public Point serialize(final Long element, final Context context) throws Exception {
        final Point dataPoint = new Point(MEASUREMENT);
        dataPoint.addField(FIELD_KEY, String.valueOf(element));
        return dataPoint;
    }
}

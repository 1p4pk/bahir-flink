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
package org.apache.flink.streaming.connectors.influxdb.source;

import com.influxdb.Arguments;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

/** InfluxDB data points */
/* Split Reader (HTTP Server) Line Protocol -> DataPoint -> Deserializer */
public class DataPoint {
    @Getter private final String name;
    private final Map<String, Object> data = new HashMap();
    @Getter @Setter private Number timestamp;

    DataPoint(
            final String measurementName,
            @Nullable final Map<String, Object> data,
            @Nullable final Number timestamp) {
        Arguments.checkNotNull(measurementName, "measurement");
        this.name = measurementName;
        this.data.putAll(data);
        this.timestamp = timestamp;
    }

    public static DataPoint valueOf(final Map<String, Object> data) {
        return new DataPoint(
                String.valueOf(data.remove("measurement")), data, (Number) data.remove("__ts"));
    }

    public DataPoint putField(final String field, final Object value) {
        Arguments.checkNonEmpty(field, "fieldName");
        this.data.put(field, value);
        return this;
    }

    public Object getField(final String field) {
        Arguments.checkNonEmpty(field, "fieldName");
        return this.data.getOrDefault(field, null);
    }

    public DataPoint addTag(final String key, final String value) {
        Arguments.checkNotNull(key, "tagName");
        this.data.put(key, value);
        return this;
    }

    public DataPoint getTag(final String key) {
        Arguments.checkNotNull(key, "tagName");
        this.data.getOrDefault(key, null);
        return this;
    }
}

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
package org.apache.flink.streaming.connectors.influxdb.common;

import com.influxdb.Arguments;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * InfluxDB data point class.
 *
 * <p>{@link InfluxParser} parses line protocol into this data point representation.
 */
public final class DataPoint {
    @Getter private final String name;
    private final Map<String, String> tags = new HashMap();
    private final Map<String, Object> fields = new HashMap();
    @Getter private final Number timestamp;

    DataPoint(final String measurementName, @Nullable final Number timestamp) {
        Arguments.checkNotNull(measurementName, "measurement");
        this.name = measurementName;
        this.timestamp = timestamp;
    }

    /**
     * Converts this {@link DataPoint} to {@link Point}.
     *
     * @return {@link Point}.
     */
    public Point toPoint() {
        final Point out = new Point(this.name);
        out.time(this.timestamp, WritePrecision.NS);
        out.addTags(this.tags);
        out.addFields(this.fields);
        return out;
    }

    /**
     * Adds key and value to field set.
     *
     * @param field Key of field.
     * @param value Value for the field key.
     */
    public void addField(final String field, final Object value) {
        Arguments.checkNonEmpty(field, "fieldName");
        this.fields.put(field, value);
    }

    /**
     * Gets value for a specific field.
     *
     * @param field Key of field.
     * @return value Value for the field key.
     */
    public Object getField(final String field) {
        Arguments.checkNonEmpty(field, "fieldName");
        return this.fields.getOrDefault(field, null);
    }

    /**
     * Adds key and value to tag set.
     *
     * @param key Key of tag.
     * @param value Value for the tag key.
     */
    public void addTag(final String key, final String value) {
        Arguments.checkNotNull(key, "tagName");
        this.tags.put(key, value);
    }

    /**
     * Gets value for a specific tag.
     *
     * @param key Key of tag.
     * @return value Value for the tag key.
     */
    public String getTag(final String key) {
        Arguments.checkNotNull(key, "tagName");
        return this.tags.getOrDefault(key, null);
    }
}

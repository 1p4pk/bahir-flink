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
package org.apache.flink.streaming.connectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBCommittableSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.streaming.connectors.influxdb.sink.commiter.InfluxDBCommitter;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBWriter;
import org.apache.flink.streaming.connectors.util.InfluxDBContainer;
import org.apache.flink.streaming.connectors.util.InfluxDBTestSerializer;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

public class InfluxDBSinkITCase extends TestLogger {

    @ClassRule
    public static final InfluxDBContainer<?> influxDBContainer =
            InfluxDBContainer.createWithDefaultTag();

    static final List<Long> SOURCE_DATA = Arrays.asList(1L, 2L, 3L);

    static final Queue<String> COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

    static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(1, getPointOutput(x).toLineProtocol())
                                            .stream())
                    .collect(Collectors.toList());

    @Before
    void init() {
        COMMIT_QUEUE.clear();
    }

    /**
     * Test the following topology.
     *
     * <pre>
     *     1,2,3               "Poin(1,null,-9223372036854775808)", "(1,null,-9223372036854775808)",
     *                          "(2,null,-9223372036854775808)", "(2,null,-9223372036854775808)",
     *                          "(3,null,-9223372036854775808)", "(3,null,-9223372036854775808)"
     *     (source1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    void testIncrementPipeline() throws Exception {
        final StreamExecutionEnvironment env = this.buildStreamEnv();

        final InfluxDBConfig influxDBConfig =
                InfluxDBConfig.builder()
                        .url(influxDBContainer.getUrl())
                        .username(influxDBContainer.getUsername())
                        .password(influxDBContainer.getPassword())
                        .bucket(influxDBContainer.getBucket())
                        .organization(influxDBContainer.getOrganization())
                        .build();

        final InfluxDBCommitter committer = new InfluxDBCommitter();

        final InfluxDBSink<Long> influxDBSink =
                InfluxDBSink.<Long>builder()
                        .writer(new InfluxDBWriter<>(new InfluxDBTestSerializer(), influxDBConfig))
                        .committer(committer)
                        .committableSerializer(InfluxDBCommittableSerializer.INSTANCE)
                        .build();

        env.addSource(new FiniteTestSource(SOURCE_DATA), BasicTypeInfo.LONG_TYPE_INFO)
                .sinkTo(influxDBSink);

        env.execute();

        final InfluxDBClient influxDBClient = influxDBContainer.getNewInfluxDB();
        final List<FluxTable> tables =
                influxDBClient
                        .getQueryApi()
                        .query("from(bucket:  \"test-bucket\") |> range(start: -1h)");
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                final Point point = new Point(record.getMeasurement());
                point.addTag("LongValue", (String) record.getValueByKey("LongValue"));
                point.addField(
                        Objects.requireNonNull(record.getField()), (String) record.getValue());
                COMMIT_QUEUE.add(point.toLineProtocol());
            }
        }
        assertThat(
                COMMIT_QUEUE,
                containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        return env;
    }

    private static Point getPointOutput(final Long element) {
        final Point dataPoint = new Point("Test");
        dataPoint.addTag("LongValue", String.valueOf(element));
        dataPoint.addField("fieldKey", "fieldValue");
        return dataPoint;
    }
}

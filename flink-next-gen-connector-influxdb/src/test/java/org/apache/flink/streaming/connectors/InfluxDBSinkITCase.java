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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class InfluxDBSinkITCase extends TestLogger {

    static final List<Long> SOURCE_DATA = Arrays.asList(1L, 2L, 3L);

    static final Queue<String> COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

    static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(
                                            2, Tuple3.of(x, null, Long.MIN_VALUE).toString())
                                            .stream())
                    .collect(Collectors.toList());

    @RegisterExtension
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void init() {
        COMMIT_QUEUE.clear();
    }

    /**
     * Test the following topology.
     *
     * <pre>
     *     1,2,3                +1              2,3,4
     *     (source1/1) -----> (map1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    void testIncrementPipeline() throws Exception {
        final StreamExecutionEnvironment env = this.buildStreamEnv();

        env.setParallelism(1);

        final Sink influxDBSink =
                new InfluxDBSink<Long>((Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE);

        env.addSource(new CollectSource(SOURCE_DATA), BasicTypeInfo.LONG_TYPE_INFO)
                .sinkTo(influxDBSink);

        env.execute();

        assertThat(
                COMMIT_QUEUE,
                containsInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.toArray()));
    }

    // create a testing Source
    private static class CollectSource<T> implements SourceFunction<T> {
        private final Iterable<T> elements;

        private CollectSource(final Iterable<T> elements) {
            this.elements = elements;
        }

        @Override
        public void run(final SourceContext<T> sourceContext) throws Exception {
            for (final T element : this.elements) {
                sourceContext.collect(element);
            }
        }

        @Override
        public void cancel() {}
    }

    /** Simple incrementation with map. */
    public static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(final Long record) throws Exception {
            return record + 1;
        }
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //        env.enableCheckpointing(100);
        return env;
    }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.apache.flink.streaming.connectors.util.InfluxDBTestDeserializer;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for the InfluxDB source for Flink. */
class InfluxDBSourceIntegrationTestCase extends TestLogger {

    private static final String HTTP_ADDRESS = "http://localhost";
    private static final int PORT = 8000;

    private static final HttpRequestFactory HTTP_REQUEST_FACTORY =
            new NetHttpTransport().createRequestFactory();
    private static final ExponentialBackOff HTTP_BACKOFF =
            new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(250)
                    .setMaxElapsedTimeMillis(10000)
                    .setMaxIntervalMillis(1000)
                    .setMultiplier(1.3)
                    .setRandomizationFactor(0.5)
                    .build();

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    private final InfluxDBSource<Long> influxDBSource =
            InfluxDBSource.<Long>builder()
                    .setPort(PORT)
                    .setDeserializer(new InfluxDBTestDeserializer())
                    .build();

    @BeforeAll
    static void setUp() {
        CollectSink.VALUES.clear();
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
        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(checkHealthCheckAvailable());

        final int writeResponseCode = writeToInfluxDB("test longValue=1i 1");

        assertEquals(writeResponseCode, HttpURLConnection.HTTP_NO_CONTENT);

        jobClient.cancel();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    @Test
    void testBadRequestException() throws Exception {
        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(checkHealthCheckAvailable());
        final HttpResponseException thrown =
                Assertions.assertThrows(
                        HttpResponseException.class,
                        () -> writeToInfluxDB("malformedLineProtocol_test"));
        assertTrue(thrown.getMessage().contains("Unable to parse line."));
        jobClient.cancel();
    }

    @Test
    void testRequestTooLargeException() throws Exception {
        final InfluxDBSource<Long> influxDBSource =
                InfluxDBSource.<Long>builder()
                        .setPort(PORT)
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .setMaximumLinesPerRequest(2)
                        .build();
        this.env
                .fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(checkHealthCheckAvailable());

        final String lines = "test longValue=1i 1\ntest longValue=1i 1\ntest longValue=1i 1";
        final HttpResponseException thrown =
                Assertions.assertThrows(HttpResponseException.class, () -> writeToInfluxDB(lines));
        assertTrue(
                thrown.getMessage()
                        .contains("Payload too large. Maximum number of lines per request is 2."));
        jobClient.cancel();
    }

    @SneakyThrows
    private static int writeToInfluxDB(final String line) {
        final HttpContent content = ByteArrayContent.fromString("text/plain; charset=utf-8", line);
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildPostRequest(
                        new GenericUrl(String.format("%s:%s/api/v2/write", HTTP_ADDRESS, PORT)),
                        content);
        return request.execute().getStatusCode();
    }

    @SneakyThrows
    private static boolean checkHealthCheckAvailable() {
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildGetRequest(
                        new GenericUrl(String.format("%s:%s/health", HTTP_ADDRESS, PORT)));

        request.setUnsuccessfulResponseHandler(
                new HttpBackOffUnsuccessfulResponseHandler(HTTP_BACKOFF));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(HTTP_BACKOFF));

        final int statusCode = request.execute().getStatusCode();
        return statusCode == HttpURLConnection.HTTP_OK;
    }

    // ---------------- private helper class --------------------

    /** Simple incrementation with map. */
    private static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(final Long record) {
            return record + 1;
        }
    }

    /** create a simple testing sink */
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(final Long value) {
            VALUES.add(value);
        }
    }
}

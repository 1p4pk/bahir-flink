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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Integration test for the InfluxDB source for Flink. */
public class InfluxDBSourceIntegrationTestCase extends TestLogger {
    @Rule public ExpectedException exceptionRule = ExpectedException.none();

    private static final int WAIT_MILLIS = 5000;

    private StreamExecutionEnvironment env = null;
    private InfluxDBSource<Long> influxDBSource = null;
    private HttpRequestFactory requestFactory = null;

    @Before
    public void setUp() {
        CollectSink.VALUES.clear();

        this.influxDBSource =
                InfluxDBSource.<Long>builder()
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .build();
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(1);
        this.requestFactory = new NetHttpTransport().createRequestFactory();
    }

    @SneakyThrows
    @After
    public void tearDown() {}

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

    /**
     * Test the following topology.
     *
     * <pre>
     *     1,2,3                +1              2,3,4
     *     (source1/1) -----> (map1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    public void testIncrementPipeline() throws Exception {
        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        Thread.sleep(WAIT_MILLIS);

        final HttpRequest request = this.createPostRequest("test longValue=1i 1");
        final HttpResponse response = request.execute();

        assertThat(response.getStatusCode(), equalTo(HttpURLConnection.HTTP_NO_CONTENT));

        jobClient.cancel();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    @Test
    public void testBadRequestException() throws Exception {
        this.exceptionRule.expect(HttpResponseException.class);
        this.exceptionRule.expectMessage("Unable to parse line.");
        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        Thread.sleep(WAIT_MILLIS);

        final HttpRequest request = this.createPostRequest("malformedLineProtocol_test");

        request.execute();

        jobClient.cancel();
    }

    @Test
    public void testRequestTooLargeException() throws Exception {
        this.exceptionRule.expect(HttpResponseException.class);
        this.exceptionRule.expectMessage(
                "Payload too large. Maximum number of lines per request is 2.");
        final InfluxDBSource<Long> influxDBSource =
                InfluxDBSource.<Long>builder()
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .setMaximumLinesPerRequest(2)
                        .build();
        this.env
                .fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        Thread.sleep(WAIT_MILLIS);

        final String lines = "test longValue=1i 1\ntest longValue=1i 1\ntest longValue=1i 1";

        final HttpRequest request = this.createPostRequest(lines);
        assertTrue(checkHealthCheckAvailable());
        int writeResponseCode = writeToInfluxDB("test longValue=1i 1");
        assertEquals(204, writeResponseCode);

        request.execute();

        jobClient.cancel();
    }

    @SneakyThrows
    private HttpRequest createPostRequest(final String body) {
        return this.requestFactory.buildPostRequest(
                new GenericUrl("http://localhost:8000/api/v2/write"),
                new ByteArrayContent(
                        "text/plain; charset=utf-8", body.getBytes(StandardCharsets.UTF_8)));
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

    @SneakyThrows
    private static int writeToInfluxDB(String line) {
        HttpContent content = ByteArrayContent.fromString("text/plain; charset=utf-8", line);
        HttpRequest request =
                HTTP_REQUEST_FACTORY.buildPostRequest(
                        new GenericUrl("http://localhost:8000/api/v2/write"), content);
        return request.execute().getStatusCode();
    }

    @SneakyThrows
    private static boolean checkHealthCheckAvailable() {
        HttpRequest request =
                HTTP_REQUEST_FACTORY.buildGetRequest(
                        new GenericUrl("http://localhost:8000/health"));

        request.setUnsuccessfulResponseHandler(
                new HttpBackOffUnsuccessfulResponseHandler(HTTP_BACKOFF));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(HTTP_BACKOFF));

        int statusCode = request.execute().getStatusCode();
        return statusCode == 200;
    }
}

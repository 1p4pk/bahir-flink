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
package org.apache.bahir.benchmark.generator;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.HttpURLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BlockingOffer {

    private static final String WRITE_PROTOCOL = "http";
    private static final String WRITE_API = "/api/v2/write";
    private static final String HEALTH_CHECK_API = "/health";

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

    private final String writeURL;
    private final String healthURL;
    private final int engineRuntime;
    private final int eventsPerSecond;
    private final int eventsPerRequest;
    private final int[] generatedEventsPerSecond;
    private final int[] generatedSuccessfulRequestsPerSecond;
    private final int[] generatedRequestsPerSecond;

    private BufferedWriter measurements;

    private final String host;
    private final int port;

    private long startTime;
    private int lastSecond = 0;

    @SneakyThrows
    public BlockingOffer(
            final String host,
            final int port,
            final int engineRuntime,
            final int eventsPerSecond,
            final int eventsPerRequest,
            final String filePath) {
        this.engineRuntime = engineRuntime;
        this.eventsPerSecond = eventsPerSecond;
        this.eventsPerRequest = eventsPerRequest;
        this.generatedEventsPerSecond = new int[engineRuntime + 300];
        this.generatedSuccessfulRequestsPerSecond = new int[engineRuntime + 300];
        this.generatedRequestsPerSecond = new int[engineRuntime + 300];
        this.host = host;
        this.port = port;
        this.healthURL = WRITE_PROTOCOL + "://" + host + ":" + port + HEALTH_CHECK_API;
        this.writeURL = WRITE_PROTOCOL + "://" + host + ":" + port + WRITE_API;

        final Date date = Calendar.getInstance().getTime();
        final DateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
        final String strDate = dateFormat.format(date);
        final File file =
                new File(
                        filePath.replace(".csv", "_")
                                + "port_"
                                + port
                                + "_host_"
                                + host
                                + "_"
                                + strDate
                                + ".csv");
        if (file.createNewFile()) {
            this.measurements = new BufferedWriter(new FileWriter(file));
        }
    }

    @SneakyThrows
    public void waitForConnection() {
        // wait until client appears
        log.info("Listening on port {} and host {}", this.port, this.host);
        if (this.checkServerAvailability()) {
            this.startTime = System.nanoTime();
            log.info("connected successfully.");
        } else {
            log.error("could not establish a connection to server.");
        }
    }

    @SneakyThrows
    public void offer(final String events) {
        final int currentSecond = (int) ((System.nanoTime() - this.startTime) / 1_000_000_000);
        if (this.generatedEventsPerSecond[currentSecond] < this.eventsPerSecond) {
            if (this.sendRequest(events) == HttpURLConnection.HTTP_NO_CONTENT) {
                this.generatedEventsPerSecond[currentSecond] += this.eventsPerRequest;
                this.generatedSuccessfulRequestsPerSecond[currentSecond] += 1;
            }
            this.generatedRequestsPerSecond[currentSecond] += 1;
        }
        if (currentSecond > this.lastSecond) {
            log.info(
                    this.generatedEventsPerSecond[this.lastSecond]
                            + " events for second "
                            + this.lastSecond);
            this.lastSecond = currentSecond;
        }
    }

    @SneakyThrows
    public void writeFile() {
        this.measurements.write("seconds,events,requests,successfulRequests\n");
        for (int i = 0; i < this.engineRuntime; i++) {
            this.measurements.write(
                    i
                            + ","
                            + this.generatedEventsPerSecond[i]
                            + ","
                            + this.generatedRequestsPerSecond[i]
                            + ","
                            + this.generatedSuccessfulRequestsPerSecond[i]
                            + "\n");
        }
        this.measurements.flush();
        log.info("Wrote result file.");
    }

    @SneakyThrows
    private int sendRequest(final String events) {
        final HttpContent content =
                ByteArrayContent.fromString("text/plain; charset=utf-8", events);
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildPostRequest(new GenericUrl(this.writeURL), content);
        return request.execute().getStatusCode();
    }

    @SneakyThrows
    private boolean checkServerAvailability() {
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildGetRequest(new GenericUrl(this.healthURL));
        request.setUnsuccessfulResponseHandler(
                new HttpBackOffUnsuccessfulResponseHandler(HTTP_BACKOFF));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(HTTP_BACKOFF));
        return request.execute().getStatusCode() == HttpURLConnection.HTTP_OK;
    }
}

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
package org.apache.flink.streaming.connectors.influxdb.source.reader;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.common.InfluxParser;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@link DataPoint}.
 */
@Slf4j
public class InfluxDBSplitReader implements SplitReader<DataPoint, InfluxDBSplit> {

    private static final int INGEST_QUEUE_CAPACITY = 1000;
    private static final int MAXIMUM_LINES_PER_REQUEST = 1000;
    private static final long ENQUEUE_WAIT_TIME = 5;

    private static final int DEFAULT_PORT = 8000;
    private HttpServer server = null;

    private final FutureCompletingBlockingQueue<List<DataPoint>> ingestionQueue =
            new FutureCompletingBlockingQueue<>(INGEST_QUEUE_CAPACITY);

    private final InfluxParser parser = new InfluxParser();
    private InfluxDBSplit split;

    @SneakyThrows
    @Override
    public RecordsWithSplitIds<DataPoint> fetch() throws IOException {
        if (this.split == null) {
            return null;
        }
        final InfluxDBSplitRecords<DataPoint> recordsBySplits =
                new InfluxDBSplitRecords<>(this.split.splitId());

        this.ingestionQueue.getAvailabilityFuture().get();
        final List<DataPoint> requests = this.ingestionQueue.poll();
        if (requests == null) {
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }
        recordsBySplits.addAll(requests);
        recordsBySplits.prepareForRead();
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<InfluxDBSplit> splitsChange) {
        if (splitsChange.splits().size() == 0) {
            return;
        }
        this.split = splitsChange.splits().get(0);

        if (this.server != null) {
            return;
        }
        try {
            this.server = HttpServer.create(new InetSocketAddress(DEFAULT_PORT), 0);
        } catch (final IOException e) {
            throw new RuntimeException(
                    "Unable to start HTTP Server on Port " + DEFAULT_PORT + ": " + e.getMessage());
        }

        this.server.createContext("/api/v2/write", new InfluxDBAPIHandler());
        this.server.setExecutor(null); // creates a default executor
        this.server.start();
    }

    @Override
    public void wakeUp() {
        this.ingestionQueue.notifyAvailable();
    }

    @Override
    public void close() throws Exception {
        if (this.server != null) {
            this.server.stop(1); // waits max 1 second for pending requests to finish
        }
    }

    // ---------------- private helper class --------------------

    private class InfluxDBAPIHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange t) throws IOException {
            final BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(t.getRequestBody(), StandardCharsets.UTF_8));

            try {
                String line;
                final List<DataPoint> points = new ArrayList<>();
                int n = 0;
                while ((line = in.readLine()) != null) {
                    final DataPoint dataPoint =
                            InfluxDBSplitReader.this.parser.parseToDataPoint(line);
                    points.add(dataPoint);
                    n++;
                    if (n > MAXIMUM_LINES_PER_REQUEST) {
                        throw new RequestTooLargeException(
                                "Payload too large. Maximum number of lines per request is "
                                        + MAXIMUM_LINES_PER_REQUEST
                                        + ".");
                    }
                }

                boolean result =
                        CompletableFuture.supplyAsync(
                                        () -> {
                                            boolean success = false;
                                            try {
                                                success =
                                                        InfluxDBSplitReader.this.ingestionQueue.put(
                                                                InfluxDBSplitReader.this
                                                                        .split
                                                                        .splitId()
                                                                        .hashCode(),
                                                                points);
                                            } catch (InterruptedException e) {
                                                success = false;
                                            }
                                            return success;
                                        })
                                .get(ENQUEUE_WAIT_TIME, TimeUnit.SECONDS);

                if (!result) {
                    throw new TimeoutException("Failed to enqueue");
                }

                t.sendResponseHeaders(204, -1);
                InfluxDBSplitReader.this.ingestionQueue.notifyAvailable();
            } catch (final ParseException e) {
                // 400 Bad Request
                this.sendResponse(t, 400, e.getMessage());
            } catch (final RequestTooLargeException e) {
                // 413 Payload Too Large
                this.sendResponse(t, 413, e.getMessage());
            } catch (final TimeoutException e) {
                // 429 Too Many Requests
                this.sendResponse(t, 429, "Server overloaded");
                log.error(e.getMessage());
            } catch (ExecutionException | InterruptedException e) {
                this.sendResponse(t, 500, "Server Error");
                log.error(e.getMessage());
            }
        }

        private void sendResponse(
                @NotNull final HttpExchange t,
                final int responseCode,
                @NotNull final String message)
                throws IOException {
            final byte[] response = message.getBytes();
            t.sendResponseHeaders(responseCode, response.length);
            final OutputStream os = t.getResponseBody();
            os.write(response);
            os.close();
        }
    }

    private static class RequestTooLargeException extends RuntimeException {
        RequestTooLargeException(final String message) {
            super(message);
        }
    }

    private static class InfluxDBSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final List<T> records;
        private Iterator<T> recordIterator;
        private final String splitId;

        private InfluxDBSplitRecords(final String splitId) {
            this.splitId = splitId;
            this.records = new ArrayList<>();
        }

        private boolean addAll(final List<T> records) {
            return this.records.addAll(records);
        }

        private void prepareForRead() {
            this.recordIterator = this.records.iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (this.recordIterator.hasNext()) {
                return this.splitId;
            }
            return null;
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}

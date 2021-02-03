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

import static org.apache.flink.util.Preconditions.checkNotNull;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.influxdb.source.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxParser;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@link DataPoint}.
 */
public class InfluxDBSplitReader implements SplitReader<DataPoint, InfluxDBSplit> {

    private static final int INGEST_QUEUE_CAPACITY = 1000;
    private static final int MAXIMUM_LINES_PER_REQUEST = 1000;

    private static final int DEFAULT_PORT = 8000;
    private HttpServer server = null;

    private final ArrayBlockingQueue<List<DataPoint>> ingestionQueue =
            new ArrayBlockingQueue<>(INGEST_QUEUE_CAPACITY);
    private final InfluxParser parser = new InfluxParser();
    private InfluxDBSplit split;

    @Override
    public RecordsWithSplitIds<DataPoint> fetch() throws IOException {
        if (split == null) {
            return null;
        }
        // Queue
        final InfluxDBSplitRecords<DataPoint> recordsBySplits = new InfluxDBSplitRecords<>();
        try {
            // TODO blocking call -> handle wakeUp signal
            final Collection<DataPoint> recordsForSplit =
                    recordsBySplits.recordsForSplit(split.splitId());

            // TODO blocking call -> handle wakeUp signal
            List<List<DataPoint>> requests = new ArrayList<>();
            requests.add(this.ingestionQueue.take());
            this.ingestionQueue.drainTo(requests);
            for (List<DataPoint> request : requests) {
                recordsForSplit.addAll(request);
            }

            recordsBySplits.prepareForRead();
            return recordsBySplits;
        } catch (final InterruptedException e) {
            // TODO: check what to do with split
            e.printStackTrace();
            return null;
        }
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
            // TODO add splits back
            e.printStackTrace();
        }

        this.server.createContext("/api/v2/write", new InfluxDBAPIHandler());
        this.server.setExecutor(null); // creates a default executor
        this.server.start();
    }

    @Override
    public void wakeUp() {
        // TODO make fetch return instantly when this method is called (see SplitReader interface)
    }

    @Override
    public void close() throws Exception {
        if (this.server != null) {
            // TODO: check what to do with queue
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

                InfluxDBSplitReader.this.ingestionQueue.put(points); // TODO use offer with timeout

                t.sendResponseHeaders(204, -1);
            } catch (final ParseException e) {
                final byte[] response = e.getMessage().getBytes();
                // 400 Bad Request
                t.sendResponseHeaders(400, response.length);
                OutputStream os = t.getResponseBody();
                os.write(response);
                os.close();
            } catch (final RequestTooLargeException e) {
                final byte[] response = e.getMessage().getBytes();
                // 413 Payload Too Large
                t.sendResponseHeaders(413, response.length);
                OutputStream os = t.getResponseBody();
                os.write(response);
                os.close();
            } catch (
                    final InterruptedException
                            e) { // TODO InterruptedException may not be the right type
                final byte[] response = "Server overloaded".getBytes();
                // 429 Too Many Requests
                t.sendResponseHeaders(429, response.length);
                OutputStream os = t.getResponseBody();
                os.write(response);
                os.close();

                // TODO rethrow/forward exception so that Flink knows the ingestion queue was full
                e.printStackTrace();
            }
        }
    }

    private static class RequestTooLargeException extends RuntimeException {
        RequestTooLargeException(String message) {
            super(message);
        }
    }

    private static class InfluxDBSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final Map<String, Collection<T>> recordsBySplits;
        private Iterator<Map.Entry<String, Collection<T>>> splitIterator;
        private String currentSplitId;
        private Iterator<T> recordIterator;

        private InfluxDBSplitRecords() {
            this.recordsBySplits = new HashMap<>();
        }

        private Collection<T> recordsForSplit(final String splitID) {
            return this.recordsBySplits.computeIfAbsent(splitID, id -> new ArrayList<>());
        }

        private void prepareForRead() {
            this.splitIterator = this.recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (this.splitIterator.hasNext()) {
                final Map.Entry<String, Collection<T>> entry = this.splitIterator.next();
                this.currentSplitId = entry.getKey();
                this.recordIterator = entry.getValue().iterator();
                return this.currentSplitId;
            } else {
                this.currentSplitId = null;
                this.recordIterator = null;
                return null;
            }
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            checkNotNull(
                    this.currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return this.recordsBySplits.keySet();
        }
    }
}

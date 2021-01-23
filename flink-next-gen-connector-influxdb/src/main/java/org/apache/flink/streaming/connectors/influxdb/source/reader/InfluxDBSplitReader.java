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
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.influxdb.source.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@link DataPoint}.
 */
public class InfluxDBSplitReader implements SplitReader<DataPoint, InfluxDBSplit> {

    private static final int INGEST_QUEUE_CAPACITY = 1000;
    private HttpServer server = null;
    private final ArrayBlockingQueue<DataPoint> ingestionQueue =
            new ArrayBlockingQueue<>(INGEST_QUEUE_CAPACITY);
    private final ArrayBlockingQueue<InfluxDBSplit> splitQueue =
            new ArrayBlockingQueue<>(INGEST_QUEUE_CAPACITY);

    @Override
    public RecordsWithSplitIds<DataPoint> fetch() throws IOException {
        // Queue
        final InfluxDBSplitRecords<DataPoint> recordsBySplits = new InfluxDBSplitRecords<>();
        final InfluxDBSplit nextSplit;
        try {
            nextSplit = this.splitQueue.take();
        } catch (final InterruptedException e) {
            // TODO: check what to do with split
            e.printStackTrace();
            return null;
        }
        final Collection<DataPoint> recordsForSplit =
                recordsBySplits.recordsForSplit(nextSplit.splitId());
        try {
            recordsForSplit.add(this.ingestionQueue.take());
            this.ingestionQueue.drainTo(recordsForSplit);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        // OLD hard coded
        // recordsForSplit.add(new Tuple2(1L, 1L));
        // recordsForSplit.add(new Tuple2(2L, 2L));
        // recordsForSplit.add(new Tuple2(3L, 3L));
        recordsBySplits.prepareForRead();
        try {
            Thread.sleep(30000);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<InfluxDBSplit> splitsChange) {
        splitsChange.splits().stream()
                .forEach(
                        split -> {
                            try {
                                this.splitQueue.put(split);
                            } catch (final InterruptedException e) {
                                // TODO: Check what to do with failing split
                                e.printStackTrace();
                            }
                        });
        if (this.server != null) {
            // TODO: Add split back to enumerator if does not match own split
            return;
        }
        try {
            this.server = HttpServer.create(new InetSocketAddress(8000), 0);
        } catch (final IOException e) {
            // TODO: Add split back to enumerator
            e.printStackTrace();
        }
        this.server.createContext("/api/v2/write", new InfluxDBAPIHandler());
        this.server.setExecutor(null); // creates a default executor
        this.server.start();
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (this.server != null) {
            // TODO: check what to do with queue
            this.server.stop(1); // waits max 1 second for pending requests to finish
        }
    }

    // ---------------- private helper class --------------------

    private static class InfluxDBAPIHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange t) throws IOException {
            final String response = "This is the response";
            t.sendResponseHeaders(200, response.length());
            final OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
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
            return null;
        }
    }
}

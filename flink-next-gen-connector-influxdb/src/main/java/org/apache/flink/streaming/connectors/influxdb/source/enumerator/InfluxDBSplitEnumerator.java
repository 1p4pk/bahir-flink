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
package org.apache.flink.streaming.connectors.influxdb.source.enumerator;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.jetbrains.annotations.Nullable;

/** The enumerator class for InfluxDB source. */
@Internal
public class InfluxDBSplitEnumerator
        implements SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> {

    private final SplitEnumeratorContext<InfluxDBSplit> context;
    private boolean firstRun = true;

    public InfluxDBSplitEnumerator(final SplitEnumeratorContext<InfluxDBSplit> context) {
        this.context = checkNotNull(context);
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void handleSplitRequest(final int i, @Nullable final String s) {
        if (this.firstRun) {
            this.context.assignSplit(new InfluxDBSplit("0"), i);
            this.firstRun = false;
        } else {
            this.context.signalNoMoreSplits(i);
        }
    }

    @Override
    public void addSplitsBack(final List<InfluxDBSplit> list, final int i) {}

    @Override
    public void addReader(final int i) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public InfluxDBSourceEnumState snapshotState() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
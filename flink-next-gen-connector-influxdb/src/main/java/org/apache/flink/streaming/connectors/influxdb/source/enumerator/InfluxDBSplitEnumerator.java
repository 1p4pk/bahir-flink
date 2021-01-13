package org.apache.flink.streaming.connectors.influxdb.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The enumerator class for InfluxDB source. */
@Internal
public class InfluxDBSplitEnumerator implements SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> {

    private final SplitEnumeratorContext<InfluxDBSplit> context;

    public InfluxDBSplitEnumerator(SplitEnumeratorContext<InfluxDBSplit> context) {
        this.context = checkNotNull(context);
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {
        context.assignSplit(new InfluxDBSplit(), i);
    }

    @Override
    public void addSplitsBack(List<InfluxDBSplit> list, int i) {

    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public InfluxDBSourceEnumState snapshotState() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

package org.apache.flink.streaming.connectors.influxdb.source.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The source reader for the InfluxDB line protocol.
 */
 public class InfluxDBSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<Tuple2<T, Long>, T, InfluxDBSplit, InfluxDBSplit> {

    public InfluxDBSourceReader(Supplier<InfluxDBSplitReader<T>> splitReaderSupplier,
                                RecordEmitter<Tuple2<T, Long>, T, InfluxDBSplit> recordEmitter,
                                Configuration config,
                                SourceReaderContext context) {
        super(splitReaderSupplier::get, recordEmitter, config, context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, InfluxDBSplit> map) {

    }

    @Override
    protected InfluxDBSplit initializedState(InfluxDBSplit influxDBSplit) {
        return null;
    }

    @Override
    protected InfluxDBSplit toSplitType(String s, InfluxDBSplit influxDBSplitState) {
        return null;
    }
}

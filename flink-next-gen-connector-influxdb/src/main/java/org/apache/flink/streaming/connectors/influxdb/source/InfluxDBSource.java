package org.apache.flink.streaming.connectors.influxdb.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumState;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumStateSerializer;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSplitEnumerator;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBRecordEmitter;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSourceReader;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSplitReader;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplitSerializer;

import java.util.function.Supplier;

/**
 * The Source implementation of InfluxDB. Please use a {@link InfluxDBSourceBuilder} to construct a {@link
 * InfluxDBSource}. The following example shows how to create an InfluxDBSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * add example
 * }</pre>
 *
 * <p>See {@link InfluxDBSourceBuilder} for more details.
 *
 * @param <OUT> the output type of the source.
 */
public class InfluxDBSource<Long> implements Source<Long, InfluxDBSplit, InfluxDBSourceEnumState>,
        ResultTypeQueryable<Long> {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Long, InfluxDBSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        Supplier<InfluxDBSplitReader<Long>> splitReaderSupplier = () -> new InfluxDBSplitReader<>();
        InfluxDBRecordEmitter<Long> recordEmitter = new InfluxDBRecordEmitter<>();
        Configuration config = new Configuration();
        config.setInteger("ELEMENT_QUEUE_CAPACITY", 1000);
        return new InfluxDBSourceReader<>(splitReaderSupplier, recordEmitter, config, sourceReaderContext);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> createEnumerator(SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext) throws Exception {
        return new InfluxDBSplitEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> restoreEnumerator(SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext, InfluxDBSourceEnumState influxDBSourceEnumState) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSplit> getSplitSerializer() {
        return new InfluxDBSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new InfluxDBSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<Long> getProducedType() {
        return (TypeInformation<Long>) Types.LONG;
    }
}

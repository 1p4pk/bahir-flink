package org.apache.flink.streaming.connectors.influxdb.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

public class InfluxDBRecordEmitter<T> implements RecordEmitter<Tuple2<T, Long>, T, InfluxDBSplit> {
    @Override
    public void emitRecord(Tuple2<T, Long> element, SourceOutput<T> output, InfluxDBSplit splitState) throws Exception {
        output.collect(element.f0, element.f1);
    }
}

package org.apache.bahir.benchmark;

import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;

public class InfluxDBBenchmarkDeserializer implements InfluxDBDataPointDeserializer<DataPoint> {

    @Override
    public DataPoint deserialize(final DataPoint dataPoint) {
        dataPoint.addField("deserializeTimestamp", System.currentTimeMillis() / 1000L);
        return dataPoint;
    }
}

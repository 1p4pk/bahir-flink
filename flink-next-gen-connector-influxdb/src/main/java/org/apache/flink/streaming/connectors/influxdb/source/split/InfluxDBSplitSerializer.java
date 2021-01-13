package org.apache.flink.streaming.connectors.influxdb.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * InfluxDBSplit}.
 */
public class InfluxDBSplitSerializer implements SimpleVersionedSerializer<InfluxDBSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(InfluxDBSplit influxDBSplit) throws IOException {
        return new byte[0];
    }

    @Override
    public InfluxDBSplit deserialize(int i, byte[] bytes) throws IOException {
        return new InfluxDBSplit();
    }
}

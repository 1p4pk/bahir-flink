package org.apache.flink.streaming.connectors.influxdb.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class InfluxDBSourceEnumStateSerializer implements SimpleVersionedSerializer<InfluxDBSourceEnumState> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(InfluxDBSourceEnumState influxDBSourceEnumState) throws IOException {
        return new byte[0];
    }

    @Override
    public InfluxDBSourceEnumState deserialize(int i, byte[] bytes) throws IOException {
        return new InfluxDBSourceEnumState();
    }
}

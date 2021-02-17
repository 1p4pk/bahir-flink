package org.apache.flink.streaming.connectors.influxdb.benchmark;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.api.connector.sink.SinkWriter.Context;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;

public class InfluxDBBenchmarkSerializer implements InfluxDBSchemaSerializer<Long> {

    @Override
    public Point serialize(final Long element, final Context context) throws Exception {
        final Point dataPoint = new Point("testSink");
        dataPoint.addTag("simpleTag", String.valueOf(element));
        dataPoint.addField("fieldKey", "fieldValue");
        dataPoint.time(System.nanoTime(), WritePrecision.NS);
        return dataPoint;
    }
}

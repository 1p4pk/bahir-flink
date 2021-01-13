package org.apache.flink.streaming.connectors.influxdb.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/** A {@link SourceSplit} for a InfluxDB split. */
public class InfluxDBSplit implements SourceSplit, Serializable {

    /** The unique ID of the split. Unique within the scope of this source. */
//    private final String id;

    @Override
    public String splitId() {
        return "0";
    }
}

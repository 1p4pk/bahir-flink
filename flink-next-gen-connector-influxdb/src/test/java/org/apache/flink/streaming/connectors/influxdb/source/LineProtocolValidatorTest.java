package org.apache.flink.streaming.connectors.influxdb.source;

import org.junit.jupiter.api.Test;

public class LineProtocolValidatorTest {

    static final String INPUT =
            "myMeasurement,tag1=value1,tag2=value2 fieldKey=\"fieldValue\" 1556813561098000000";

    @Test
    public void testVaidator() {
        LineProtocolValidator.parse(INPUT);
    }
}

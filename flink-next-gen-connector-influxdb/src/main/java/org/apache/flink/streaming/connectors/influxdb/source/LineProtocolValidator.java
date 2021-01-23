package org.apache.flink.streaming.connectors.influxdb.source;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

// WIP
public class LineProtocolValidator {
    public static void parse(final String lineProtocol) {
        String[] split = lineProtocol.split("\\s+");
        String mesurementAndTags = split[0];
        String fields = split[1];
        String timestamp = split[2];

        String mesurement = mesurementAndTags.substring(0, mesurementAndTags.indexOf(','));
        String tags = mesurementAndTags.substring(mesurementAndTags.indexOf(',')).substring(1);

        Map<String, String> tagsValue = extractTagsValue(tags);
        Map<String, String> fielgsValue = extractTagsValue(fields);

        System.out.println("Still going on");
    }

    private static Map<String, String> extractTagsValue(final String tags) {
        return Arrays.stream(tags.split(","))
                .map(tag -> tag.split("="))
                .collect(Collectors.toMap(strings -> strings[0], strings -> strings[1]));
    }
}

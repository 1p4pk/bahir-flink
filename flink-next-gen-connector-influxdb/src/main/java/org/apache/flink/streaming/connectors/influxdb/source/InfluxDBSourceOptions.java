package org.apache.flink.streaming.connectors.influxdb.source;

import java.util.Properties;
import java.util.function.Function;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/* Configurations for a InfluxDBSource. */
public class InfluxDBSourceOptions {

    public static final ConfigOption<Long> ENQUEUE_WAIT_TIME =
            ConfigOptions.key("enqueue.wait.time")
                    .longType()
                    .defaultValue(5L)
                    .withDescription("The time out for enqueuing an HTTP request to the queue.");

    public static final ConfigOption<Integer> INGEST_QUEUE_CAPACITY =
            ConfigOptions.key("ingest.queue.capacity")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of queue that buffers HTTP requests data points before fetching.");

    public static final ConfigOption<Integer> MAXIMUM_LINES_PER_REQUEST =
            ConfigOptions.key("maximum.lines.per.request")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum number of lines that should be parsed per HTTP request.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(8000)
                    .withDescription(
                            "TCP port on which the splitreader's HTTP server is running on.");

    @SuppressWarnings("unchecked")
    public static <T> T getOption(
            final Properties props,
            final ConfigOption configOption,
            final Function<String, T> parser) {
        final String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}

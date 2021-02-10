package org.apache.bahir.benchmark.generator;

import java.util.concurrent.ExecutorService;

public class SimpleLineProtocolGenerator extends LineProtocolGenerator {

    private int fieldValue = 0;

    public SimpleLineProtocolGenerator(
            final long eventsPerSecond,
            final long eventsPerRequest,
            final long timeInSeconds,
            final ExecutorService executor) {
        super(eventsPerSecond, eventsPerRequest, timeInSeconds, executor);
    }

    public SimpleLineProtocolGenerator(
            final long eventsPerSecond, final long eventsPerRequest, final long timeInSeconds) {
        super(eventsPerSecond, eventsPerRequest, timeInSeconds);
    }

    @Override
    protected String generateEventsPerRequest() {
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < this.eventsPerRequest; i++) {
            final long eventTime = System.currentTimeMillis() / 1000L;
            stringBuilder.append("testGenerator"); // measurement
            stringBuilder.append(",simpleTag=testTag "); // simple tag
            stringBuilder.append("fieldCount=");
            stringBuilder.append(this.fieldValue++);
            stringBuilder.append("i ");
            stringBuilder.append(eventTime);
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }
}

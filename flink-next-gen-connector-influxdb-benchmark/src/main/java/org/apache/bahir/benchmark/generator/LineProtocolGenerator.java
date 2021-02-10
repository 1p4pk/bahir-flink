package org.apache.bahir.benchmark.generator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class LineProtocolGenerator {

    protected final long eventsPerSecond;
    protected final long eventsPerRequest;
    protected final long timeInSeconds;
    private final ExecutorService executor;

    LineProtocolGenerator(
            final long eventsPerSecond,
            final long eventsPerRequest,
            final long timeInSeconds,
            final ExecutorService executor) {
        this.eventsPerSecond = eventsPerSecond;
        this.eventsPerRequest = eventsPerRequest;
        this.timeInSeconds = timeInSeconds;
        this.executor = executor;
    }

    LineProtocolGenerator(
            final long eventsPerSecond, final long eventsPerRequest, final long timeInSeconds) {
        this(eventsPerSecond, eventsPerRequest, timeInSeconds, Executors.newSingleThreadExecutor());
    }

    protected abstract String generateEventsPerRequest();

    public CompletableFuture<Boolean> generate(final BlockingOffer blockingOffer) {
        return CompletableFuture.supplyAsync(
                () -> this.sendEventsTimeAware(blockingOffer), this.executor);
    }

    public Long getTotalEvents() {
        return this.eventsPerSecond * this.timeInSeconds;
    }

    public void shutdown() {
        this.executor.shutdown();
    }

    // ------------- private helpers  --------------

    private Boolean sendEventsTimeAware(final BlockingOffer blockingOffer) {
        long totalSentEvents = 0;
        final long totalEvents = this.getTotalEvents();
        final long startTime = System.nanoTime();
        long timeNow = startTime;
        final long finalEndTime = startTime + TimeUnit.SECONDS.toNanos(this.timeInSeconds);

        while (totalSentEvents < totalEvents && timeNow <= finalEndTime) {
            timeNow = System.nanoTime();
            final long nanoDifference = timeNow - startTime;

            final long currentEventTarget = nanoDifference * this.eventsPerSecond / 1_000_000_000;
            final long missingEvents = currentEventTarget - totalSentEvents;

            // Ensures that we don't sent too many events
            final long eventsToBeSent = Math.min(totalEvents - totalSentEvents, missingEvents);

            // Send the events
            for (int i = 0; i < eventsToBeSent; i++) {
                try {
                    final String events = this.generateEventsPerRequest();
                    blockingOffer.offer(events);
                    totalSentEvents += 1;
                } catch (final IllegalStateException e) {
                    log.info(
                            "Events remaining {} Events sent {}",
                            totalEvents - totalSentEvents,
                            totalSentEvents);
                    this.shutdown();
                    return false;
                }
                log.trace("Events to be sent {}", eventsToBeSent);
                if (i % 10_000 == 0 && System.nanoTime() > finalEndTime) {
                    this.shutdown();
                    return false; // To check if we have exceeded our max time
                }
            }
        }
        log.info(
                "Finished Events left {},d, Events sent {},d",
                totalEvents - totalSentEvents,
                totalSentEvents);

        this.shutdown();
        return true;
    }
}

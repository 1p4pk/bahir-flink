/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.benchmark.generator;

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

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
package org.apache.flink.streaming.connectors.influxdb.benchmark.influxDBConfig;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

@Slf4j
public final class InfluxDBClientConfig {
    private final InfluxDBClient influxDBClient;

    @Getter private static final String username = "test-user";
    @Getter private static final String password = "test-password";
    @Getter private static final String organization = "test-org";
    @Getter private static final String bucket = "test-bucket";
    @Getter private static final String url = "http://localhost:8086";

    private InfluxDBClientConfig() {
        final OkHttpClient.Builder client =
                new OkHttpClient.Builder()
                        .connectTimeout(10, TimeUnit.MINUTES)
                        .readTimeout(10, TimeUnit.MINUTES)
                        .writeTimeout(10, TimeUnit.MINUTES)
                        .retryOnConnectionFailure(true);
        final InfluxDBClientOptions influxDBClientOptions =
                InfluxDBClientOptions.builder()
                        .url(url)
                        .authenticate(username, password.toCharArray())
                        .bucket(bucket)
                        .org(organization)
                        .okHttpClient(client)
                        .build();
        this.influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);
    }

    public static InfluxDBClient getClient() {
        final InfluxDBClientConfig influxDBClientConfig = new InfluxDBClientConfig();
        return influxDBClientConfig.influxDBClient;
    }

    public static void clearData(final InfluxDBClient influxDBClient) {
        final Bucket oldBucket = influxDBClient.getBucketsApi().findBucketByName(bucket);
        influxDBClient.getBucketsApi().deleteBucket(oldBucket);
        final Bucket newBucket = new Bucket();
        newBucket.setName(bucket);
        final Organization org = influxDBClient.getOrganizationsApi().findOrganizations().get(0);
        newBucket.setOrgID(org.getId());
        influxDBClient.getBucketsApi().createBucket(newBucket);
        log.trace("clear data");
    }
}

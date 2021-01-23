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
package org.apache.flink.streaming.connectors.influxdb;

// TODO: WIP

public class InfluxDBConfig {

    private String url;
    private String username;
    private String password;
    private String bucket;
    private String organization;
    private int retention = 0;
    //    private String retentionUnit = RetentionUnit.NANOSECONDS.label;

    public InfluxDBConfig(InfluxDBConfig.Builder builder) {}

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getBucket() {
        return bucket;
    }

    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String bucket;
        private String organization;

        public Builder(
                String url, String username, String password, String bucket, String organization) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.bucket = bucket;
            this.organization = organization;
        }
    }

    //    /** @return a influxDb client */
    //    public InfluxDBClient getNewInfluxDB() {
    //        final InfluxDBClientOptions influxDBClientOptions =
    //                InfluxDBClientOptions.builder()
    //                        .url(this.getUrl())
    //                        .authenticate(this.username, this.password.toCharArray())
    //                        .bucket(this.bucket)
    //                        .org(this.organization)
    //                        .build();
    //        return InfluxDBClientFactory.create(influxDBClientOptions);
    //    }
}

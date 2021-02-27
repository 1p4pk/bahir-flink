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
package org.apache.flink.streaming.connectors.influxdb.benchmark;

import java.util.Arrays;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.influxdb.benchmark.commands.SinkCommand;
import org.apache.flink.streaming.connectors.influxdb.benchmark.commands.SourceCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Slf4j
@Command(
        name = "benchmark",
        mixinStandardHelpOptions = true,
        subcommands = {SinkCommand.class, SourceCommand.class},
        commandListHeading = "%nCommands:%n%n")
public class BenchmarkApplication implements Callable<Void> {

    @Spec private CommandSpec spec;

    public static void main(final String[] args) {
        log.info("Starting benchmark with args: {}", Arrays.toString(args));
        System.exit(new CommandLine(new BenchmarkApplication()).execute(args));
    }

    @Override
    public Void call() throws Exception {
        throw new CommandLine.ParameterException(
                this.spec.commandLine(), "Missing required subcommand");
    }
}

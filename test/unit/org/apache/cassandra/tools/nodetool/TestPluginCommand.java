/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Plugin command used by {@link PluginCommandTest}. It is hidden because the test classpath is only visible
 * to the in-process nodetool, so listing it would make the help output of {@code bin/nodetool} differ from
 * the files under {@code test/resources/nodetool/help}.
 */
@Command(name = "plugintest", hidden = true, description = "Print the arguments this command received")
public class TestPluginCommand extends AbstractCommand
{
    @Option(names = { "--keyspace" }, description = "Keyspace name")
    private String keyspace;

    @Parameters(index = "0", paramLabel = "table", description = "Table name")
    private String table;

    @Override
    protected void execute(NodeProbe probe)
    {
        probe.output().out.printf("plugintest keyspace=%s table=%s%n", keyspace, table);
    }
}

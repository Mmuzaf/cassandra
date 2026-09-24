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

import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.management.CommandInvokerService;
import org.apache.cassandra.management.SimpleCommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandExecutionsTest extends CQLNodetoolProtocolTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    /**
     * Seeds history in-process. Under {@code static_mbean} nodetool never reaches the command service, so a
     * nodetool call would leave nothing for the other protocols to read.
     */
    private static UUID seedExecution() throws Exception
    {
        return CommandInvokerService.instance.invokeCommand("version", CommandExecutionsTest::emptyArgs).getExecutionId();
    }

    private static CommandExecutionArgs emptyArgs()
    {
        return new SimpleCommandExecutionArgs(Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testListShowsSeededExecution() throws Exception
    {
        UUID executionId = seedExecution();

        ToolRunner.ToolResult result = invokeNodetool("commandexecutions");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains(executionId.toString()).contains("version").contains("COMPLETED");
    }

    @Test
    public void testLatestOfCommand() throws Exception
    {
        UUID executionId = seedExecution();

        ToolRunner.ToolResult result = invokeNodetool("commandexecutions", "--command", "version", "--latest");
        result.assertOnCleanExit();
        assertThat(result.getStdout())
            .contains("status: COMPLETED")
            .contains("command: version")
            .contains(executionId.toString());
    }

    @Test
    public void testById() throws Exception
    {
        UUID executionId = seedExecution();

        ToolRunner.ToolResult result = invokeNodetool("commandexecutions", executionId.toString());
        result.assertOnCleanExit();
        assertThat(result.getStdout())
            .contains("execution_id: " + executionId)
            .contains("status: COMPLETED")
            .contains("(output not retained for synchronous executions)");
    }

    @Test
    public void testLatestWithoutCommandFails()
    {
        ToolRunner.ToolResult result = invokeNodetool("commandexecutions", "--latest");
        result.asserts().failure();
        assertThat(result.getStdout()).contains("--latest requires --command");
    }

    @Test
    public void testIdWithOptionFails() throws Exception
    {
        ToolRunner.ToolResult result = invokeNodetool("commandexecutions", seedExecution().toString(), "--latest");
        result.asserts().failure();
        assertThat(result.getStdout()).contains("An execution id cannot be combined with --command or --latest");
    }

    @Test
    public void testUnknownIdFails()
    {
        UUID unknown = UUID.randomUUID();

        ToolRunner.ToolResult result = invokeNodetool("commandexecutions", unknown.toString());
        result.asserts().failure();
        assertThat(result.getStdout()).contains("No retained execution with id '" + unknown + '\'');
    }
}

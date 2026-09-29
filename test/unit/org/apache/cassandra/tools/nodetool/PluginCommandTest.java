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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TestNodetoolCommandsProvider} supplies {@link TestPluginCommand}, which is not part of
 * {@link NodetoolCommand}, so every protocol here runs a command that nodetool picked up from the classpath.
 */
public class PluginCommandTest extends CQLNodetoolProtocolTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testPluginCommandRuns()
    {
        ToolRunner.ToolResult result = invokeNodetool("plugintest", "--keyspace", "ks", "tbl");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("plugintest keyspace=ks table=tbl");
    }
}

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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

@RunWith(Parameterized.class)
public abstract class CQLToolRunnerTester extends CQLTester
{
    public static final Map<String, ToolHandler> runnersMap = Map.of(
        "invokeNodetool", ToolRunner::invokeNodetool,
        "invokeNodetoolInJvmV1", ToolRunner::invokeNodetoolInJvmV1,
        "invokeNodetoolInJvmV2", ToolRunner::invokeNodetoolInJvmV2);

    @Parameterized.Parameter
    public String runner;

    @Parameterized.Parameters(name = "{0}")
    public static Set<String> runners()
    {
        return runnersMap.keySet();
    }

    @BeforeClass
    public static void setupNetwork() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    protected ToolRunner.ToolResult invokeNodetool(String... args)
    {
        return runnersMap.get(runner).execute(args);
    }

    public interface ToolHandler
    {
        ToolRunner.ToolResult execute(String... args);
        default ToolRunner.ToolResult execute(List<String> args) { return execute(args.toArray(new String[0])); }
    }
}

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

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see Info
 */
public class InfoTest extends CQLNodetoolProtocolTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testInfoOutput()
    {
        ToolRunner.ToolResult tool = invokeNodetool("info");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();

        assertThat(stdout).contains("ID");
        assertThat(stdout).contains("Gossip active");
        assertThat(stdout).contains("Native Transport active");
        assertThat(stdout).contains("Load");
        assertThat(stdout).contains("Uncompressed load");
        assertThat(stdout).contains("Generation No");
        assertThat(stdout).contains("Uptime (seconds)");
        assertThat(stdout).contains("Heap Memory (MB)");
        assertThat(stdout).contains("Data Center");
        assertThat(stdout).contains("Rack");
        assertThat(stdout).contains("Exceptions");
        assertThat(stdout).contains("Key Cache");
        assertThat(stdout).contains("Row Cache");
        assertThat(stdout).contains("Counter Cache");
        assertThat(stdout).contains("Percent Repaired");
        assertThat(stdout).contains("Token");
        assertThat(stdout).contains("Bootstrap state");
    }

    @Test
    public void testInfoWithMissingCacheMBean() throws Exception
    {
        javax.management.MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName chunkCacheEntries = new ObjectName("org.apache.cassandra.metrics:type=Cache,scope=ChunkCache,name=Entries");

        boolean wasRegistered = mbs.isRegistered(chunkCacheEntries);
        if (wasRegistered)
            mbs.unregisterMBean(chunkCacheEntries);
        try
        {
            ToolRunner.ToolResult tool = invokeNodetool("info");
            tool.assertOnCleanExit();
            String stdout = tool.getStdout();

            assertThat(stdout).doesNotContain("Chunk Cache");
            assertThat(stdout).contains("Percent Repaired");
            assertThat(stdout).contains("Bootstrap state");
        }
        finally
        {
            if (wasRegistered && !mbs.isRegistered(chunkCacheEntries))
            {
                CassandraMetricsRegistry.Metrics.registerMBean(
                    ChunkCache.instance.metrics.entries, chunkCacheEntries, MBeanWrapper.instance, false);
            }
        }
    }
}

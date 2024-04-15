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

package org.apache.cassandra.distributed.test.management;

import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.test.jmx.JMXGetterCheckTest.testAllValidGetters;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class JmxManagementDistributedTest extends TestBaseImpl
{
    @Test
    public void testOneNetworkInterfaceProvisioning() throws Exception
    {
        Set<String> allInstances = new HashSet<>();
        int iterations = 2; // Make sure the JMX infrastructure all cleans up properly by running this multiple times.

        try (Cluster cluster = Cluster.build(1)
                                      .withDynamicPortAllocation(true)
                                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                      .withConfig(c -> c.with(Feature.values())).start())
        {
            Set<String> instancesContacted = new HashSet<>();
            for (IInvokableInstance instance : cluster)
            {
                testInstance(instancesContacted, instance);
            }

            while (!Thread.currentThread().isInterrupted())
            {
                Thread.sleep(1000);
                System.out.println(">>>>> sleeping");
            }
            allInstances.addAll(instancesContacted);
            // Make sure we actually exercise the mbeans by testing a bunch of getters.
            // Without this it's possible for the test to pass as we don't touch any mBeans that we register.
            testAllValidGetters(cluster);
        }
        Assert.assertEquals("Each instance from each cluster should have been unique", iterations * 2, allInstances.size());
    }

    private void testInstance(Set<String> instancesContacted, IInvokableInstance instance)
    {
        IInstanceConfig config = instance.config();
        try (JMXConnector jmxc = JMXUtil.getJmxConnector(config))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            // instances get their default domain set to their IP address, so us it
            // to check that we are actually connecting to the correct instance
            String defaultDomain = mbsc.getDefaultDomain();
            instancesContacted.add(defaultDomain);
            Assert.assertThat(defaultDomain, startsWith(JMXUtil.getJmxHost(config) + ':' + config.jmxPort()));
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Could not connect to JMX", t);
        }
    }
}

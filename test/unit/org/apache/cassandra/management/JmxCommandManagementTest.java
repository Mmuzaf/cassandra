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

package org.apache.cassandra.management;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.DynamicMBean;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.management.api.CommandMBean;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.management.CassandraCommandRegistry.MBEAN_MANAGEMENT_COMMAND;

/**
 * Test compares JMX metrics to virtual table metrics values, basically all metric values must be equal and
 * have the same representation in both places.
 */
public class JmxCommandManagementTest
{
    private static EmbeddedCassandraService cassandra;
    protected static MBeanServerConnection jmxConnection;
    private static Cluster cluster;
    private Session session;

    @BeforeClass
    public static void setup() throws Exception
    {
        // Since we run EmbeddedCassandraServer, we need to manually associate JMX address; otherwise it won't start
        int jmxPort = CQLTester.getAutomaticallyAllocatedPort(InetAddress.getLoopbackAddress());
        CASSANDRA_JMX_LOCAL_PORT.setInt(jmxPort);
        cassandra = ServerTestUtils.startEmbeddedCassandraService();
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        createMBeanServerConnection();
    }

    @Before
    public void config() throws Throwable
    {
        session = cluster.connect();
        session.execute("select release_version from system.local");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    @Test
    public void testManagementCommandExists() throws Exception
    {
        List<ObjectName> mbeanByMetricGroup = jmxConnection.queryNames(null, null)
                                                                        .stream()
                                                                        .filter(on -> on.getDomain().equals(MBEAN_MANAGEMENT_COMMAND))
                                                                        .collect(Collectors.toList());

        for (ObjectName e : mbeanByMetricGroup)
        {
            System.out.println(">>>> MBean: " + e);
            System.out.println(">>>>> " + jmxConnection.getMBeanInfo(e));
        }

        DynamicMBean mbean = getMxBean("cleanup", DynamicMBean.class);
        List<String> params = new ArrayList<>();

//        Consumer<Field> fldCnsmr = fld -> params.add(toString(U.field(p.commandArg(), fld.getName())));
//        visitCommandParams(p.command().argClass(), fldCnsmr, fldCnsmr, (grp, flds) -> flds.forEach(fldCnsmr));

        String[] signature = new String[0];
        Arrays.fill(signature, String.class.getName());

        String out = (String)mbean.invoke(CommandMBean.INVOKE, params.toArray(), signature);

//        System.out.println(">>>> " + out)
//        Object result = mbean.invoke(CommandMBean.INVOKE, X.EMPTY_OBJECT_ARRAY, U.EMPTY_STRS);
    }

    public static <T> T getMxBean(String command, Class<T> clazz) throws IOException
    {
        ObjectName mbeanName = CommandUtils.makeMBeanName(command);
        if (!jmxConnection.isRegistered(mbeanName))
            throw new IllegalStateException("MBean not registered: " + mbeanName);
        return MBeanServerInvocationHandler.newProxyInstance(jmxConnection, mbeanName, clazz, false);
    }

    public static void createMBeanServerConnection() throws Exception
    {
        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", RMISocketFactory.getDefaultSocketFactory());
        JMXConnector jmxc = JMXConnectorFactory.connect(getJMXServiceURL(), env);
        jmxConnection =  jmxc.getMBeanServerConnection();
    }

    public static JMXServiceURL getJMXServiceURL() throws MalformedURLException
    {
        return new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",
                                               InetAddress.getLoopbackAddress().getHostAddress(),
                                               CASSANDRA_JMX_LOCAL_PORT.getInt()));
    }
}

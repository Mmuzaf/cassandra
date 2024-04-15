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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandMBean;
import org.apache.cassandra.service.EmbeddedCassandraService;
import picocli.CommandLine;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.management.CassandraCommandRegistry.MBEAN_MANAGEMENT_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

        String[] signature = new String[params.size()];
        Arrays.fill(signature, String.class.getName());

        String out = (String) mbean.invoke(CommandMBean.INVOKE, params.toArray(), signature);

//        System.out.println(">>>> " + out)
//        Object result = mbean.invoke(CommandMBean.INVOKE, X.EMPTY_OBJECT_ARRAY, U.EMPTY_STRS);
    }

    @Test
    public void testExecuteCleanupCommand() throws Exception
    {
        executeCommand("cleanup", "--jobs", "2", "custom_keyspace", "table1", "table2");
    }

    public void executeCommand(String... args)
    {
        // find command
        String command = args[0];
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
    }

    private static class CliCommandInvoker<T>
    {
        public T execute(Command command, String... args)
        {
            return null;
        }
    }

    // Utility methods
    // ----------------

    public static <T> T field(Object obj, String fieldName)
    {
        try
        {
            for (Class<?> cls = obj.getClass(); cls != Object.class; cls = cls.getSuperclass())
            {
                for (Field field : cls.getDeclaredFields())
                {
                    if (field.getName().equals(fieldName))
                    {
                        field.setAccessible(true);
                        return (T) field.get(obj);
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']', e);
        }

        throw new RuntimeException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']');
    }

    public static boolean isBoolean(Class<?> cls)
    {
        return cls == Boolean.class || cls == boolean.class;
    }

    private static String toString(Object val)
    {
        if (val == null || (isBoolean(val.getClass()) && !(boolean) val))
            return "";

        if (val.getClass().isArray())
        {
            int length = Array.getLength(val);

            if (length == 0)
                return "";

            StringBuffer sb = new StringBuffer();

            for (int i = 0; i < length; i++)
            {
                if (i != 0)
                    sb.append(',');

                sb.append(toString(Array.get(val, i)));
            }

            return sb.toString();
        }

        return Objects.toString(val);
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

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

import java.util.Arrays;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class CommandServiceTest extends CQLTester
{
    private static final String COMMAND_MBEAN_PATTERN = "org.apache.cassandra.management:type=Command,name=*";

    @Test
    public void testCommandServiceMBeanRegistered()
    {
        try
        {
            ObjectName serviceName = new ObjectName(CommandInvokerServiceMBean.MBEAN_NAME);
            assertThat(MBeanWrapper.instance.isRegistered(serviceName)).as("CommandInvokerService MBean should be registered after start()").isTrue();
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to check MBean registration", e);
        }
    }

    @Test
    public void testCommandMBeansRegistered()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        try
        {
            ObjectName pattern = new ObjectName(COMMAND_MBEAN_PATTERN);
            Set<ObjectName> commandMBeans = MBeanWrapper.instance.queryNames(pattern, null);

            assertThat(commandMBeans).as("At least one Command MBean should be registered").isNotEmpty();

            String[] commandNames = service.getCommandNames();
            assertThat(commandNames).as("Service should return command names").isNotEmpty();

            for (String commandName : commandNames)
            {
                String mbeanName = service.getCommandMBeanName(commandName);
                assertNotNull("MBean name should not be null for command: " + commandName, mbeanName);

                ObjectName objectName = new ObjectName(mbeanName);
                assertThat(MBeanWrapper.instance.isRegistered(objectName)).as("Command MBean should be registered for: " + commandName).isTrue();
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to verify Command MBeans", e);
        }
    }

    @Test
    public void testUnsupportedCommandsNotRegistered()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        String[] commandNames = service.getCommandNames();

        for (String unsupported : CassandraCommandRegistry.UNSUPPORTED_COMMANDS)
        {
            Command<?> cmd = service.getRegistry().command(unsupported);
            assertThat(cmd).as("Unsupported command '%s' should not be in the registry", unsupported).isNull();

            assertThat(Arrays.asList(commandNames))
                .as("Unsupported command '%s' should not appear in getCommandNames()", unsupported)
                .noneMatch(name -> name.equals(unsupported) || name.startsWith(unsupported + "."));
        }
    }

    @Test
    public void testCommandMBeanInvoke()
    {
        CommandInvokerService service = CommandInvokerService.instance;

        try
        {
            for (String testCommand : service.getCommandNames())
            {
                String mbeanName = service.getCommandMBeanName(testCommand);
                ObjectName commandObjectName = new ObjectName(mbeanName);
                assertThat(MBeanWrapper.instance.isRegistered(commandObjectName)).as("Command MBean should be registered").isTrue();

                MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
                String schema = (String) mbs.invoke(commandObjectName, "getJsonSchema", null, null);
                assertThat(schema).as("getJsonSchema() should return non-null JSON string").isNotNull().isNotEmpty();

                assertThat(schema.trim()).as("Schema should start with '{'").startsWith("{");
            }
        }
        catch (Exception e)
        {
            throw new AssertionError("Failed to test CommandMBeanAdapter invoke", e);
        }
    }
}

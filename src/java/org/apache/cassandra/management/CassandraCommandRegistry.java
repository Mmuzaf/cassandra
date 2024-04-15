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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandMBean;
import org.apache.cassandra.management.api.CommandsRegistry;
import org.apache.cassandra.utils.MBeanWrapper;
import picocli.CommandLine;

import static org.apache.cassandra.management.CommandUtils.makeMBeanName;

public class CassandraCommandRegistry implements CommandsRegistry
{
    private final Map<String, Command<?, ?>> commands = new HashMap<>();
    private final List<ObjectName> registeredMBeans = new ArrayList<>();
    public static final String MBEAN_MANAGEMENT_COMMAND = "org.apache.cassandra.management";
    public static final CassandraCommandRegistry instance = new CassandraCommandRegistry();

    private CassandraCommandRegistry()
    {
        register(CleanupCommand.class);
    }

    private <T> void register(Class<T> commandClazz)
    {
        Command<?, ?> cmd = new CassandraCommand<>(commandClazz);
        commands.put(cmd.name(), cmd);
    }

    public void start(MBeanWrapper mbeanWrapper)
    {
        for (Map.Entry<String, Command<?, ?>> command : commands.entrySet())
        {
            ObjectName mBean = makeMBeanName(command.getKey());
            registeredMBeans.add(mBean);
            mbeanWrapper.registerMBean(new CommandMBean(command.getValue()), mBean);
        }
    }

    public void stop(MBeanWrapper mbeanWrapper)
    {
        for (ObjectName mBean : registeredMBeans)
            mbeanWrapper.unregisterMBean(mBean);
    }

    @Override
    public Command command(String name)
    {
        return commands.get(name);
    }

    @Override
    public Iterator<Map.Entry<String, Command<?, ?>>> iterator()
    {
        return commands.entrySet().iterator();
    }

    private static class CassandraCommand<T, R> implements Command<T, R>
    {
        private final String name;
        private final String description;
        private final Class<T> commandUserObject;

        public CassandraCommand(Class<T> command)
        {
            CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.forAnnotatedObject(command);
            this.name = spec.name();
            this.description = spec.usageMessage().description().length == 0 ? "" : spec.usageMessage().description()[0];
            this.commandUserObject = command;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public String description()
        {
            return description;
        }

        @Override
        public Class<T> commandUserObject()
        {
            return commandUserObject;
        }
    }
}

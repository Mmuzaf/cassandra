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

package org.apache.cassandra.management.picocli;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandsProvider;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.NodetoolCommandsProvider;

import picocli.CommandLine.Model.CommandSpec;

import static org.apache.cassandra.management.ManagementUtils.loadService;

/** Registers the commands of every {@link NodetoolCommandsProvider} on the classpath. */
public class NodetoolPluginCommandsProvider implements CommandsProvider
{
    @Override
    public Collection<Command<?>> commands()
    {
        List<Command<?>> commands = new ArrayList<>();
        for (NodetoolCommandsProvider provider : loadService(NodetoolCommandsProvider.class))
        {
            for (Class<?> commandClass : provider.commands())
                commands.add(adapt(provider, commandClass));
        }
        return commands;
    }

    @SuppressWarnings("unchecked")
    private static Command<?> adapt(NodetoolCommandsProvider provider, Class<?> commandClass)
    {
        if (!CommandSpec.forAnnotatedObject(commandClass).subcommands().isEmpty())
            return new PicocliCommandRegistryAdapter((Class<? extends AbstractCommand>) commandClass);

        if (!AbstractCommand.class.isAssignableFrom(commandClass))
            throw new IllegalStateException(String.format("Command %s from %s must extend %s",
                                                          commandClass.getName(),
                                                          provider.getClass().getName(),
                                                          AbstractCommand.class.getName()));

        return PicocliCommandAdapter.forClass((Class<? extends AbstractCommand>) commandClass);
    }
}

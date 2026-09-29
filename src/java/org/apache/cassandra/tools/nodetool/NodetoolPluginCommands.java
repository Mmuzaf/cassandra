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

import picocli.CommandLine;

import static org.apache.cassandra.management.ManagementUtils.loadService;

/** Adds the commands of every {@link NodetoolCommandsProvider} on the classpath to the nodetool command tree. */
public final class NodetoolPluginCommands
{
    private NodetoolPluginCommands()
    {
    }

    public static void attach(CommandLine root, CommandLine.IFactory factory)
    {
        for (NodetoolCommandsProvider provider : loadService(NodetoolCommandsProvider.class))
        {
            for (Class<?> commandClass : provider.commands())
            {
                CommandLine command = new CommandLine(commandClass, factory);
                String name = command.getCommandName();
                if (root.getSubcommands().containsKey(name))
                    throw new IllegalStateException(String.format("Command '%s' (%s) from %s clashes with an existing nodetool command",
                                                                  name, commandClass.getName(), provider.getClass().getName()));
                root.addSubcommand(name, command);
            }
        }
    }
}

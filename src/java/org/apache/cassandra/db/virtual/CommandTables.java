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

package org.apache.cassandra.db.virtual;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.cassandra.db.virtual.model.CommandArgumentRow;
import org.apache.cassandra.db.virtual.model.CommandRow;
import org.apache.cassandra.db.virtual.walker.CommandArgumentRowWalker;
import org.apache.cassandra.db.virtual.walker.CommandRowWalker;
import org.apache.cassandra.management.CommandInvokerService;
import org.apache.cassandra.management.CommandInvokerService.CommandEntry;

/**
 * Catalog of the management commands invocable with {@code INVOKE COMMAND}, and of the arguments each of
 * them accepts. Lets clients discover commands over CQL; cqlsh completes {@code INVOKE COMMAND} from it.
 */
public class CommandTables
{
    private CommandTables()
    {
    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        // Both iterables re-read the registry on every iteration. SystemViewsKeyspace is initialized long
        // before CommandInvokerService.start(), so a snapshot taken here would always be empty; deferring
        // the CommandInvokerService.instance access also keeps it out of this class' initialization.
        Iterable<CommandEntry> commands = () -> CommandInvokerService.instance.listCommands().iterator();
        Iterable<CommandArgumentRow> arguments =
            () -> StreamSupport.stream(commands.spliterator(), false)
                               .flatMap(CommandArgumentRow::forCommand)
                               .iterator();

        return Arrays.asList(
            CollectionVirtualTableAdapter.create(keyspace,
                                                 "commands",
                                                 "Management commands invocable with INVOKE COMMAND",
                                                 new CommandRowWalker(),
                                                 commands,
                                                 CommandRow::new),
            CollectionVirtualTableAdapter.create(keyspace,
                                                 "command_arguments",
                                                 "Arguments accepted by the management commands",
                                                 new CommandArgumentRowWalker(),
                                                 arguments,
                                                 Function.identity()));
    }
}

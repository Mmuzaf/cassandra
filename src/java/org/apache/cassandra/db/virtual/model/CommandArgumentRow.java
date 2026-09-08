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

package org.apache.cassandra.db.virtual.model;

import java.util.stream.Stream;

import com.google.common.base.Strings;

import org.apache.cassandra.management.CommandInvokerService.CommandEntry;
import org.apache.cassandra.management.api.ArgumentMetadata;
import org.apache.cassandra.management.api.CommandMetadata;

import static org.apache.cassandra.management.ManagementUtils.normalizeOptionName;
import static org.apache.cassandra.management.api.ParameterMetadata.COMMAND_POSITIONAL_PARAM_PREFIX;

/**
 * A single argument of a management command, for a
 * {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 * <p>
 * The {@code argument} name is the key an operator writes in the {@code WITH} clause of
 * {@code INVOKE COMMAND}: the normalized option label for options, {@code param<index>} for positional
 * parameters. Both are produced the same way by
 * {@link org.apache.cassandra.tools.nodetool.strategy.CqlCommandExecutionStrategy} and resolved by
 * {@link org.apache.cassandra.management.CommandExecutionArgsSerde}.
 */
public class CommandArgumentRow
{
    private static final String KIND_OPTION = "option";
    private static final String KIND_PARAMETER = "parameter";

    private final String command;
    private final String argument;
    private final String kind;
    private final ArgumentMetadata argumentMetadata;

    private CommandArgumentRow(String command, String argument, String kind, ArgumentMetadata argumentMetadata)
    {
        this.command = command;
        this.argument = argument;
        this.kind = kind;
        this.argumentMetadata = argumentMetadata;
    }

    /** Every option and positional parameter accepted by the given command. */
    public static Stream<CommandArgumentRow> forCommand(CommandEntry entry)
    {
        CommandMetadata metadata = entry.command().metadata();

        Stream<CommandArgumentRow> options =
            metadata.options()
                    .stream()
                    .map(option -> new CommandArgumentRow(entry.fullName(),
                                                          normalizeOptionName(option.paramLabel()),
                                                          KIND_OPTION,
                                                          option));

        Stream<CommandArgumentRow> parameters =
            metadata.parameters()
                    .stream()
                    .map(param -> new CommandArgumentRow(entry.fullName(),
                                                         COMMAND_POSITIONAL_PARAM_PREFIX + param.index(),
                                                         KIND_PARAMETER,
                                                         param));

        return Stream.concat(options, parameters);
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String command()
    {
        return command;
    }

    @Column(type = Column.Type.CLUSTERING)
    public String argument()
    {
        return argument;
    }

    /** Accepted number of values: {@code 0} for flags, {@code 1}, {@code 0..1}, {@code 0..*}. */
    @Column
    public String arity()
    {
        return emptyToNull(argumentMetadata.arity());
    }

    @Column
    public String defaultValue()
    {
        return emptyToNull(argumentMetadata.defaultValue());
    }

    @Column
    public String description()
    {
        return emptyToNull(argumentMetadata.description());
    }

    /** Either {@code option} or {@code parameter}. */
    @Column
    public String kind()
    {
        return kind;
    }

    @Column
    public boolean required()
    {
        return argumentMetadata.required();
    }

    /** Simple name of the Java type the value is converted to, e.g. {@code String}, {@code List}. */
    @Column
    public String type()
    {
        Class<?> type = argumentMetadata.type();
        return type == null ? null : type.getSimpleName();
    }

    private static String emptyToNull(String value)
    {
        return Strings.isNullOrEmpty(value) ? null : value;
    }
}

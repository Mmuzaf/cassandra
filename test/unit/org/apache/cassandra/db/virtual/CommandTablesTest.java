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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@code system_views.commands} and {@code system_views.command_arguments} catalog that cqlsh
 * completes {@code INVOKE COMMAND} from.
 */
public class CommandTablesTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @Before
    public void config()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, CommandTables.getAll(KS_NAME)));
        disablePreparedReuseForTest();
    }

    @Test
    public void testCommandsListsLeafCommands() throws Throwable
    {
        List<String> commands = column(execute("SELECT command FROM " + KS_NAME + ".commands"), "command");

        assertThat(commands).contains("version", "profile.start", "profile.status");
        // Parent registries are traversed, not listed: only leaves are invocable.
        assertThat(commands).doesNotContain("profile");
    }

    @Test
    public void testCommandsExposeDescription() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT description FROM " + KS_NAME + ".commands WHERE command = 'profile.start'");

        assertThat(rows).hasSize(1);
        assertThat(rows.one().getString("description")).isEqualTo("Run Async-Profiler on a Cassandra process");
    }

    @Test
    public void testCommandArgumentsOfProfileStart() throws Throwable
    {
        List<String> arguments = column(execute("SELECT argument FROM " + KS_NAME + ".command_arguments " +
                                                "WHERE command = 'profile.start'"), "argument");

        assertThat(arguments).contains("event", "duration", "filename");
    }

    @Test
    public void testCommandArgumentMetadata() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT * FROM " + KS_NAME + ".command_arguments " +
                                        "WHERE command = 'profile.start' AND argument = 'duration'");

        assertThat(rows).hasSize(1);
        UntypedResultSet.Row row = rows.one();
        assertThat(row.getString("kind")).isEqualTo("option");
        assertThat(row.getString("type")).isEqualTo("String");
        assertThat(row.getString("arity")).isEqualTo("1");
        assertThat(row.getBoolean("required")).isFalse();
        assertThat(row.getString("description")).contains("Duration of profiling");
    }

    @Test
    public void testBooleanFlagArgument() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT * FROM " + KS_NAME + ".command_arguments " +
                                        "WHERE command = 'gcstats' AND argument = 'human_readable'");

        assertThat(rows).hasSize(1);
        UntypedResultSet.Row row = rows.one();
        assertThat(row.getString("type")).isEqualTo("boolean");
        assertThat(row.getString("arity")).isEqualTo("0");
        assertThat(row.getString("default_value")).isEqualTo("false");
    }

    @Test
    public void testPositionalParametersUseParamIndexNames() throws Throwable
    {
        // 'profile.fetch <remoteFile> <localFile>' - positional params are addressed as param0/param1,
        // the same names CqlCommandExecutionStrategy writes into the WITH clause.
        List<String> arguments = column(execute("SELECT argument FROM " + KS_NAME + ".command_arguments " +
                                                "WHERE command = 'profile.fetch'"), "argument");

        assertThat(arguments).contains("param0", "param1");

        UntypedResultSet rows = execute("SELECT kind, required FROM " + KS_NAME + ".command_arguments " +
                                        "WHERE command = 'profile.fetch' AND argument = 'param0'");
        assertThat(rows.one().getString("kind")).isEqualTo("parameter");
        assertThat(rows.one().getBoolean("required")).isTrue();
    }

    @Test
    public void testCommandWithoutArgumentsHasNoRows() throws Throwable
    {
        assertThat(execute("SELECT argument FROM " + KS_NAME + ".command_arguments WHERE command = 'profile.status'"))
            .isEmpty();
    }

    private static List<String> column(UntypedResultSet rows, String name)
    {
        List<String> values = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            values.add(row.getString(name));
        return values;
    }
}

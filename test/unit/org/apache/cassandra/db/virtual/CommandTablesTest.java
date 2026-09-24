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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@code system_views.commands} and {@code system_views.command_arguments} catalog that cqlsh
 * completes {@code INVOKE COMMAND} from, and the {@code system_views.command_executions} history.
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

    @Test
    public void testSynchronousExecutionRecorded() throws Throwable
    {
        UUID executionId = executeCommand("INVOKE COMMAND version;");
        UntypedResultSet.Row row = execution(executionId);

        assertThat(row.getString("command")).isEqualTo("version");
        assertThat(row.getString("status")).isEqualTo("COMPLETED");
        assertThat(row.has("completed_at")).isTrue();
        assertThat(row.has("output")).as("Synchronous output goes back to the caller, not the table").isFalse();
    }

    @Test
    public void testProgressibleExecutionCompletesAsynchronously() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable(keyspace, "CREATE TABLE %s (k text PRIMARY KEY, v int)");

        UUID executionId = executeCommand(String.format("INVOKE COMMAND cleanup WITH \"keyspace\" = '%s';", keyspace));
        assertThat(execution(executionId).getString("command")).isEqualTo("cleanup");

        Util.spinAssertEquals(null, "COMPLETED", () -> execution(executionId).getString("status"), 1, TimeUnit.MINUTES);
        assertThat(execution(executionId).has("completed_at")).isTrue();
    }

    @Test
    public void testFailureCategoryRecorded() throws Throwable
    {
        UUID executionId = executeCommand("INVOKE COMMAND cleanup WITH \"keyspace\" = 'nonexistent_keyspace';");

        Util.spinAssertEquals(null, "FAILED", () -> execution(executionId).getString("status"), 1, TimeUnit.MINUTES);
        UntypedResultSet.Row row = execution(executionId);
        assertThat(row.getString("error_type")).isEqualTo("VALIDATION");
        assertThat(row.getString("error")).contains("nonexistent_keyspace");
    }

    /** Runs an INVOKE COMMAND statement and returns its execution id. */
    private UUID executeCommand(String statement) throws Throwable
    {
        return execute(statement).one().getUUID("execution_id");
    }

    private UntypedResultSet.Row execution(UUID executionId)
    {
        try
        {
            UntypedResultSet rows = execute("SELECT * FROM " + KS_NAME + ".command_executions WHERE execution_id = ?", executionId);
            assertThat(rows).as("Execution %s should be in the history", executionId).hasSize(1);
            return rows.one();
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private static List<String> column(UntypedResultSet rows, String name)
    {
        List<String> values = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            values.add(row.getString(name));
        return values;
    }
}

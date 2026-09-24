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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.management.CommandInvokerService.ExecutionHistory;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.ExecutionStatus;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.management.api.ProgressibleCommand;
import org.apache.cassandra.management.picocli.PicocliCommandAdapter;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_COMMAND;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_COMPLETED_AT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR_TYPE;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_EXECUTION_ID;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_OUTPUT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_STARTED_AT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_STATUS;
import static org.apache.cassandra.management.ManagementUtils.findRegistryCommand;
import static org.apache.cassandra.utils.JsonUtils.fromJsonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    public void testProgressibleCommandsRegistered()
    {
        CommandInvokerService service = CommandInvokerService.instance;

        for (String name : List.of("repair", "cleanup", "compressiondictionary.train", "consensus_admin.finish-migration"))
            assertThat(registryCommand(service, name))
                .as("'%s' should be registered as progressible", name)
                .isInstanceOf(ProgressibleCommand.class);

        for (String name : List.of("info", "version"))
            assertThat(registryCommand(service, name))
                .as("'%s' should stay synchronous", name)
                .isNotInstanceOf(ProgressibleCommand.class);

        for (CommandInvokerService.CommandEntry entry : service.listCommands())
        {
            if (!(entry.command() instanceof PicocliCommandAdapter))
                continue;

            PicocliCommandAdapter adapter = (PicocliCommandAdapter) entry.command();
            assertThat(adapter instanceof ProgressibleCommand)
                .as("Adapter and bean must agree on the progressible marker for '%s'", entry.fullName())
                .isEqualTo(ProgressibleCommand.class.isAssignableFrom(adapter.commandClass()));
        }
    }

    @Test
    public void testStartCommandReturnsBeforeCompletion() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        String name = registerLatchCommand(service, "latchstart", started, release);

        try
        {
            UUID executionId = service.startCommand(name, CommandServiceTest::emptyArgs).getExecutionId();

            assertThat(started.await(1, TimeUnit.MINUTES)).as("Command should have started").isTrue();
            ExecutionHistory record = findInHistory(service, executionId);
            assertThat(record).isNotNull();
            assertThat(record.status()).isEqualTo(ExecutionStatus.RUNNING);
            assertThat(record.endTime()).as("A running command has no completion time").isNull();
            Util.spinAssertEquals(null, "working", () -> record.output().trim(), 1, TimeUnit.MINUTES);

            Map<String, String> polled = fromJsonMap(service.getExecution(executionId.toString()));
            assertThat(polled.get(FIELD_STATUS)).isEqualTo(ExecutionStatus.RUNNING.name());
            assertThat(polled.get(FIELD_COMPLETED_AT)).isNull();
            assertThat(polled.get(FIELD_OUTPUT)).contains("working");

            release.countDown();
            Util.spinAssertEquals(null, ExecutionStatus.COMPLETED, record::status, 1, TimeUnit.MINUTES);
            assertThat(record.output()).contains("done");
            assertThat(record.endTime()).isNotNull();
        }
        finally
        {
            release.countDown();
            unregisterCommand(service, name);
        }
    }

    @Test
    public void testRunningExecutionsAreNotEvicted() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        String name = registerLatchCommand(service, "latchevict", started, release);

        try
        {
            UUID executionId = service.startCommand(name, CommandServiceTest::emptyArgs).getExecutionId();
            assertThat(started.await(1, TimeUnit.MINUTES)).isTrue();

            for (int i = 0; i < 120; i++)
                service.invokeCommand("version", CommandServiceTest::emptyArgs);

            assertThat(findInHistory(service, executionId))
                .as("A running execution must survive eviction while shorter ones churn")
                .isNotNull();
        }
        finally
        {
            release.countDown();
            unregisterCommand(service, name);
        }
    }

    @Test
    public void testGetExecutionJson() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        UUID executionId = service.invokeCommand("version", CommandServiceTest::emptyArgs).getExecutionId();

        Map<String, String> execution = fromJsonMap(service.getExecution(executionId.toString()));
        assertThat(execution).containsOnlyKeys(FIELD_EXECUTION_ID, FIELD_COMMAND, FIELD_STATUS, FIELD_STARTED_AT,
                                               FIELD_COMPLETED_AT, FIELD_ERROR, FIELD_ERROR_TYPE, FIELD_OUTPUT);
        assertThat(execution.get(FIELD_EXECUTION_ID)).isEqualTo(executionId.toString());
        assertThat(execution.get(FIELD_COMMAND)).isEqualTo("version");
        assertThat(execution.get(FIELD_STATUS)).isEqualTo(ExecutionStatus.COMPLETED.name());
        assertThat(execution.get(FIELD_ERROR)).isNull();
        assertThat(execution.get(FIELD_OUTPUT))
            .as("A synchronous execution returned its output to the caller and retains none")
            .isNull();
        assertThat(Long.parseLong(execution.get(FIELD_COMPLETED_AT)))
            .isGreaterThanOrEqualTo(Long.parseLong(execution.get(FIELD_STARTED_AT)));
    }

    @Test
    public void testGetExecutionsFilteredAndOrdered() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        UUID first = service.invokeCommand("version", CommandServiceTest::emptyArgs).getExecutionId();
        UUID second = service.invokeCommand("version", CommandServiceTest::emptyArgs).getExecutionId();

        List<Map<String, String>> executions = executions(service.getExecutions("version"));
        assertThat(executions).allSatisfy(e -> assertThat(e.get(FIELD_COMMAND)).isEqualTo("version"));
        assertThat(executions).allSatisfy(e -> assertThat(e).doesNotContainKey(FIELD_OUTPUT));

        List<String> ids = executions.stream().map(e -> e.get(FIELD_EXECUTION_ID)).collect(Collectors.toList());
        assertThat(ids.indexOf(second.toString()))
            .as("Newest execution comes first")
            .isLessThan(ids.indexOf(first.toString()));

        assertThat(executions(service.getExecutions("nosuchcommand"))).isEmpty();
        assertThat(executions(service.getExecutions(null)))
            .as("A null filter returns every retained execution")
            .hasSizeGreaterThanOrEqualTo(executions.size());
    }

    @Test
    public void testGetExecutionUnknownId()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        assertThat(service.getExecution(UUID.randomUUID().toString())).isNull();
        assertThat(service.getExecution("not-a-uuid")).isNull();
        assertThat(service.getExecution(null)).isNull();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, String>> executions(String json)
    {
        return (List<Map<String, String>>) (List<?>) JsonUtils.fromJsonList(json);
    }

    /** Registers a command that reports progress, then blocks until {@code release} is counted down. */
    private static String registerLatchCommand(CommandInvokerService service,
                                               String name,
                                               CountDownLatch started,
                                               CountDownLatch release)
    {
        CassandraCommandRegistry registry = (CassandraCommandRegistry) service.getRegistry();
        registry.register(new LatchCommand(name, started, release));
        return name;
    }

    /** Other tests walk every registered name and expect an MBean, so a test command must not outlive its test. */
    private static void unregisterCommand(CommandInvokerService service, String name)
    {
        ((CassandraCommandRegistry) service.getRegistry()).unregister(name);
    }

    private static Command<?> registryCommand(CommandInvokerService service, String fullName)
    {
        Command<?> command = findRegistryCommand(fullName, service.getRegistry());
        assertThat(command).as("'%s' should be registered", fullName).isNotNull();
        return command;
    }

    private static class LatchCommand implements Command<Void>, ProgressibleCommand
    {
        private final String name;
        private final CountDownLatch started;
        private final CountDownLatch release;

        LatchCommand(String name, CountDownLatch started, CountDownLatch release)
        {
            this.name = name;
            this.started = started;
            this.release = release;
        }

        @Override
        public CommandMetadata metadata()
        {
            return new CommandMetadata()
            {
                public String name() { return name; }
                public String description() { return "Blocks until released, for tests"; }
                public List<OptionMetadata> options() { return Collections.emptyList(); }
                public List<ParameterMetadata> parameters() { return Collections.emptyList(); }
                public List<CommandMetadata> subcommands() { return Collections.emptyList(); }
            };
        }

        @Override
        public Void execute(CommandExecutionArgs arguments, CommandExecutionContext context)
        {
            Output output = context.service(Output.class);
            output.out.println("working");
            started.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
            output.out.println("done");
            return null;
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

    /**
     * A successful execution is recorded in the bounded execution history as a success.
     */
    @Test
    public void testSuccessfulExecutionRecordedInHistory() throws Exception
    {
        CommandInvokerService service = CommandInvokerService.instance;
        UUID executionId = service.invokeCommand("version", CommandServiceTest::emptyArgs).getExecutionId();

        CommandInvokerService.ExecutionHistory record = findInHistory(service, executionId);
        assertThat(record).as("Successful 'version' execution should be recorded in history").isNotNull();
        assertThat(record.isSuccess()).as("Record should be marked successful").isTrue();
        assertThat(record.commandName()).isEqualTo("version");
    }

    @Test
    public void testValidationFailureRecordedInHistory()
    {
        CommandInvokerService service = CommandInvokerService.instance;

        int before = service.executionHistory().size();
        assertThatThrownBy(() -> service.invokeCommand("getauthcacheconfig", CommandServiceTest::emptyArgs))
            .isInstanceOf(CommandValidationException.class);

        List<CommandInvokerService.ExecutionHistory> history = service.executionHistory();
        assertThat(history.size()).as("Failed execution should still be recorded").isGreaterThan(before);

        CommandInvokerService.ExecutionHistory last = history.get(history.size() - 1);
        assertThat(last.commandName()).isEqualTo("getauthcacheconfig");
        assertThat(last.isSuccess()).as("Validation failure should be recorded as not successful").isFalse();
        assertThat(last.error()).as("Failure record should retain the error").isNotNull();
    }

    @Test
    public void testMalformedJsonMappedToValidationException()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        CommandMetadata metadata = service.getRegistry().command("version").metadata();

        assertThatThrownBy(() -> service.invokeCommand("version", () -> CommandExecutionArgsSerde.fromJson("{not valid json", metadata)))
            .isInstanceOf(CommandValidationException.class)
            .hasMessageContaining("Bad usage");
    }

    @Test
    public void testMalformedJsonReportedAsValidationErrorViaMBean()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        Command<?> command = service.getRegistry().command("version");
        CommandMBeanAdapter adapter = new CommandMBeanAdapter("version", command, service::invokeCommand);

        assertThatThrownBy(() -> adapter.invoke(CommandMBeanAdapter.INVOKE_METHOD,
                                                new Object[]{ "{not valid json" },
                                                new String[]{ String.class.getName() }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Error decoding JSON string");
    }

    @Test
    public void testCommandNotFoundReportedAsIllegalArgumentViaMBean()
    {
        CommandInvokerService service = CommandInvokerService.instance;
        Command<?> command = service.getRegistry().command("version");
        CommandMBeanAdapter adapter = new CommandMBeanAdapter("nonexistentcommand", command, service::invokeCommand);

        assertThatThrownBy(() -> adapter.invoke(CommandMBeanAdapter.INVOKE_METHOD,
                                                new Object[]{ "{}" },
                                                new String[]{ String.class.getName() }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Command not found");
    }

    private static CommandExecutionArgs emptyArgs()
    {
        return new SimpleCommandExecutionArgs(Collections.emptyMap(), Collections.emptyMap());
    }

    private static CommandInvokerService.ExecutionHistory findInHistory(CommandInvokerService service, UUID executionId)
    {
        return service.executionHistory().stream()
                      .filter(r -> executionId.equals(r.executionId()))
                      .reduce((first, second) -> second) // last match
                      .orElse(null);
    }
}

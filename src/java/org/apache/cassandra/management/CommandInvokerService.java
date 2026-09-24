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


import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.CommandRegistry;
import org.apache.cassandra.management.api.ExecutionErrorType;
import org.apache.cassandra.management.api.ExecutionStatus;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.management.api.ProgressibleCommand;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.management.ManagementUtils.countCommands;
import static org.apache.cassandra.management.ManagementUtils.findRegistryCommand;
import static org.apache.cassandra.management.ManagementUtils.fullCommandName;

/**
 * Owns the command registry and executes commands against it.
 * <p>
 * Like StorageService and SnapshotManager, this is a daemon-lifetime singleton started from
 * {@link org.apache.cassandra.service.CassandraDaemon}. It builds the registry at startup, registers one
 * {@link CommandMBeanAdapter} per command, runs commands on behalf of the JMX and CQL transports, and
 * unregisters everything on shutdown.
 */
public class CommandInvokerService implements CommandInvokerServiceMBean
{
    private static final String MBEAN_DOMAIN = "org.apache.cassandra.management";
    private static final String MBEAN_TYPE_COMMAND = "Command";
    private static final int MAX_EXECUTION_HISTORY = 100;
    private static final Logger logger = LoggerFactory.getLogger(CommandInvokerService.class);

    public static final CommandInvokerService instance = new CommandInvokerService();

    private final BoundedExecutionHistory executionHistory = new BoundedExecutionHistory(MAX_EXECUTION_HISTORY);
    private final Map<String, ObjectName> commandMBeanNames = new ConcurrentHashMap<>();
    private final MBeanAccessor accessor = new InternalNodeMBeanAccessor();
    private final CommandRegistry registry;

    private volatile boolean started = false;
    private volatile ExecutorPlus asyncExecutor;

    private CommandInvokerService()
    {
        this.registry = new CassandraCommandRegistry();
    }

    public synchronized void start()
    {
        if (started)
            return;

        logger.info("Starting command service");

        asyncExecutor = executorFactory().withJmxInternal().pooled("ManagementCommands", Integer.MAX_VALUE);
        registerCommandMBeansRecursively(registry, "");
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        started = true;
        logger.info("Command service started with '{}' commands", countCommands(registry));
    }

    public static void shutdown()
    {
        instance.stop();
    }

    public synchronized void stop()
    {
        if (!started)
            return;

        logger.info("Stopping command service");

        unregisterCommandMBeans();

        try
        {
            MBeanWrapper.instance.unregisterMBean(MBEAN_NAME);
        }
        catch (Exception e)
        {
            logger.warn("Failed to unregister CommandInvokerService MBean", e);
        }

        if (asyncExecutor != null)
        {
            asyncExecutor.shutdown();
            asyncExecutor = null;
        }

        started = false;
        logger.info("Command service stopped");
    }

    public CommandRegistry getRegistry()
    {
        return registry;
    }

    /**
     * The entry point every server-side transport uses. A {@link ProgressibleCommand} starts on the
     * management executor and the caller gets the execution id back at once; anything else runs to
     * completion on the calling thread.
     */
    public CommandResult execute(String fullCommandName, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        Command<?> command = lookup(fullCommandName);
        return command instanceof ProgressibleCommand ? startCommand(fullCommandName, command, argumentsSupplier)
                                                      : invokeCommand(fullCommandName, command, argumentsSupplier);
    }

    /**
     * @param fullCommandName Full command name, including the command delimiter for subcommands.
     * @param argumentsSupplier Supplier of command execution arguments.
     *                          Defers argument construction until every validation check has passed.
     * @return captured output of the command execution.
     */
    public CommandResult invokeCommand(String fullCommandName, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        return invokeCommand(fullCommandName, lookup(fullCommandName), argumentsSupplier);
    }

    /**
     * Execute a command by name with the given arguments on the calling thread.
     *
     * @param command to execute.
     * @param argumentsSupplier supplier of command execution arguments.
     * @return captured output of the command execution.
     */
    private CommandResult invokeCommand(String fullCommandName, Command<?> command, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        Prepared prepared = prepare(fullCommandName, command, argumentsSupplier, false);
        run(prepared, command);

        ExecutionHistory record = prepared.record;
        return new CommandResult(record.executionId,
                                 prepared.captured.getCapturedOutput(),
                                 record.startTime,
                                 record.endTime - record.startTime);
    }

    /**
     * Start a {@link org.apache.cassandra.management.api.ProgressibleCommand} and return as soon as it is
     * running. Progress and outcome are in {@code system_views.command_executions}.
     * <p>
     * Argument validation still happens on the calling thread, so bad arguments fail the same way they do
     * for {@link #invokeCommand(String, Supplier)}.
     */
    public CommandResult startCommand(String fullCommandName, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        return startCommand(fullCommandName, lookup(fullCommandName), argumentsSupplier);
    }

    private CommandResult startCommand(String fullCommandName, Command<?> command, Supplier<CommandExecutionArgs> argumentsSupplier)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        ExecutorPlus executor = asyncExecutor;
        if (executor == null)
            throw new IllegalStateException("CommandInvokerService is not started");

        Prepared prepared = prepare(fullCommandName, command, argumentsSupplier, true);

        try
        {
            executor.execute(() -> {
                try
                {
                    run(prepared, command);
                }
                catch (Throwable ignore)
                {
                }
            });
        }
        catch (RuntimeException e)
        {
            CommandExecutionException mapped = new CommandExecutionException(format("Command '%s' (execution ID: %s) could not be started",
                                                                                    fullCommandName, prepared.record.executionId),
                                                                             e,
                                                                             prepared.record.executionId);
            prepared.record.failed(Clock.Global.currentTimeMillis(), mapped);
            throw mapped;
        }

        return new CommandResult(prepared.record.executionId,
                                 startedHint(fullCommandName, prepared.record.executionId),
                                 prepared.record.startTime,
                                 0);
    }

    public static String startedHint(String fullCommandName, UUID executionId)
    {
        return format("Command '%s' started with execution id %s. Follow it with: " +
                      "SELECT * FROM system_views.command_executions WHERE execution_id = %s; " +
                      "Over JMX, run: nodetool commandexecutions %s",
                      fullCommandName, executionId, executionId, executionId);
    }

    private Command<?> lookup(String fullCommandName)
    {
        if (!started)
            throw new IllegalStateException("CommandInvokerService is not started");

        Command<?> command = findRegistryCommand(fullCommandName, registry);
        if (command == null)
            throw new IllegalArgumentException("Command not found: " + fullCommandName);

        return command;
    }

    /**
     * Mint the execution id, publish the history record, then authorize and validate. The record goes into
     * history before anything can fail, so rejected invocations stay visible to operators.
     */
    private Prepared prepare(String fullCommandName,
                             Command<?> command,
                             Supplier<CommandExecutionArgs> argumentsSupplier,
                             boolean retainOutput)
        throws CommandExecutionException, CommandValidationException, CommandAuthorizationException
    {
        UUID executionId = UUID.randomUUID();
        CapturingOutput captured = new CapturingOutput();
        ExecutionHistory record = new ExecutionHistory(executionId,
                                                       fullCommandName,
                                                       Clock.Global.currentTimeMillis(),
                                                       retainOutput ? captured : null);
        CommandExecutionContext context = new ServerCommandExecutionContext(new NodeProbe(accessor, captured.createOutput()));
        executionHistory.add(record);

        try
        {
            // TODO: CASSANDRA-XXXXX. Restrict command execution to environments without authentication
            //  This is a temporary limitation until full authentication support is implemented.
            if (DatabaseDescriptor.getAuthenticator().requireAuthentication())
            {
                throw new CommandAuthorizationException(format("Command execution '%s' via management port is currently " +
                                                               "only supported when authentication is disabled " +
                                                               "(AllowAllAuthenticator). Full authentication and authorization " +
                                                               "support will be added in a future release.", fullCommandName));
            }

            CommandExecutionArgs arguments = argumentsSupplier.get();
            validateArguments(arguments, command.metadata());
            return new Prepared(record, captured, arguments, context);
        }
        catch (CommandAuthorizationException e)
        {
            record.failed(Clock.Global.currentTimeMillis(), e);
            logger.error("Command '{}' (execution ID: {}) authorization failed", fullCommandName, executionId, e);
            throw e;
        }
        catch (IllegalStateException | IllegalArgumentException | MarshalException e)
        {
            String msg = format("Bad usage for command '%s' (execution ID: %s)", fullCommandName, executionId);
            CommandValidationException mapped = new CommandValidationException(msg, e);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error(msg, e);
            throw mapped;
        }
        catch (RuntimeException e)
        {
            String msg = format("Command '%s' (execution ID: %s) execution failed", fullCommandName, executionId);
            CommandExecutionException mapped = new CommandExecutionException(msg, e, executionId);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error(msg, e);
            throw mapped;
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            CommandExecutionException mapped = new CommandExecutionException(format("Unexpected error while preparing '%s': %s",
                                                                                    fullCommandName, e.getMessage()),
                                                                             e,
                                                                             executionId);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error("Command '{}' (execution ID: {}) unexpected error", fullCommandName, executionId, e);
            throw mapped;
        }
    }

    /** Run a prepared command, recording its outcome on the history record. */
    private void run(Prepared prepared, Command<?> command)
        throws CommandExecutionException, CommandValidationException
    {
        ExecutionHistory record = prepared.record;
        String fullCommandName = record.commandName;
        UUID executionId = record.executionId;

        try
        {
            logger.info("Executing command '{}' with execution ID: {}", fullCommandName, executionId);

            // Currently, for picocli-based commands in C*, which have no structured result,
            // the output is written to the Output in the context.
            Object ignore = command.execute(prepared.arguments, prepared.context);

            record.completed(Clock.Global.currentTimeMillis());
            logger.info("Command '{}' (execution ID: {}) completed successfully", fullCommandName, executionId);
        }
        catch (IllegalStateException | IllegalArgumentException | MarshalException e)
        {
            String msg = format("Bad usage for command '%s' (execution ID: %s)", fullCommandName, executionId);
            CommandValidationException mapped = new CommandValidationException(msg, e);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error(msg, e);
            throw mapped;
        }
        catch (Exception e)
        {
            String msg = format("Command '%s' (execution ID: %s) execution failed", fullCommandName, executionId);
            CommandExecutionException mapped = new CommandExecutionException(msg, e, executionId);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error(msg, e);
            throw mapped;
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            CommandExecutionException mapped = new CommandExecutionException(format("Unexpected error while executing '%s': %s",
                                                                                    fullCommandName, e.getMessage()),
                                                                             e,
                                                                             executionId);
            record.failed(Clock.Global.currentTimeMillis(), mapped);
            logger.error("Command '{}' (execution ID: {}) unexpected error", fullCommandName, executionId, e);
            throw mapped;
        }
    }

    @Override
    public String[] getCommandNames()
    {
        List<String> commandNames = new ArrayList<>();
        for (CommandEntry entry : listCommands())
            commandNames.add(entry.fullName());
        return commandNames.toArray(new String[0]);
    }

    /**
     * Leaf commands of the registry, keyed by their full (dot-delimited) name. Parent registries are
     * traversed but not returned, since only leaves are executable.
     * <p>
     * The registry is walked on every {@link Iterable#iterator()} call rather than snapshotted, so callers
     * created before {@link #start()} (e.g. the virtual tables of
     * {@link org.apache.cassandra.db.virtual.SystemViewsKeyspace}) still observe the commands.
     */
    public Iterable<CommandEntry> listCommands()
    {
        return () -> {
            List<CommandEntry> commands = new ArrayList<>();
            collectCommandsRecursively(registry, "", commands);
            return commands.iterator();
        };
    }

    public List<ExecutionHistory> executionHistory()
    {
        return executionHistory.snapshot();
    }

    @Override
    public String getExecution(String executionId)
    {
        UUID id;
        try
        {
            id = UUID.fromString(executionId);
        }
        catch (IllegalArgumentException | NullPointerException e)
        {
            return null;
        }

        for (ExecutionHistory record : executionHistory.snapshot())
        {
            if (record.executionId().equals(id))
                return JsonUtils.writeAsJsonString(toMap(record, true));
        }
        return null;
    }

    @Override
    public String getExecutions(String commandName)
    {
        boolean all = commandName == null || commandName.isEmpty();
        List<ExecutionHistory> records = executionHistory.snapshot();
        List<Map<String, String>> result = new ArrayList<>();

        for (int i = records.size() - 1; i >= 0; i--)
        {
            ExecutionHistory record = records.get(i);
            if (all || record.commandName().equals(commandName))
                result.add(toMap(record, false));
        }
        return JsonUtils.writeAsJsonString(result);
    }

    /**
     * The single place where an execution record becomes named fields, shared by both MBean operations so
     * they cannot drift from {@code system_views.command_executions}. Every value is a string because
     * {@link JsonUtils#fromJsonMap(String)} hands the caller a raw map that it reads as {@code Map<String, String>}.
     */
    private static Map<String, String> toMap(ExecutionHistory record, boolean withOutput)
    {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_EXECUTION_ID, record.executionId().toString());
        fields.put(FIELD_COMMAND, record.commandName());
        fields.put(FIELD_STATUS, record.status().name());
        fields.put(FIELD_STARTED_AT, Long.toString(record.startTime()));

        Long completedAt = record.endTime();
        fields.put(FIELD_COMPLETED_AT, completedAt == null ? null : Long.toString(completedAt));

        Throwable error = record.error();
        fields.put(FIELD_ERROR, error == null ? null : ManagementUtils.causeMessages(error));

        ExecutionErrorType errorType = record.errorType();
        fields.put(FIELD_ERROR_TYPE, errorType == null ? null : errorType.name());

        if (withOutput)
            fields.put(FIELD_OUTPUT, record.output());

        return fields;
    }

    /** Validate command arguments against metadata. */
    protected void validateArguments(CommandExecutionArgs arguments, CommandMetadata metadata)
    {
        for (OptionMetadata option : metadata.options())
        {
            if (option.required() && !arguments.hasOption(option))
                throw new IllegalArgumentException(String.format("Required option '%s' is missing", option.paramLabel()));
        }

        for (ParameterMetadata param : metadata.parameters())
        {
            if (param.required() && !arguments.hasParameter(param))
                throw new IllegalArgumentException(String.format("Required parameter at index %d ('%s') is missing",
                                                                 param.index(), param.paramLabel()));
        }
    }

    private void collectCommandsRecursively(CommandRegistry registry,
                                            String parentCommandName,
                                            List<CommandEntry> result)
    {
        for (Map.Entry<String, Command<?>> entry : registry.commands())
        {
            String commandName = entry.getKey();
            Command<?> command = entry.getValue();

            String fullCommandName = fullCommandName(parentCommandName, commandName);
            if (command instanceof CommandRegistry)
                collectCommandsRecursively((CommandRegistry) command, fullCommandName, result);
            else
                result.add(new CommandEntry(fullCommandName, command));
        }
    }

    @Override
    public int getCommandCount()
    {
        return countCommands(registry);
    }

    @Override
    public String getCommandMBeanName(String fullCommandName)
    {
        ObjectName objectName = commandMBeanNames.get(fullCommandName);
        if (objectName == null)
            throw new IllegalArgumentException("Command MBean name not found: " + fullCommandName);
        return objectName.toString();
    }

    public boolean isStarted()
    {
        return started;
    }

    private void registerCommandMBeansRecursively(CommandRegistry registry, String parentCommandName)
    {
        for (Map.Entry<String, Command<?>> e : registry.commands())
        {
            String commandName = e.getKey();
            Command<?> command = e.getValue();
            String fullCommandName = fullCommandName(parentCommandName, commandName);

            if (command instanceof CommandRegistry)
            {
                registerCommandMBeansRecursively((CommandRegistry) command, fullCommandName);
            }
            else
            {
                try
                {
                    String escapedName = ObjectName.quote(fullCommandName);
                    ObjectName objectName = new ObjectName(format("%s:type=%s,name=%s",
                                                                  MBEAN_DOMAIN, MBEAN_TYPE_COMMAND, escapedName));
                    CommandMBeanAdapter commandMBean = new CommandMBeanAdapter(fullCommandName, command, this::execute);
                    MBeanWrapper.instance.registerMBean(commandMBean, objectName, MBeanWrapper.OnException.LOG);

                    ObjectName prev = commandMBeanNames.putIfAbsent(fullCommandName, objectName);
                    if (prev != null)
                    {
                        throw new IllegalStateException("Command name conflict during MBean registration: '" +
                                                        fullCommandName + "' is already registered");
                    }
                    logger.debug("Registered command MBean: {} -> {}", fullCommandName, objectName);
                }
                catch (Exception ex)
                {
                    logger.warn("Failed to register MBean for command: {}", fullCommandName, ex);
                }
            }
        }
    }

    private void unregisterCommandMBeans()
    {
        for (ObjectName objectName : commandMBeanNames.values())
            MBeanWrapper.instance.unregisterMBean(objectName, MBeanWrapper.OnException.LOG);
        commandMBeanNames.clear();
    }

    /** A leaf command paired with its full (dot-delimited) name, as returned by {@link #listCommands()}. */
    public static class CommandEntry
    {
        private final String fullName;
        private final Command<?> command;

        CommandEntry(String fullName, Command<?> command)
        {
            this.fullName = fullName;
            this.command = command;
        }

        public String fullName()
        {
            return fullName;
        }

        public Command<?> command()
        {
            return command;
        }
    }

    /** A validated command execution, ready to run on the caller thread or on {@link #asyncExecutor}. */
    private static class Prepared
    {
        final ExecutionHistory record;
        final CapturingOutput captured;
        final CommandExecutionArgs arguments;
        final CommandExecutionContext context;

        Prepared(ExecutionHistory record,
                 CapturingOutput captured,
                 CommandExecutionArgs arguments,
                 CommandExecutionContext context)
        {
            this.record = record;
            this.captured = captured;
            this.arguments = arguments;
            this.context = context;
        }
    }

    public static class CapturingOutput
    {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final PrintStream output = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        private final PrintStream error = new PrintStream(buffer, true, StandardCharsets.UTF_8);

        public String getCapturedOutput()
        {
            output.flush();
            error.flush();
            return buffer.toString(StandardCharsets.UTF_8);
        }

        Output createOutput()
        {
            return new Output(output, error);
        }
    }

    private static class ServerCommandExecutionContext implements CommandExecutionContext
    {
        private final Map<Class<?>, Object> services = new HashMap<>();

        public ServerCommandExecutionContext(NodeProbe probe)
        {
            services.put(NodeProbe.class, probe);
            services.put(Output.class, probe.output());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T service(Class<T> serviceType)
        {
            T svc = (T) services.get(serviceType);
            if (svc == null)
                throw new IllegalArgumentException("Service not available in this context: " + serviceType.getName());
            return svc;
        }

        @Override
        public Set<Class<?>> services()
        {
            return services.keySet();
        }
    }

    public static class CommandResult
    {
        private final UUID executionId;
        private final String output;
        private final long startTime;
        private final long durationMillis;

        public CommandResult(UUID executionId, String output, long startTime, long durationMillis)
        {
            this.executionId = executionId;
            this.output = output;
            this.startTime = startTime;
            this.durationMillis = durationMillis;
        }

        public UUID getExecutionId()
        {
            return executionId;
        }

        public String getOutput()
        {
            return output;
        }

        public long getStartTime()
        {
            return startTime;
        }

        public long getDurationMillis()
        {
            return durationMillis;
        }
    }

    /**
     * Record of a single command execution. Retained in {@link BoundedExecutionHistory} and exposed to
     * operators as command execution history through virtual tables.
     */
    public static class ExecutionHistory
    {
        final UUID executionId;
        final String commandName;
        final long startTime;
        /** Non-null only for asynchronous executions. Synchronous callers already received their output. */
        final CapturingOutput captured;
        volatile long endTime;
        volatile ExecutionStatus status = ExecutionStatus.RUNNING;
        volatile Throwable error;

        ExecutionHistory(UUID executionId, String commandName, long startTime, CapturingOutput captured)
        {
            this.executionId = executionId;
            this.commandName = commandName;
            this.startTime = startTime;
            this.captured = captured;
        }

        void completed(long endTime)
        {
            this.endTime = endTime;
            this.status = ExecutionStatus.COMPLETED;
        }

        void failed(long endTime, Throwable error)
        {
            this.error = error;
            this.endTime = endTime;
            this.status = ExecutionStatus.FAILED;
        }

        public UUID executionId() { return executionId; }
        public String commandName() { return commandName; }
        public ExecutionStatus status() { return status; }
        public boolean isSuccess() { return status == ExecutionStatus.COMPLETED; }
        public long startTime() { return startTime; }
        public Long endTime() { return status == ExecutionStatus.RUNNING ? null : endTime; }
        public Throwable error() { return error; }

        public ExecutionErrorType errorType()
        {
            if (error == null)
                return null;
            if (error instanceof CommandValidationException)
                return ExecutionErrorType.VALIDATION;
            if (error instanceof CommandAuthorizationException)
                return ExecutionErrorType.AUTHORIZATION;
            return ExecutionErrorType.EXECUTION;
        }

        /** Output produced so far, or null for synchronous executions. */
        public String output()
        {
            return captured == null ? null : captured.getCapturedOutput();
        }
    }

    private static class BoundedExecutionHistory
    {
        private final Deque<ExecutionHistory> dq = new ConcurrentLinkedDeque<>();
        private final AtomicInteger size = new AtomicInteger(0);
        private final int maxSize;

        public BoundedExecutionHistory(int maxSize)
        {
            this.maxSize = maxSize;
        }

        /**
         * Evicts the oldest finished record, so a running command is still observable no matter how many
         * short commands ran since it started. The deque exceeds {@code maxSize} while every record is running.
         */
        public void add(ExecutionHistory info)
        {
            dq.offer(info);

            if (size.incrementAndGet() > maxSize)
            {
                for (Iterator<ExecutionHistory> it = dq.iterator(); it.hasNext(); )
                {
                    if (it.next().status != ExecutionStatus.RUNNING)
                    {
                        it.remove();
                        size.decrementAndGet();
                        break;
                    }
                }
            }
        }

        /** Returns a point-in-time snapshot of the retained records, oldest first. */
        public List<ExecutionHistory> snapshot()
        {
            return new ArrayList<>(dq);
        }
    }

    @FunctionalInterface
    public interface Executor
    {
        CommandResult execute(String commandName, Supplier<CommandExecutionArgs> argsSupplier)
            throws CommandExecutionException, CommandValidationException, CommandAuthorizationException;
    }
}

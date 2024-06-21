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

package org.apache.cassandra.management.api;

import java.io.IOException;
import java.util.List;
import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.cassandra.management.BaseCommand;
import org.apache.cassandra.management.ServiceBridge;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.Output;
import picocli.CommandLine;

import static java.lang.Integer.parseInt;
import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.promptAndReadPassword;
import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.readUserPasswordFromFile;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Command options for NodeTool commands that are executed via JMX.
 */
public class JmxConnectionMixin
{
    public static final String MIXIN_KEY = "jmx";

    @CommandLine.Option(names = { "-h", "--host" }, description = "Node hostname or ip address")
    public String host = "127.0.0.1";

    @CommandLine.Option(names = { "-p", "--port" }, description = "Remote jmx agent port number")
    public String port = "7199";

    @CommandLine.Option(names = { "-u", "--username" }, description = "Remote jmx agent username")
    public String username = EMPTY;

    @CommandLine.Option(names = { "-pw", "--password" }, description = "Remote jmx agent password")
    public String password = EMPTY;

    @CommandLine.Option(names = { "-pwf", "--password-file" }, description = "Path to the JMX password file")
    public String passwordFilePath = EMPTY;

    @CommandLine.Option(names = { "-pp", "--print-port" }, description = "Operate in 4.0 mode with hosts disambiguated by port number")
    public boolean printPort = false;

    @Inject
    private INodeProbeFactory nodeProbeFactory;
    @Inject
    private Output output;

    /**
     * This method is called by picocli and used depending on the execution strategy.
     * @param parseResult The parsed command line.
     * @return The exit code.
     */
    public static int executionStrategy(CommandLine.ParseResult parseResult)
    {
        List<CommandLine> parsedCommands = parseResult.asCommandLineList();
        int start = indexOfLastSubcommandWithSameParent(parsedCommands);
        CommandLine.Model.CommandSpec lastParent = parsedCommands.get(start).getCommandSpec();
        CommandLine.Model.CommandSpec jmx = lastParent.mixins().get(MIXIN_KEY);
        if (jmx == null)
            throw new CommandLine.InitializationException("No JmxConnect mixin found in the command hierarchy");

        if (lastParent.userObject() instanceof BaseCommand)
            ((BaseCommand) lastParent.userObject()).setBridge(((JmxConnectionMixin) jmx.userObject()).init(lastParent));
        return new CommandLine.RunLast().execute(parseResult);
    }

    private static int indexOfLastSubcommandWithSameParent(List<CommandLine> parsedCommands)
    {
        int start = parsedCommands.size() - 1;
        for (int i = parsedCommands.size() - 2; i >= 0; i--)
        {
            if (parsedCommands.get(i).getParent() != parsedCommands.get(i + 1).getParent())
                break;
            start = i;
        }
        return start;
    }

    /**
     * Initialize the JMX connection to the Cassandra node.
     * @param spec The command specification to be executed after the initialization.
     * @return The ServiceBridge instance to interact with the Cassandra node.
     */
    private ServiceBridge init(CommandLine.Model.CommandSpec spec)
    {
        try
        {
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            return username.isEmpty() ? nodeProbeFactory.create(host, parseInt(port))
                                      : nodeProbeFactory.create(host, parseInt(port), username, password);
        }
        catch (IOException | SecurityException e)
        {
            Throwable rootCause = Throwables.getRootCause(e);
            output.err.printf("nodetool: Failed to connect to '%s:%s' - %s: '%s'.%n", host, port,
                              rootCause.getClass().getSimpleName(), rootCause.getMessage());
            throw new CommandLine.ExecutionException(spec.commandLine(), "Failed to connect to JMX", e);
        }
    }
}

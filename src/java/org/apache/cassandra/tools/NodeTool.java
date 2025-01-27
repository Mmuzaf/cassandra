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
package org.apache.cassandra.tools;

import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.TopLevelCommand;
import org.apache.cassandra.tools.nodetool.layout.CassandraCliHelpLayout;
import org.apache.cassandra.utils.FBUtilities;
import picocli.CommandLine;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

    protected final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeTool(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    /**
     * Execute the command line utility with the given arguments via the JMX connection.
     *
     * @param args command line arguments
     * @return 0 on success, 1 on bad use, 2 on execution error
     */
    public int execute(String... args)
    {
        try
        {
            CommandLine commandLine = createCommandLine(new CassandraCliFactory(nodeProbeFactory, output));
            configureCliLayout(commandLine);
            commandLine.setExecutionStrategy(JmxConnect::executionStrategy)
                       .setExecutionExceptionHandler((ex, c, arg) -> {
                           // Used for backward compatibility, some commands are validated when a command is run.
                           if (ex instanceof IllegalArgumentException |
                               ex instanceof IllegalStateException)
                           {
                               badUse(ex);
                               return 1;
                           }

                           err(Throwables.getRootCause(ex));
                           return 2;
                       })
                       .setParameterExceptionHandler((ex, arg) -> {
                           badUse(ex);
                           return 1;
                       })
                       // Some of the Cassandra commands don't comply with the POSIX standard, so we need to disable such options.
                       // Example: ./nodetool -h localhost -p 7100 repair mykeyspayce -hosts 127.0.0.1,127.0.0.2
                       //
                       // This also means that option parameters must be separated from the option name by whitespace
                       // or the = separator character, so -D key=value and -D=key=value will be recognized but
                       // -Dkey=value will not.
                       .setPosixClusteredShortOptionsAllowed(false);

            printHistory(args);
            return commandLine.execute(args);
        }
        catch (Exception e)
        {
            err(output.err::println, e);
            return 2;
        }
    }

    public static List<String> getCommandsWithoutRoot(String separator)
    {
        List<String> commands = new ArrayList<>();
        getCommandsWithoutRoot(createCommandLine(new CassandraCliFactory(new NodeProbeFactory(), Output.CONSOLE)), commands, separator);
        return commands;
    }

    private static void getCommandsWithoutRoot(CommandLine cli, List<String> commands, String separator)
    {
        String name = cli.getCommandSpec().qualifiedName(separator);
        // Skip the root command as it's not a real command.
        if (cli.getCommandSpec().root() != cli.getCommandSpec())
            commands.add(name.replace(cli.getCommandSpec().root().qualifiedName() + separator, ""));
        for (CommandLine sub : cli.getSubcommands().values())
            getCommandsWithoutRoot(sub, commands, separator);
    }

    public static CommandLine.Model.CommandSpec lastExecutableSubcommandWithSameParent(List<CommandLine> parsedCommands)
    {
        int start = parsedCommands.size() - 1;
        for (int i = parsedCommands.size() - 2; i >= 0; i--)
        {
            if (parsedCommands.get(i).getParent() != parsedCommands.get(i + 1).getParent())
                break;
            start = i;
        }
        return parsedCommands.get(start).getCommandSpec();
    }

    private static CommandLine createCommandLine(CassandraCliFactory factory)
    {
        return new CommandLine(new TopLevelCommand(), factory)
                   .addMixin(JmxConnect.MIXIN_KEY, factory.create(JmxConnect.class))
                   .setOut(new PrintWriter(factory.output.out, true))
                   .setErr(new PrintWriter(factory.output.err, true));
    }

    private static void configureCliLayout(CommandLine commandLine)
    {
        switch (CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getEnum(true, CliLayout.class))
        {
            case CASSANDRA:
                commandLine.setHelpFactory(CassandraCliHelpLayout::new)
                           .setUsageHelpWidth(CassandraCliHelpLayout.DEFAULT_USAGE_HELP_WIDTH)
                           .setHelpSectionKeys(CassandraCliHelpLayout.cassandraHelpSectionKeys());
                break;
            case PICOCLI:
                break;
            default:
                throw new IllegalStateException("Unknown CLI layout: " +
                                                CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getString());
        }
    }

    private enum CliLayout
    {
        CASSANDRA,
        PICOCLI
    }

    public static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE).newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    public static void badUse(Consumer<String> out, Throwable e)
    {
        out.accept("nodetool: " + e.getMessage());
        out.accept("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void badUse(Exception e)
    {
        badUse(output.out::println, e);
    }

    public static void err(Consumer<String> out, Throwable e)
    {
        // CASSANDRA-11537: friendly error message when server is not ready
        if (e instanceof InstanceNotFoundException)
            throw new IllegalArgumentException("Server is not initialized yet, cannot run nodetool.");

        out.accept("error: " + e.getMessage());
        out.accept("-- StackTrace --");
        out.accept(getStackTraceAsString(e));
    }

    protected void err(Throwable e)
    {
        err(output.err::println, e);
    }

    private static class CassandraCliFactory implements CommandLine.IFactory
    {
        private final CommandLine.IFactory fallback;
        private final INodeProbeFactory nodeProbeFactory;
        private final Output output;

        public CassandraCliFactory(INodeProbeFactory nodeProbeFactory, Output output)
        {
            this.fallback = CommandLine.defaultFactory();
            this.nodeProbeFactory = nodeProbeFactory;
            this.output = output;
        }

        public <K> K create(Class<K> cls)
        {
            try
            {
                K bean = this.fallback.create(cls);
                Class<?> beanClass = bean.getClass();
                do
                {
                    Field[] fields = beanClass.getDeclaredFields();
                    for (Field field : fields)
                    {
                        if (!field.isAnnotationPresent(Inject.class))
                            continue;
                        field.setAccessible(true);
                        if (field.getType().equals(INodeProbeFactory.class))
                            field.set(bean, nodeProbeFactory);
                        else if (field.getType().equals(Output.class))
                            field.set(bean, output);
                    }
                }
                while ((beanClass = beanClass.getSuperclass()) != null);
                return bean;
            }
            catch (Exception e)
            {
                throw new CommandLine.InitializationException("Failed to create instance of " + cls, e);
            }
        }
    }
}

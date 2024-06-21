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

import java.io.PrintWriter;
import java.lang.reflect.Field;
import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.management.CassandraHelpLayout;
import org.apache.cassandra.management.api.JmxConnectionMixin;
import org.apache.cassandra.management.api.TopLevelCommand;
import org.apache.cassandra.utils.FBUtilities;
import picocli.CommandLine;

import static org.apache.cassandra.tools.NodeTool.badUse;
import static org.apache.cassandra.tools.NodeTool.err;
import static org.apache.cassandra.tools.NodeTool.printHistory;

public class NodeToolV2
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeToolV2(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeToolV2(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    /**
     * Execute the command line utility with the given arguments via the JMX connection.
     * @param args command line arguments
     * @return 0 on success, 1 on bad use, 2 on execution error
     */
    public int execute(String... args)
    {
        CommandLine.IFactory factory;
        CommandLine commandLine = new CommandLine(new TopLevelCommand(), factory = new CassandraCliFactory(nodeProbeFactory, output));

        configureCliLayout(commandLine);
        commandLine.setOut(new PrintWriter(output.out, true))
                   .setErr(new PrintWriter(output.err, true))
                   .setExecutionExceptionHandler((ex, cmdLine, parseResult) -> {
                       err(cmdLine.getErr()::println, Throwables.getRootCause(ex));
                       return 2;
                   })
                   .setParameterExceptionHandler((ex, arg) -> {
                       badUse(commandLine.getOut()::println, Throwables.getRootCause(ex));
                       return 1;
                   });

        try
        {
            JmxConnectionMixin mixin = factory.create(JmxConnectionMixin.class);
            commandLine.setExecutionStrategy(JmxConnectionMixin::executionStrategy);
            commandLine.addMixin(JmxConnectionMixin.MIXIN_KEY, mixin);
            commandLine.getSubcommands().values().forEach(sub -> sub.addMixin(JmxConnectionMixin.MIXIN_KEY, mixin));

            printHistory(args);
            return commandLine.execute(args);
        }
        catch (Exception e)
        {
            err(commandLine.getErr()::println, e);
            return 2;
        }
    }

    private static void configureCliLayout(CommandLine commandLine)
    {
        if (CassandraRelevantProperties.CASSANDRA_CLI_PICOCLI_LAYOUT.getBoolean())
            return;

        commandLine.setHelpFactory(CassandraHelpLayout::new)
                   .setUsageHelpWidth(CassandraHelpLayout.DEFAULT_USAGE_HELP_WIDTH)
                   .setHelpSectionKeys(CassandraHelpLayout.cassandraHelpSectionKeys());
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

        public <K> K create(Class<K> cls) throws Exception
        {
            Object bean = this.fallback.create(cls);
            Field[] fields = bean.getClass().getDeclaredFields();
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
            return (K) bean;
        }
    }
}

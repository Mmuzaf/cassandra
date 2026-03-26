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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.inject.Inject;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;

import picocli.CommandLine;

import static org.junit.Assert.assertTrue;

public class NodetoolClassHierarchyTest extends CQLTester
{
    /**
     * Ensures no command in the nodetool hierarchy has duplicate {@code @Inject} fields
     * for the same type across its class inheritance chain.
     * <p>
     * {@code AbstractCommand} declares {@code @Inject} ann for {@code INodeProbeFactory}
     * and {@code Output}. If a subclass or any intermediate classes in the hierarchy
     * redeclares an {@code @Inject} field of the same type, the picocli factory would
     * inject both, leading to ambiguity and wasted resources.
     */
    @Test
    public void testNoDuplicatesForInjectableFields() throws Exception
    {
        checkInjectableDuplicates(NodeTool.createCommandLine(CommandLine.defaultFactory()));
    }

    private void checkInjectableDuplicates(CommandLine command)
    {
        for (CommandLine sub : command.getSubcommands().values())
            checkInjectableDuplicates(sub);

        if (command.getCommandSpec().userObject() instanceof AbstractCommand)
        {
            AbstractCommand userObject = (AbstractCommand) command.getCommandSpec().userObject();
            int nodeProbeFactoryCount = 0;
            int outputCount = 0;
            Class<?> beanClass = userObject.getClass();
            do
            {
                Field[] fields = beanClass.getDeclaredFields();
                for (Field field : fields)
                {
                    if (!field.isAnnotationPresent(Inject.class))
                        continue;
                    if (field.getType().equals(INodeProbeFactory.class))
                        nodeProbeFactoryCount++;
                    else if (field.getType().equals(Output.class))
                        outputCount++;
                    else
                        throw new AssertionError("Unexpected injectable field type: " + field.getType());
                }
            }
            while ((beanClass = beanClass.getSuperclass()) != null);

            if (nodeProbeFactoryCount > 1 || outputCount > 1)
                throw new AssertionError("Multiple injectable fields in the command class hierarchy (should be exactly 1 for each type): " +
                                         userObject.getClass().getCanonicalName());
        }
    }

    /**
     * Commands must be self-contained: every {@code @Option} and {@code @Parameters}
     * a command needs must be declared on the command itself or via {@code @Mixin},
     * not inherited through {@code @ParentCommand}. Self-contained commands can be
     * instantiated and executed independently without walking the parent hierarchy,
     * which is required for remote execution via CQL or MBean.
     */
    @Test
    public void testNodetoolCommandsMustBeSelfContained()
    {
        CommandLine root = new CommandLine(NodetoolCommand.class);
        Map<String, List<String>> affected = new TreeMap<>();

        commandTreeWalker(root, cmd -> {
            List<String> parentArgs = collectConsumedParentArgs(cmd);
            if (!parentArgs.isEmpty())
                affected.put(fullCommandName(cmd), parentArgs);
        });

        assertTrue("The following commands declare @ParentCommand and consume " +
                   "options or parameters from a parent command: " + buildAffectedCommandMessage(affected),
                   affected.isEmpty());
    }

    /**
     * Previously, in the Airline implementation of nodetool, the defaul values for command
     * parameters were declared as values in the java field of the command class. We left this
     * as-is, but with picocli we have to make sure that this is consistend with annotations
     * such as {@code @Option(defaultValue = "...")}.
     * <p>
     * When {@code @Option(defaultValue = "...")} is declared on a field, the Java field
     * initializer must produce the same value. This is required because picocli only applies
     * {@code defaultValue} during {@code parseArgs()}, any code that reads the field before
     * parsing sees only the Java initializer, not the annotations default.
     */
    @Test
    public void testOptionAnnotationDefaultMatchesJavaInitializer()
    {
        CommandLine root = new CommandLine(NodetoolCommand.class);
        Map<String, List<String>> violations = new TreeMap<>();

        commandTreeWalker(root, cmd -> {
            List<String> cmdViolations = collectDefaultValueMismatches(cmd);
            if (!cmdViolations.isEmpty())
                violations.put(fullCommandName(cmd), cmdViolations);
        });

        assertTrue("The following commands have @Option(defaultValue) that does not match " +
                   "the Java field initializer. Either remove defaultValue from the annotation " +
                   "or align the Java field initializer with it:\n" +
                   buildAffectedCommandMessage(violations),
                   violations.isEmpty());
    }

    /**
     * Commands must not declare aliases. Alias MBean registration is not supported,
     * so allowing aliases would create a mismatch between the CLI (which would honor them)
     * and the JMX/CQL transports (which would not). If alias support is added in the future,
     * this test should be replaced with proper alias-aware registration and lookup logic.
     */
    @Test
    public void testNoCommandDeclaresAliases()
    {
        CommandLine root = new CommandLine(NodetoolCommand.class);
        Map<String, List<String>> affected = new TreeMap<>();

        commandTreeWalker(root, cmd -> {
            String[] aliases = cmd.getCommandSpec().aliases();
            if (aliases.length > 0)
                affected.put(fullCommandName(cmd), Arrays.asList(aliases));
        });

        assertTrue("The following commands declare aliases, but alias registration " +
                   "is not yet supported across all transports (JMX, CQL):\n" +
                   buildAffectedCommandMessage(affected),
                   affected.isEmpty());
    }

    private static List<String> collectDefaultValueMismatches(CommandLine cmd)
    {
        List<String> violations = new ArrayList<>();

        for (CommandLine.Model.OptionSpec optionSpec : cmd.getCommandSpec().options())
        {
            if (optionSpec.usageHelp() || optionSpec.versionHelp())
                continue;

            String annotationDefault = optionSpec.defaultValue();
            if (annotationDefault == null)
                continue;

            Object javaValue = optionSpec.getValue();
            String javaValueStr = javaValue == null ? "null" : String.valueOf(javaValue);

            if (!annotationDefault.equals(javaValueStr))
            {
                violations.add(String.format("option %s: @Option(defaultValue=\"%s\") but Java initializer gives \"%s\"",
                                             Arrays.toString(optionSpec.names()),
                                             annotationDefault,
                                             javaValueStr));
            }
        }

        for (CommandLine.Model.PositionalParamSpec paramSpec : cmd.getCommandSpec().positionalParameters())
        {
            String annotationDefault = paramSpec.defaultValue();
            if (annotationDefault == null)
                continue;

            Object javaValue = paramSpec.getValue();
            String javaValueStr = javaValue == null ? "null" : String.valueOf(javaValue);

            if (!annotationDefault.equals(javaValueStr))
            {
                violations.add(String.format("parameter index=%s (%s): @Parameters(defaultValue=\"%s\") but Java initializer gives \"%s\"",
                                             paramSpec.index(),
                                             paramSpec.paramLabel(),
                                             annotationDefault,
                                             javaValueStr));
            }
        }

        return violations;
    }

    /**
     * For a given command, follows the {@code @ParentCommand} chain and collects all
     * options and parameters declared on each parent.
     */
    private static List<String> collectConsumedParentArgs(CommandLine cmd)
    {
        Object userObject = cmd.getCommandSpec().userObject();
        if (userObject == null)
            return List.of();

        List<String> parentArgs = new ArrayList<>();
        CommandLine currentCmd = cmd;
        Class<?> currentClass = userObject.getClass();

        while (true)
        {
            Field parentField = findAnnotatedField(currentClass);
            if (parentField == null)
                break;

            CommandLine parentCmd = currentCmd.getParent();
            if (parentCmd == null)
                break;

            addOptionsFromSpec(parentCmd, parentArgs);
            addParametersFromSpec(parentCmd, parentArgs);

            Object parentUserObject = parentCmd.getCommandSpec().userObject();
            if (parentUserObject == null)
                break;

            currentCmd = parentCmd;
            currentClass = parentUserObject.getClass();
        }

        return parentArgs;
    }

    private static void commandTreeWalker(CommandLine cmd, Consumer<CommandLine> consumer)
    {
        consumer.accept(cmd);
        for (CommandLine sub : cmd.getSubcommands().values())
            commandTreeWalker(sub, consumer);
    }

    private static Field findAnnotatedField(Class<?> clazz)
    {
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass())
        {
            for (Field field : c.getDeclaredFields())
            {
                if (field.isAnnotationPresent(CommandLine.ParentCommand.class))
                    return field;
            }
        }
        return null;
    }

    private static void addOptionsFromSpec(CommandLine cmd, List<String> out)
    {
        for (CommandLine.Model.OptionSpec option : cmd.getCommandSpec().options())
        {
            if (option.usageHelp() || option.versionHelp())
                continue;
            out.add(String.format("option '%s': %s (%s)",
                                  cmd.getCommandName(),
                                  String.join("/", option.names()),
                                  option.type().getSimpleName()));
        }
    }

    private static void addParametersFromSpec(CommandLine cmd, List<String> out)
    {
        for (CommandLine.Model.PositionalParamSpec param : cmd.getCommandSpec().positionalParameters())
        {
            out.add(String.format("parameter '%s': %s index=%s (%s)",
                                  cmd.getCommandName(),
                                  param.paramLabel(),
                                  param.index(),
                                  param.type().getSimpleName()));
        }
    }

    /** Build the full command path, e.g. {@code nodetool.removenode.status}. */
    private static String fullCommandName(CommandLine cmd)
    {
        List<String> parts = new ArrayList<>();
        CommandLine current = cmd;
        while (current != null)
        {
            parts.add(0, current.getCommandName());
            current = current.getParent();
        }
        return String.join(".", parts);
    }

    private static String buildAffectedCommandMessage(Map<String, List<String>> affected)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : affected.entrySet())
        {
            sb.append("  Command: ").append(entry.getKey()).append('\n');
            for (String detail : entry.getValue())
                sb.append("    - ").append(detail).append('\n');
        }
        return sb.toString();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.management.api;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

import static javax.management.MBeanOperationInfo.ACTION;
import static org.apache.cassandra.management.CommandUtils.optionName;
import static org.apache.cassandra.management.CommandUtils.buildAttributeDescription;
import static org.apache.cassandra.management.CommandUtils.paramName;

/**
 * Command MBean exposes single mamagement command to the JMX interface.
 *
 * @see Command
 * @see CommandsRegistry
 */
public class CommandMBean implements DynamicMBean
{
    /** Name of the JMX method to invoke command. */
    public static final String INVOKE = "invoke";
    public static final String OPTIONAL = "0..1";
    public static final String REPREATABLE = "0..*";

    /** Used for tests. Name of the method to retrive last method result. */
    public static final String LAST_RES_METHOD = "lastResult";

    private static final Logger logger = LoggerFactory.getLogger(CommandMBean.class);
    private final Command command;

    /**
     * @param command Management command to expose.
     */
    public CommandMBean(Command command)
    {
        this.command = command;
    }

    @Override
    public Object invoke(String actionName,
                         Object[] params,
                         String[] signature) throws MBeanException, ReflectionException
    {
        // Default JMX invoker pass arguments in for params: Object[] = { "invoke", parameter_values_array, types_array}
        // while JConsole pass params values directly in params array. This check supports both way of invocation.
        if (params.length == 3
            && (params[0].equals(INVOKE) || params[0].equals(LAST_RES_METHOD))
            && params[1] instanceof Object[])
            return invoke((String) params[0], (Object[]) params[1], (String[]) params[2]);

        if (!INVOKE.equals(actionName))
            throw new UnsupportedOperationException(actionName);

        try
        {
            StringBuilder resStr = new StringBuilder();
            Consumer<String> printer = str -> resStr.append(str).append('\n');

//            CommandInvoker<A> invoker = new CommandInvoker<>(command, new ParamsToArgument(params).argument(), ignite);
//            res = invoker.invoke(printer, false);

            return resStr.toString();
        }
        catch (Exception e)
        {
            logger.error("Invoke error:", e);
            throw e;
        }
    }

    /**
     * All options are required <em>within the ArgGroup</em>, while the group itself is optional:
     * <pre>
     * public class DependentOptions {
     *     &#064;ArgGroup(exclusive = false, multiplicity = "0..1")
     *     Dependent group;
     *
     *     static class Dependent {
     *         &#064;Option(names = "-a", required = true) int a;
     *         &#064;Option(names = "-b", required = true) int b;
     *         &#064;Option(names = "-c", required = true) int c;
     *     }
     * }</pre>
     *
     * @return MBean info.
     */
    @Override
    public MBeanInfo getMBeanInfo()
    {
        List<MBeanParameterInfo> args = new ArrayList<>();
        visitCommandSpec(command, (name, desc) -> args.add(new MBeanParameterInfo(name, String.class.getName(), desc)));
        return new MBeanInfo(CommandMBean.class.getName(),
                             command.getClass().getSimpleName(),
                             null,
                             null,
                             new MBeanOperationInfo[]{
                                 new MBeanOperationInfo(INVOKE,
                                                        command.description(),
                                                        args.toArray(MBeanParameterInfo[]::new),
                                                        String.class.getName(),
                                                        ACTION)
                             },
                             null);
    }

    public static void visitCommandSpec(Command command, BiConsumer<String, String> visitor)
    {
        CommandLine.Model.CommandSpec spec = CommandLine.Model.CommandSpec.forAnnotatedObject(command.commandUserObject());
        // Options are required within the ArgGroup, while the group itself is optional.
        for (CommandLine.Model.OptionSpec option : spec.options())
        {
            if (!option.isOption())
                continue;

            visitor.accept(optionName(option.longestName()), buildAttributeDescription(option.description()));
        }

        // Positional parameters are required.
        for (CommandLine.Model.PositionalParamSpec positional : spec.positionalParameters())
        {
            if (!positional.isPositional())
                continue;

            visitor.accept(paramName(positional.paramLabel()), buildAttributeDescription(positional.description()));
        }
    }

    @Override
    public AttributeList getAttributes(String[] attributes)
    {
        throw new UnsupportedOperationException("getAttributes");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes)
    {
        throw new UnsupportedOperationException("setAttributes");
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException
    {
        throw new UnsupportedOperationException("getAttribute");
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
                                                         MBeanException, ReflectionException
    {
        throw new UnsupportedOperationException("setAttribute");
    }

    /**  */
    private class ParamsToArgument implements Function<Field, Object>
    {
        private int cntr;
        private final Object[] vals;

        private ParamsToArgument(Object[] vals)
        {
            this.vals = vals;
        }

        public Object argument()
        {
            // This will map vals to argument fields.
//            return CommandUtils.argument(command.argClass(), (fld, pos) -> apply(fld), this);
            return null;
        }

        @Override
        public Object apply(Field field)
        {
            String val = (String) vals[cntr];

            cntr++;

//            return !F.isEmpty(val) ? CommandUtils.parseVal(val, field.getType()) : null;
            return null;
        }
    }
}

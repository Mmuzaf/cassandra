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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import javax.inject.Inject;

import org.apache.cassandra.exceptions.CommandExecutionException;
import picocli.CommandLine;

public class CommandInvoker<T, R>
{
    private final Command<T, R> command;
    private final CommandContext context;
    private final String[] args;

    public CommandInvoker(Command<T, R> command, CommandContext context, String... args)
    {
        this.command = command;
        this.context = context;
        this.args = args;
    }

    /**
     * Invoke the command and return the exit code.
     * @param resultConsumer a consumer for the result.
     * @return the exit code.
     */
    public int invokeExitCode(Consumer<R> resultConsumer)
    {
        return new CommandLine(instantiateCommand(command), new InjectContextFactory(context)).execute(args);
    }

    /**
     * Invoke the command and return the result.
     * @param printer a consumer for the result.
     * @return the result.
     */
    public R invokeResult(Consumer<String> printer)
    {
        new CommandLine(instantiateCommand(command), new InjectContextFactory(context)).execute(args);
        return null;
    }

    private Object instantiateCommand(Command<?, ?> command)
    {
        try
        {
            return new InjectContextFactory(context).create(command.commandUserObject());
        }
        catch (Exception e)
        {
            throw new CommandExecutionException(String.format("Failed to inject resource [command=%s]",
                                                              command.commandUserObject()), e);
        }
    }

    private static class InjectContextFactory implements CommandLine.IFactory
    {
        private final CommandContext context;

        public InjectContextFactory(CommandContext context)
        {
            this.context = context;
        }

        /**
         * Create an instance of the specified class. This method is called by picocli to obtain instances of classes.
         * @param clazz the class of the object to return.
         * @return an instance of the specified class.
         * @param <T> the type of the object to return.
         * @throws Exception an exception.
         */
        public <T> T create(Class<T> clazz) throws Exception
        {
            if (clazz.isInterface())
            {
                if (Collection.class.isAssignableFrom(clazz))
                {
                    if (List.class.isAssignableFrom(clazz))
                        return clazz.cast(new ArrayList<>());
                    else if (SortedSet.class.isAssignableFrom(clazz))
                        return clazz.cast(new TreeSet<>());
                    else if (Set.class.isAssignableFrom(clazz))
                        return clazz.cast(new LinkedHashSet<>());
                    else if (Queue.class.isAssignableFrom(clazz))
                        return clazz.cast(new LinkedList<>());
                    else
                        return clazz.cast(new ArrayList<>());
                }
                if (SortedMap.class.isAssignableFrom(clazz))
                {
                    return clazz.cast(new TreeMap<>());
                }
                if (Map.class.isAssignableFrom(clazz))
                {
                    return clazz.cast(new LinkedHashMap<>());
                }
            }

            // Inject the context into the command object.
            Object target = clazz.getDeclaredConstructor().newInstance();
            for (Field field : clazz.getDeclaredFields())
            {
                if (field.isAnnotationPresent(Inject.class) && field.getType().isAssignableFrom(context.getClass()))
                    inject(field, target, context);
            }
            return clazz.cast(target);
        }

        private static void inject(Field field, Object target, Object resource) throws Exception
        {
            field.setAccessible(true);
            field.set(target, resource);
        }
    }
}

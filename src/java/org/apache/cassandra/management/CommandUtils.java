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

import java.io.PrintStream;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.service.StorageServiceMBean;

public final class CommandUtils
{
    public static void checkJobs(PrintStream out, StorageServiceMBean ssProxy, int jobs)
    {
        int compactors = ssProxy.getConcurrentCompactors();
        if (jobs > compactors)
            out.printf("jobs (%d) is bigger than configured concurrent_compactors (%d) on the host, using at most %d threads%n",
                       jobs, compactors, compactors);
    }

    public static ObjectName makeMBeanName(String commandName)
    {
        try
        {
            return new ObjectName(String.format("%s:name=%s", CassandraCommandRegistry.MBEAN_MANAGEMENT_COMMAND,
                                                commandName));
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String buildAttributeDescription(String[] description)
    {
        StringBuilder sb = new StringBuilder();
        for (String line : description)
        {
            sb.append(line);
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    public static String paramName(String label)
    {
        if (label.startsWith("<") && label.endsWith(">"))
            return label.substring(1, label.length() - 1);
        return label;
    }

    public static String optionName(String name)
    {
        if (name.startsWith("--"))
            return name.substring(2);
        else if (name.startsWith("-"))
            return name.substring(1);
        return name;
    }
}

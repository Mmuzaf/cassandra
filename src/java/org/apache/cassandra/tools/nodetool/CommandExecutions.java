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

import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_COMMAND;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_COMPLETED_AT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR_TYPE;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_EXECUTION_ID;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_OUTPUT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_STARTED_AT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_STATUS;

@Command(name = "commandexecutions",
         description = "Print management command executions. The node retains the last 100 in memory and " +
                       "none across restarts")
public class CommandExecutions extends AbstractCommand
{
    private static final List<String> LIST_FIELDS =
        List.of(FIELD_EXECUTION_ID, FIELD_COMMAND, FIELD_STATUS, FIELD_STARTED_AT, FIELD_COMPLETED_AT, FIELD_ERROR_TYPE);

    private static final List<String> DETAIL_FIELDS =
        List.of(FIELD_EXECUTION_ID, FIELD_COMMAND, FIELD_STATUS, FIELD_STARTED_AT, FIELD_COMPLETED_AT, FIELD_ERROR, FIELD_ERROR_TYPE);

    @Parameters(index = "0", arity = "0..1", paramLabel = "<execution-id>",
                description = "Print one execution in full, including its output")
    private String executionId;

    @Option(paramLabel = "command", names = { "-c", "--command" },
            description = "Only show executions of this command, e.g. repair or compressiondictionary.train")
    private String command;

    @Option(names = "--latest", description = "Print the newest execution of --command in full")
    private boolean latest;

    @Override
    public void execute(NodeProbe probe)
    {
        if (executionId != null && (command != null || latest))
            throw new IllegalArgumentException("An execution id cannot be combined with --command or --latest");

        if (latest && command == null)
            throw new IllegalArgumentException("--latest requires --command");

        PrintStream out = probe.output().out;

        if (executionId != null)
        {
            printDetail(out, probe, executionId);
            return;
        }

        List<Map<String, String>> executions = executions(probe.getCommandExecutions(command));

        if (!latest)
        {
            printList(out, executions);
            return;
        }

        if (executions.isEmpty())
            throw new IllegalArgumentException("No retained executions of command '" + command + '\'');

        printDetail(out, probe, executions.get(0).get(FIELD_EXECUTION_ID));
    }

    private static void printList(PrintStream out, List<Map<String, String>> executions)
    {
        if (executions.isEmpty())
        {
            out.println("No command executions retained.");
            return;
        }

        TableBuilder table = new TableBuilder();
        table.add(LIST_FIELDS);

        for (Map<String, String> execution : executions)
        {
            List<String> row = new ArrayList<>(LIST_FIELDS.size());
            for (String field : LIST_FIELDS)
                row.add(display(execution, field));
            table.add(row);
        }

        table.printTo(out);
    }

    private static void printDetail(PrintStream out, NodeProbe probe, String executionId)
    {
        String json = probe.getCommandExecution(executionId);
        if (json == null)
            throw new IllegalArgumentException("No retained execution with id '" + executionId + '\'');

        Map<String, String> execution = JsonUtils.fromJsonMap(json);
        for (String field : DETAIL_FIELDS)
            out.println(field + ": " + display(execution, field));

        out.println();
        String output = execution.get(FIELD_OUTPUT);
        out.println(output == null ? "(output not retained for synchronous executions)" : output);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, String>> executions(String json)
    {
        return (List<Map<String, String>>) (List<?>) JsonUtils.fromJsonList(json);
    }

    private static String display(Map<String, String> execution, String field)
    {
        String value = execution.get(field);
        if (value == null)
            return "";

        if (FIELD_STARTED_AT.equals(field) || FIELD_COMPLETED_AT.equals(field))
            return Instant.ofEpochMilli(Long.parseLong(value)).toString();

        return value;
    }
}

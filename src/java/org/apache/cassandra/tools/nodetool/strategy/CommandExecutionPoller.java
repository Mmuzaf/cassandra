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

package org.apache.cassandra.tools.nodetool.strategy;

import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.management.api.ExecutionErrorType;
import org.apache.cassandra.management.api.ExecutionStatus;

import picocli.CommandLine;

import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_ERROR_TYPE;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_OUTPUT;
import static org.apache.cassandra.management.CommandInvokerServiceMBean.FIELD_STATUS;

/**
 * Blocks nodetool on a command the server has already started, streaming the output it produced so far.
 * Keeps nodetool as blocking and as verbose as it was when the server ran the command inline, while the
 * server thread is free.
 */
class CommandExecutionPoller
{
    private static final String RUNNING_STATUS = ExecutionStatus.RUNNING.name();
    private static final String FAILED_STATUS = ExecutionStatus.FAILED.name();
    private static final String VALIDATION_ERROR = ExecutionErrorType.VALIDATION.name();
    private static final String AUTHORIZATION_ERROR = ExecutionErrorType.AUTHORIZATION.name();
    private static final long POLL_INTERVAL_MILLIS = 1000;

    private CommandExecutionPoller()
    {
    }

    /**
     * @param poll reads the execution record as named fields, or returns null when the record is gone.
     *             It raises {@link ConnectionLostException} when the node stops answering.
     */
    static void await(CommandLine commandLine,
                      String commandName,
                      UUID executionId,
                      Supplier<Map<String, String>> poll)
    {
        PrintWriter out = commandLine.getOut();
        int printedChars = 0;
        Map<String, String> terminal = null;

        while (true)
        {
            Map<String, String> record;
            try
            {
                record = poll.get();
            }
            catch (ConnectionLostException e)
            {
                throw new CommandLine.ExecutionException(commandLine,
                                                         String.format("The node stopped answering while '%s' was running. " +
                                                                       "Check the server log for the final status (executionId: %s).",
                                                                       commandName, executionId),
                                                         e);
            }

            if (record == null && terminal != null)
                record = terminal;

            if (record == null)
                throw new CommandLine.ExecutionException(commandLine,
                                                         String.format("No execution record for command '%s' (executionId: %s). " +
                                                                       "It may have been evicted from the execution history. " +
                                                                       "Run 'nodetool commandexecutions %s' once the node is reachable.",
                                                                       commandName, executionId, executionId));

            String status = record.get(FIELD_STATUS);
            String output = record.get(FIELD_OUTPUT);

            if (output != null && output.length() > printedChars)
            {
                out.print(output.substring(printedChars));
                out.flush();
                printedChars = output.length();
            }

            if (terminal != null)
            {
                if (FAILED_STATUS.equals(terminal.get(FIELD_STATUS)))
                    throw failure(commandLine, executionId, terminal.get(FIELD_ERROR), terminal.get(FIELD_ERROR_TYPE));
                return;
            }

            if (!RUNNING_STATUS.equals(status))
            {
                terminal = record;
                continue;
            }

            Uninterruptibles.sleepUninterruptibly(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private static RuntimeException failure(CommandLine commandLine, UUID executionId, String error, String errorType)
    {
        if (VALIDATION_ERROR.equals(errorType))
            return new CommandLine.ParameterException(commandLine, "Invalid request: " + error);
        if (AUTHORIZATION_ERROR.equals(errorType))
            return new CommandLine.ExecutionException(commandLine, "Unauthorized: " + error);

        return new CommandLine.ExecutionException(commandLine,
                                                  String.format("Command execution failed (executionId: %s): %s",
                                                                executionId, error));
    }

    static class ConnectionLostException extends RuntimeException
    {
        ConnectionLostException(Throwable cause)
        {
            super(cause);
        }
    }
}

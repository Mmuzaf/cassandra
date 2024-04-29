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

import java.io.PrintWriter;
import java.util.Map;

import com.google.common.base.Preconditions;

import picocli.CommandLine;

@CommandLine.Command(name = "help",
    header = "Display help information about the specified command.",
    helpCommand = true,
    description = { "%nWhen no COMMAND is given, the usage help for the main command is displayed.",
                    "If a COMMAND is specified, the help for that command is shown.%n" })
public class CassandraHelpCommand implements CommandLine.IHelpCommandInitializable2, Runnable
{
    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, descriptionKey = "helpCommand.help",
        description = "Show usage help for the help command and exit.")
    private boolean helpRequested;

    @CommandLine.Parameters(paramLabel = "command", arity = "0..1", descriptionKey = "helpCommand.command",
        description = "The COMMAND to display the usage help message for.")
    private String commands;

    private CommandLine self;
    private PrintWriter out;
    private CommandLine.Help.ColorScheme colorScheme;

    /**
     * Invokes {@code #usage(PrintStream, CommandLine.Help.ColorScheme) usage} for the specified command,
     * or for the parent command.
     */
    public void run()
    {
        CommandLine parent = self == null ? null : self.getParent();
        if (parent == null)
            return;

        CommandLine.Help.ColorScheme colors = colorScheme == null ?
                                              CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO) :
                                              colorScheme;
        // If no input command argument is specified, print help for the top-level command.
        if (commands == null)
        {
            printUsage(parent, colors, out);
            return;
        }

        Map<String, CommandLine> parentSubcommands = parent.getCommandSpec().subcommands();
        CommandLine subcommand = parentSubcommands.get(commands);

        if (parent.isAbbreviatedSubcommandsAllowed())
            throw new CommandLine.ParameterException(parent, "Abbreviated subcommands are not allowed.", null, commands);

        if (subcommand == null)
            throw new CommandLine.ParameterException(parent, "Unknown subcommand '" + commands + "'.", null, commands);

        printUsage(subcommand, colors, out);
    }

    private static void printUsage(CommandLine command, CommandLine.Help.ColorScheme colors, PrintWriter out)
    {
        if (command == null)
            return;
        command.usage(out, colors);
    }

    /**
     * The printHelpIfRequested method calls the init method on commands marked
     * as helpCommand before the help command's run or call method is called.
     */
    public void init(CommandLine helpCommandLine,
                     CommandLine.Help.ColorScheme colorScheme,
                     PrintWriter out,
                     PrintWriter err)
    {
        this.self = Preconditions.checkNotNull(helpCommandLine, "helpCommandLine");
        this.colorScheme = Preconditions.checkNotNull(colorScheme, "colorScheme");
        this.out = Preconditions.checkNotNull(out, "outWriter");
    }
}

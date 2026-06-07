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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;

/**
 * Generates the raw {@code nodetool help} text files consumed by {@code doc/scripts/gen-nodetool-docs.py}.
 * <p>
 * All help output is produced from a single JVM (one {@code help} invocation per command, in-process), instead of
 * starting one JVM per command via {@code bin/nodetool}. It is run by the {@code gen-nodetool-text} Ant target, which
 * supplies the classpath and the JVM options nodetool needs (jamm javaagent, module {@code --add-opens}, etc.).
 * <p>
 * Usage: {@code java -cp <cassandra-classpath> NodetoolHelpDump.java <output-dir>}
 * <p>
 * For every top-level command {@code <cmd>} a file {@code <output-dir>/<cmd>.txt} is written with the output of
 * {@code nodetool help <cmd>}. The top-level {@code nodetool help} listing is written to
 * {@code <output-dir>/nodetool.txt}.
 */
public final class NodetoolHelpDump
{
    private static final String SEPARATOR = "$";
    private static final String ROOT_FILE = "nodetool.txt";

    /**
     * {@code nodetool help} never opens a JMX connection, so a no-op probe factory is sufficient and lets us avoid the
     * package-private {@code NodeProbeFactory} from {@code org.apache.cassandra.tools}.
     */
    private static final INodeProbeFactory NO_PROBE = new INodeProbeFactory()
    {
        public NodeProbe create(String host, int port) throws IOException
        {
            throw new UnsupportedOperationException("nodetool help does not connect to a node");
        }

        public NodeProbe create(String host, int port, String username, String password) throws IOException
        {
            throw new UnsupportedOperationException("nodetool help does not connect to a node");
        }
    };

    public static void main(String[] args) throws IOException
    {
        if (args.length < 1)
        {
            System.err.println("usage: NodetoolHelpDump <output-dir>");
            System.exit(2);
        }

        Path outDir = Paths.get(args[0]);
        Files.createDirectories(outDir);

        List<String> commands = new ArrayList<>();
        commands.add(""); // top-level "nodetool help" listing -> nodetool.txt
        for (String command : NodeTool.getCommandsWithoutRoot(SEPARATOR))
        {
            // Only top-level commands are documented; subcommands are reported with the separator in their name.
            if (!command.contains(SEPARATOR))
                commands.add(command);
        }

        for (String command : commands)
        {
            String help = captureHelp(command);
            String fileName = command.isEmpty() ? ROOT_FILE : command + ".txt";
            Files.write(outDir.resolve(fileName), help.getBytes(StandardCharsets.UTF_8));
        }

        System.out.println("Generated " + commands.size() + " nodetool help text files into " + outDir.toAbsolutePath());
    }

    private static String captureHelp(String command)
    {
        // Capture stdout and stderr separately: only the help text (stdout) is documented.
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBuffer, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(errBuffer, true, StandardCharsets.UTF_8);
        String[] helpArgs = command.isEmpty() ? new String[]{ "help" }
                                              : new String[]{ "help", command };
        int rc = new NodeTool(NO_PROBE, new Output(out, err)).execute(helpArgs);
        out.flush();
        err.flush();

        String errText = errBuffer.toString(StandardCharsets.UTF_8);
        if (!errText.isEmpty())
            System.err.print(errText);
        if (rc != 0)
            throw new RuntimeException("Failed to generate help for command '" + command + "', exit code " + rc);

        String help = outBuffer.toString(StandardCharsets.UTF_8);
        // Ensure a trailing newline so the generated files end consistently.
        return help.endsWith("\n") ? help : help + "\n";
    }
}

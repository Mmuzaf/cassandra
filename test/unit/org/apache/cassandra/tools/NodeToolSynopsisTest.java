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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;

public class NodeToolSynopsisTest
{
    @Test
    public void cliHelp()
    {
        List<String> outNodeTool = invokeNodetool(NodeTool::new, "help");
        List<String> outNodeToolV2 = invokeNodetool(NodeToolV2::new, "help");

        String diff = computeDiff(outNodeTool, outNodeToolV2);
        assertTrue(concatNodetoolOutput(outNodeTool) +
                   '\n' + "-----------------------------------------------------" +
                   '\n' + concatNodetoolOutput(outNodeToolV2) +
                   '\n' + "Difference for \"" + "help" + "\":" + diff,
                   StringUtils.isBlank(diff));
    }

    @Test
    public void dummy()
    {
        List<String> outNodeToolV2 = invokeNodetool(NodeToolV2::new, "help");
        System.out.println(concatNodetoolOutput(outNodeToolV2));
    }

    @Test
    public void compareNodeToolHelpOutput() throws Exception
    {
//        runCommandHelpOutputComparison("abortbootstrap");
        runCommandHelpOutputComparison("assassinate");
        runCommandHelpOutputComparison("forcecompact");
        runCommandHelpOutputComparison("compact");
    }

    public void runCommandHelpOutputComparison(String commandName)
    {
        List<String> outNodeTool = invokeNodetool(NodeTool::new, "help", commandName);
        List<String> outNodeToolV2 = invokeNodetool(NodeToolV2::new, "help", commandName);
        String diff = computeDiff(outNodeTool, outNodeToolV2);
        assertTrue(concatNodetoolOutput(outNodeTool) +
                   '\n' + "-----------------------------------------------------" +
                   '\n' + concatNodetoolOutput(outNodeToolV2) +
                   '\n' + " difference for \"" + commandName + "\":" + diff,
                   StringUtils.isBlank(diff));
    }

    private static String concatNodetoolOutput(List<String> output)
    {
        return '\n' + String.join("\n", output);
    }

    public static List<String> invokeNodetool(BiFunction<NodeProbeFactory, Output, Object> factory, String... commands)
    {
        ListOutputStream output = new ListOutputStream();
        List<String> args = CQLTester.buildNodetoolArgs(List.of(commands));
        args.remove("bin/nodetool");
        try
        {
            Object runner = factory.apply(new NodeProbeFactory(), new Output(new PrintStream(output), new PrintStream(output)));
            Object result = runner.getClass().getMethod("execute", String[].class)
                  .invoke(runner, new Object[] { args.toArray(new String[0]) });
            if (result instanceof Integer && (Integer) result != 0)
                throw new RuntimeException("Command failed with exit code " + result);
            return output.getOutputLines();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String computeDiff(List<String> original, List<String> revised) {
        Patch<String> patch = DiffUtils.diff(original, revised);
        List<String> diffLines = new ArrayList<>();

        for (AbstractDelta<String> delta : patch.getDeltas()) {
            for (String line : delta.getSource().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " source: " + line);
            }
            for (String line : delta.getTarget().getLines()) {
                diffLines.add(delta.getType().toString().toLowerCase() + " target: " + line);
            }
        }

        return '\n' + String.join("\n", diffLines);
    }

    public static class ListOutputStream extends OutputStream
    {
        private final List<String> outputLines = new ArrayList<>();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void write(int b)
        {
            char c = (char) b;
            if (c == '\n')
            {
                // Add the buffer to the list if it's a new line
                outputLines.add(buffer.toString());
                buffer.setLength(0); // Clear the buffer
            }
            else
                buffer.append(c);
        }

        public void flush()
        {
            if (buffer.length() > 0)
            {
                outputLines.add(buffer.toString());
                buffer.setLength(0);
            }
        }

        public List<String> getOutputLines()
        {
            flush();
            return new ArrayList<>(outputLines);
        }

        public String getOutput()
        {
            flush();
            return String.join(System.lineSeparator(), outputLines);
        }
    }
}

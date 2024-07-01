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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;

import static org.junit.Assert.assertTrue;

public class NodeToolHelpMessageTest extends CQLToolRunnerTester
{
    @Test
    public void testCompareHelpCommand()
    {
        List<String> outNodeTool = sliceStdout(invokeNodetoolInJvmV1("help"));
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolInJvmV2("help"));

        String diff = computeDiff(outNodeTool, outNodeToolV2);
        assertTrue(printFormattedDiffsMessage(outNodeTool, outNodeToolV2, "help", diff),
                   StringUtils.isBlank(diff));
    }

    @Test
    public void testBaseCommandOutput()
    {
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolInJvmV1("help", "forcecompact"));
        System.out.println(printFormattedNodeToolOutput(outNodeToolV2));
    }

    @Test
    public void testCompareCommandHelpOutputBetweenTools()
    {
        runCommandHelpOutputComparison("abortbootstrap");
        runCommandHelpOutputComparison("assassinate");
        runCommandHelpOutputComparison("forcecompact");
        runCommandHelpOutputComparison("compact");
    }

    public void runCommandHelpOutputComparison(String commandName)
    {
        List<String> outNodeTool = sliceStdout(invokeNodetoolInJvmV1("help", commandName));
        List<String> outNodeToolV2 = sliceStdout(invokeNodetoolInJvmV2("help", commandName));
        String diff = computeDiff(outNodeTool, outNodeToolV2);
        assertTrue(printFormattedDiffsMessage(outNodeTool, outNodeToolV2, commandName, diff),
                   StringUtils.isBlank(diff));
    }

    private static String printFormattedDiffsMessage(List<String> stdoutOrig,
                                                     List<String> stdoutNew,
                                                     String commandName,
                                                     String diff)
    {
        return '\n' + "> NodeTool" + '\n' +
               printFormattedNodeToolOutput(stdoutOrig) +
               '\n' + "> NodeToolV2" +
               '\n' + printFormattedNodeToolOutput(stdoutNew) +
               '\n' + " difference for \"" + commandName + "\":" + diff;
    }

    private static String printFormattedNodeToolOutput(List<String> output)
    {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < output.size(); i++)
        {
            sb.append(i).append(':').append(output.get(i));
            if(i < output.size() - 1)
                sb.append('\n');
        }
        return sb.toString();
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
}

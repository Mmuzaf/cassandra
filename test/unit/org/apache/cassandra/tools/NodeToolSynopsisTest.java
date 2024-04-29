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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;

public class NodeToolSynopsisTest
{
    @Test
    public void cliHelp()
    {
        ToolResult toolHistory = invokeNodetool("help", "assassinate");
        toolHistory.assertOnCleanExit();
        System.out.println(toolHistory.getStdout());
    }

    @Test
    public void cliHelpDiff()
    {
        ToolResult result = nodetool("help", "diff");
        result.assertOnCleanExit();

        Assertions.assertThat(result.getStdout())
                  .isEqualTo("NAME\n" +
                             "        jmxtool diff - Diff two jmx dump files and report their differences\n" +
                             "\n" +
                             "SYNOPSIS\n" +
                             "        jmxtool diff [--exclude-attribute <exclude attributes>...]\n" +
                             "                [--exclude-object <exclude objects>...]\n" +
                             "                [--exclude-operation <exclude operations>...]\n" +
                             "                [(-f <format> | --format <format>)] [(-h | --help)]\n" +
                             "                [--ignore-missing-on-left] [--ignore-missing-on-right] [--] <left>\n" +
                             "                <right>\n" +
                             "\n" +
                             "OPTIONS\n" +
                             "        --exclude-attribute <exclude attributes>\n" +
                             "            Ignores processing specific attributes. Each usage should take a\n" +
                             "            single attribute, but can use this flag multiple times.\n" +
                             "\n" +
                             "        --exclude-object <exclude objects>\n" +
                             "            Ignores processing specific objects. Each usage should take a single\n" +
                             "            object, but can use this flag multiple times.\n" +
                             "\n" +
                             "        --exclude-operation <exclude operations>\n" +
                             "            Ignores processing specific operations. Each usage should take a\n" +
                             "            single operation, but can use this flag multiple times.\n" +
                             "\n" +
                             "        -f <format>, --format <format>\n" +
                             "            What format the files are in; only support json and yaml as format\n" +
                             "\n" +
                             "        -h, --help\n" +
                             "            Display help information\n" +
                             "\n" +
                             "        --ignore-missing-on-left\n" +
                             "            Ignore results missing on the left\n" +
                             "\n" +
                             "        --ignore-missing-on-right\n" +
                             "            Ignore results missing on the right\n" +
                             "\n" +
                             "        --\n" +
                             "            This option can be used to separate command-line options from the\n" +
                             "            list of argument, (useful when arguments might be mistaken for\n" +
                             "            command-line options\n" +
                             "\n" +
                             "        <left> <right>\n" +
                             "            Files to diff\n" +
                             "\n" +
                             "\n");
    }

    private static ToolResult nodetool(String... args)
    {
        List<String> cmd = new ArrayList<>(1 + args.length);
        cmd.add("bin/nodetool");
        cmd.addAll(Arrays.asList(args));
        return ToolRunner.invoke(cmd);
    }
}

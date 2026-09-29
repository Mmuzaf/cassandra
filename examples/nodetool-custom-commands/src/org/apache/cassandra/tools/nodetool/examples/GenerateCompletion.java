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

package org.apache.cassandra.tools.nodetool.examples;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.LocalCommand;

import picocli.AutoComplete;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/** Completion for the command tree nodetool has at run time, so commands added by plugins are included. */
@Command(name = "generate-completion",
         description = "Print a bash and zsh completion script for nodetool, including commands added by " +
                       "plugins. Load it with: source <(nodetool generate-completion)")
public class GenerateCompletion extends AbstractCommand implements LocalCommand
{
    @Spec
    private CommandSpec spec;

    @Override
    public boolean shouldConnect()
    {
        return false;
    }

    @Override
    protected void execute(NodeProbe probe)
    {
        output.out.print(AutoComplete.bash(spec.root().name(), spec.root().commandLine()));
    }
}

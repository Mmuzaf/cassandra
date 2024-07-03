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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.management.BaseCommand;
import org.apache.cassandra.management.CassandraCliArgument;
import org.apache.cassandra.management.ServiceMBeanBridge;
import picocli.CommandLine;

import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.parseOptionalKeyspace;
import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.parseOptionalTables;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@CommandLine.Command(name = "compact", description = "Force a (major) compaction on one or more tables or user-defined compaction on given SSTables")
public class Compact extends BaseCommand
{
    @CassandraCliArgument(usage = "[<keyspace> <tables>...] or <SSTable file>...",
        description = "The keyspace followed by one or many tables or list of SSTable data files when using --user-defined")
    public List<String> args = new ArrayList<>();
    @CommandLine.ArgGroup
    public TablesArgs tablesArgs = new TablesArgs();
    @CommandLine.ArgGroup
    public UserDefinedArgs userDefinedArgs = new UserDefinedArgs();

    public static class TablesArgs
    {
        @CommandLine.Option(names = { "-st", "--start-token"},
            description = "Use -st to specify a token at which the compaction range starts (inclusive)")
        public String startToken = EMPTY;
        @CommandLine.Option(names = { "-et", "--end-token"},
            description = "Use -et to specify a token at which compaction range ends (inclusive)")
        public String endToken = EMPTY;
        @CommandLine.Option(names = { "--partition"},
            description = "String representation of the partition key")
        public String partitionKey = EMPTY;
        @CommandLine.Parameters(index = "0", arity = "1", description = "The keyspace followed by one or many tables")
        public String keyspace;
        @CommandLine.Parameters(index = "1..*", arity = "1", description = "The tables to be compacted")
        public String[] tables;
    }

    public static class UserDefinedArgs
    {
        @CommandLine.Option(names = { "--user-defined" }, description = "Use --user-defined to submit listed files for user-defined compaction")
        public boolean userDefined = false;
        @CommandLine.Parameters(index = "0..*", arity = "1", description = "The SSTable files to be compacted")
        public String[] sstableFiles;
    }

    @CommandLine.Option(names = { "-s", "--split-output"}, description = "Use -s to not create a single big file")
    public boolean splitOutput = false;

    @Override
    public void execute(ServiceMBeanBridge probe)
    {
        final boolean userDefined = userDefinedArgs.userDefined;
        final String startToken = tablesArgs.startToken;
        final String endToken = tablesArgs.endToken;
        final String partitionKey = tablesArgs.partitionKey;
        args = userDefined ? List.of(userDefinedArgs.sstableFiles) : Lists.asList(tablesArgs.keyspace, tablesArgs.tables);
        final boolean startEndTokenProvided = !(startToken.isEmpty() && endToken.isEmpty());
        final boolean partitionKeyProvided = !partitionKey.isEmpty();
        final boolean tokenProvided = startEndTokenProvided || partitionKeyProvided;
        if (splitOutput && (userDefined || tokenProvided))
        {
            throw new RuntimeException("Invalid option combination: Can not use split-output here");
        }
        if (userDefined && tokenProvided)
        {
            throw new RuntimeException("Invalid option combination: Can not provide tokens when using user-defined");
        }

        if (userDefined)
        {
            try
            {
                String userDefinedFiles = String.join(",", args);
                probe.cmProxy().forceUserDefinedCompaction(userDefinedFiles);
            } catch (Exception e) {
                throw new RuntimeException("Error occurred during user defined compaction", e);
            }
            return;
        }

        List<String> keyspaces = parseOptionalKeyspace(args, probe);
        String[] tableNames = parseOptionalTables(args);

        for (String keyspace : keyspaces)
        {
            try
            {
                if (startEndTokenProvided)
                {
                    probe.ssProxy().forceKeyspaceCompactionForTokenRange(keyspace, startToken, endToken, tableNames);
                }
                else if (partitionKeyProvided)
                {
                    probe.ssProxy().forceKeyspaceCompactionForPartitionKey(keyspace, partitionKey, tableNames);
                }
                else
                {
                    probe.ssProxy().forceKeyspaceCompaction(splitOutput, keyspace, tableNames);
                }
            } catch (Exception e)
            {
                throw new RuntimeException("Error occurred during compaction", e);
            }
        }
    }
}

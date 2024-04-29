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
import java.util.List;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.management.api.CommandContext;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageServiceMBean;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command to trigger the immediate cleanup of keys no longer belonging to a node.
 * This command is equivalent to {@code nodetool cleanup}.
 */
@Command(name = "cleanup",
    description = "Triggers the immediate cleanup of keys no longer belonging to a node. " +
                  "By default, clean all keyspaces")
public class CleanupCommand extends BaseCommand implements Runnable
{
    @Parameters(description = "The keyspaces followed by one or many tables")
    private String keyspace;
    @Parameters(arity = "0..*", description = "The tables to cleanup")
    private String[] tables;
    @Option(names = { "-j", "--jobs"}, description = "Number of sstables to cleanup simultanously, " +
                                                                 "set to 0 to use all available compaction threads")
    private int jobs = 2;

    @Inject
    public CommandContext context;

    @Override
    public void run()
    {
        StorageServiceMBean ssProxy = context.findMBean(StorageServiceMBean.class);
        List<String> keyspaces0 = keyspace == null ? ssProxy.getNonLocalStrategyKeyspaces() : List.of(keyspace);
        String[] tables0 = tables == null ? ArrayUtils.EMPTY_STRING_ARRAY : tables;

        for (String keyspace : keyspaces0)
        {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace))
                continue;

            try
            {
                forceKeyspaceCleanup(context.output().out, jobs, keyspace, tables0);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error occurred during cleanup", e);
            }
        }
    }

    public void forceKeyspaceCleanup(PrintStream out, int jobs, String keyspaceName, String... tableNames) throws Exception
    {
        StorageServiceMBean ssProxy = context.findMBean(StorageServiceMBean.class);
        CommandUtils.checkJobs(out, ssProxy, jobs);
        perform(out, keyspaceName,
                () -> ssProxy.forceKeyspaceCleanup(jobs, keyspaceName, tableNames),
                "cleaning up");
    }

    private void perform(PrintStream out, String ks, Callable<Integer> job, String jobName) throws Exception
    {
        switch (job.call())
        {
            case 1:
                out.printf("Aborted %s for at least one table in keyspace %s, check server logs for more information.\n",
                           jobName, ks);
                break;
            case 2:
                out.printf("Failed marking some sstables compacting in keyspace %s, check server logs for more information.\n",
                           ks);
        }
    }
}

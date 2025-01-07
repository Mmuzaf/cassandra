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

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import static org.apache.cassandra.tools.nodetool.Help.printTopCommandUsage;

@Command(name = "nodetool",
         description = "Manage your Cassandra cluster",
         subcommands = { Help.class,
                         AbortBootstrap.class,
                         Assassinate.class,
                         Bootstrap.class,
                         CIDRFilteringStats.class,
                         Cleanup.class,
                         ClearSnapshot.class,
                         ClientStats.class,
                         Compact.class,
                         CompactionHistory.class,
                         CompactionStats.class,
                         DataPaths.class,
                         Decommission.class,
                         DescribeCluster.class,
                         DescribeRing.class,
                         DisableAuditLog.class,
                         DisableAutoCompaction.class,
                         DisableBackup.class,
                         DisableBinary.class,
                         DisableFullQueryLog.class,
                         DisableGossip.class,
                         DisableHandoff.class,
                         DisableHintsForDC.class,
                         DisableOldProtocolVersions.class,
                         Drain.class,
                         DropCIDRGroup.class,
                         EnableAuditLog.class,
                         EnableAutoCompaction.class,
                         EnableBackup.class,
                         EnableBinary.class,
                         EnableFullQueryLog.class,
                         EnableGossip.class,
                         EnableHandoff.class,
                         EnableHintsForDC.class,
                         EnableOldProtocolVersions.class,
                         FailureDetectorInfo.class,
                         Flush.class,
                         ForceCompact.class,
                         GarbageCollect.class,
                         GcStats.class,
                         GetAuditLog.class,
                         GetAuthCacheConfig.class,
                         GetBatchlogReplayTrottle.class,
                         GetCIDRGroupsOfIP.class,
                         GetColumnIndexSize.class,
                         GetCompactionThreshold.class,
                         GetCompactionThroughput.class,
                         GetConcurrency.class,
                         GetConcurrentCompactors.class,
                         GetConcurrentViewBuilders.class,
                         GetDefaultKeyspaceRF.class,
                         GetEndpoints.class,
                         GetFullQueryLog.class,
                         GetInterDCStreamThroughput.class,
                         GetLoggingLevels.class,
                         GetMaxHintWindow.class,
                         GetSSTables.class,
                         GetSeeds.class,
                         GetSnapshotThrottle.class,
                         GetStreamThroughput.class,
                         GetTimeout.class,
                         GetTraceProbability.class,
                         GossipInfo.class,
                         Import.class,
                         Info.class,
                         InvalidateCIDRPermissionsCache.class,
                         InvalidateCounterCache.class,
                         InvalidateCredentialsCache.class,
                         InvalidateJmxPermissionsCache.class,
                         InvalidateKeyCache.class,
                         InvalidateNetworkPermissionsCache.class,
                         InvalidatePermissionsCache.class,
                         InvalidateRolesCache.class,
                         InvalidateRowCache.class,
                         Join.class,
                         ListCIDRGroups.class,
                         Version.class })
public class TopLevelCommand implements Runnable
{
    @Spec
    public CommandSpec spec;

    public void run()
    {
        printTopCommandUsage(spec.commandLine(),
                             spec.commandLine().getColorScheme(),
                             spec.commandLine().getOut());
    }
}

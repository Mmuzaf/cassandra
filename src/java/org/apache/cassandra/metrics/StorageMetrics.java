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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.Keyspace;

import java.util.function.ToLongFunction;
import java.util.stream.StreamSupport;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    private static final CassandraMetricsRegistry group = CassandraMetricsRegistry.getOrCreateGroup(
            new DefaultNameFactory("Storage"),
            "Metrics related to Storage.");

    public static final Counter load = group.counterMetric("Load");
    public static final Counter uncompressedLoad = group.counterMetric("UncompressedLoad");

    public static final Gauge<Long> unreplicatedLoad =
        createSummingGauge("UnreplicatedLoad", metric -> metric.unreplicatedLiveDiskSpaceUsed.getValue());
    public static final Gauge<Long> unreplicatedUncompressedLoad =
        createSummingGauge("UnreplicatedUncompressedLoad", metric -> metric.unreplicatedUncompressedLiveDiskSpaceUsed.getValue());

    public static final Counter uncaughtExceptions = group.counterMetric("Exceptions");
    public static final Counter totalHintsInProgress  = group.counterMetric("TotalHintsInProgress");
    public static final Counter totalHints = group.counterMetric("TotalHints");
    public static final Counter repairExceptions = group.counterMetric("RepairExceptions");
    public static final Counter totalOpsForInvalidToken = group.counterMetric("TotalOpsForInvalidToken");
    public static final Counter startupOpsForInvalidToken = group.counterMetric("StartupOpsForInvalidToken");

    private static Gauge<Long> createSummingGauge(String name, ToLongFunction<KeyspaceMetrics> extractor)
    {
        return group.registerMetric(name,
                                () -> StreamSupport.stream(Keyspace.all().spliterator(), false)
                                                   .mapToLong(keyspace -> extractor.applyAsLong(keyspace.metric))
                                                   .sum());
    }
}

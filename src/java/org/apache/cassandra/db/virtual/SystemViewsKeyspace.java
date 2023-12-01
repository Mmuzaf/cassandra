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
package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.db.virtual.model.CounterMetricRow;
import org.apache.cassandra.db.virtual.model.CounterMetricRowWalker;
import org.apache.cassandra.db.virtual.model.GaugeMetricRow;
import org.apache.cassandra.db.virtual.model.GaugeMetricRowWalker;
import org.apache.cassandra.db.virtual.model.HistogramMetricRow;
import org.apache.cassandra.db.virtual.model.HistogramMetricRowWalker;
import org.apache.cassandra.db.virtual.model.MeterMetricRow;
import org.apache.cassandra.db.virtual.model.MeterMetricRowWalker;
import org.apache.cassandra.db.virtual.model.MetricGroupRow;
import org.apache.cassandra.db.virtual.model.MetricGroupRowWalker;
import org.apache.cassandra.db.virtual.model.MetricRow;
import org.apache.cassandra.db.virtual.model.MetricRowWalker;
import org.apache.cassandra.db.virtual.model.TimerMetricRow;
import org.apache.cassandra.db.virtual.model.TimerMetricRowWalker;
import org.apache.cassandra.db.virtual.sysview.SystemViewCollectionAdapter;
import org.apache.cassandra.index.sai.virtual.StorageAttachedIndexTables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public final class SystemViewsKeyspace extends VirtualKeyspace
{
    private static final UnaryOperator<String> GROUP_METRICS_OPERATOR = name ->
            (name.chars().allMatch(Character::isUpperCase) ? name.toLowerCase() : name) + "GroupMetrics";
    private static final UnaryOperator<String> TYPE_METRICS_OPERATOR = name -> name + "TypeMetrics";

    public static SystemViewsKeyspace instance = new SystemViewsKeyspace();

    private SystemViewsKeyspace()
    {
        super(VIRTUAL_VIEWS, new ImmutableList.Builder<VirtualTable>()
                    .add(new CachesTable(VIRTUAL_VIEWS))
                    .add(new ClientsTable(VIRTUAL_VIEWS))
                    .add(new SettingsTable(VIRTUAL_VIEWS))
                    .add(new SystemPropertiesTable(VIRTUAL_VIEWS))
                    .add(new SSTableTasksTable(VIRTUAL_VIEWS))
                    .add(new ThreadPoolsTable(VIRTUAL_VIEWS))
                    .add(new InternodeOutboundTable(VIRTUAL_VIEWS))
                    .add(new InternodeInboundTable(VIRTUAL_VIEWS))
                    .add(new PendingHintsTable(VIRTUAL_VIEWS))
                    .addAll(TableMetricTables.getAll(VIRTUAL_VIEWS))
                    .add(new CredentialsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new JmxPermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new NetworkPermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new PermissionsCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new RolesCacheKeysTable(VIRTUAL_VIEWS))
                    .add(new CQLMetricsTable(VIRTUAL_VIEWS))
                    .add(new BatchMetricsTable(VIRTUAL_VIEWS))
                    .add(new StreamingVirtualTable(VIRTUAL_VIEWS))
                    .add(new GossipInfoTable(VIRTUAL_VIEWS))
                    .add(new QueriesTable(VIRTUAL_VIEWS))
                    .add(new LogMessagesTable(VIRTUAL_VIEWS))
                    .add(new SnapshotsTable(VIRTUAL_VIEWS))
                    .add(new PeersTable(VIRTUAL_VIEWS))
                    .add(new LocalTable(VIRTUAL_VIEWS))
                    .add(new ClusterMetadataLogTable(VIRTUAL_VIEWS))
                    .addAll(LocalRepairTables.getAll(VIRTUAL_VIEWS))
                    .addAll(CIDRFilteringMetricsTable.getAll(VIRTUAL_VIEWS))
                    .addAll(StorageAttachedIndexTables.getAll(VIRTUAL_VIEWS))
                    .addAll(dynamicVirtualTables())
                    .build());
    }

    private static Collection<VirtualTable> dynamicVirtualTables()
    {
        List<VirtualTable> tables = new ArrayList<>();
        // Register virtual tables for all metrics groups.
        Metrics.getMetricGroups().forEach((name, group) -> {
            tables.add(new VirtualTableSystemViewAdapter<>(
                    SystemViewCollectionAdapter.create(name,
                            "Metrics virtual table for '" + name + "' group.",
                            new MetricRowWalker(),
                            group.getMetrics().entrySet(),
                            MetricRow::new),
                    GROUP_METRICS_OPERATOR
            ));
        });
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("metricGroups",
                        "Metric groups",
                        new MetricGroupRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        MetricGroupRow::new),
                UnaryOperator.identity()));
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("counter",
                        "Counter metrics virtual table",
                        new CounterMetricRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        e -> e.getValue().getCounters().entrySet(),
                        (groupEntry, counter) -> new CounterMetricRow(groupEntry.getKey(), counter)),
                TYPE_METRICS_OPERATOR));
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("gauge",
                        "Gauge metrics virtual table",
                        new GaugeMetricRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        e -> e.getValue().getGauges().entrySet(),
                        (groupEntry, gauge) -> new GaugeMetricRow(groupEntry.getKey(), gauge)),
                TYPE_METRICS_OPERATOR));
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("histogram",
                        "Histogram metrics virtual table",
                        new HistogramMetricRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        e -> e.getValue().getHistograms().entrySet(),
                        (groupEntry, histogram) -> new HistogramMetricRow(groupEntry.getKey(), histogram)),
                TYPE_METRICS_OPERATOR));
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("meter",
                        "Meter metrics virtual table",
                        new MeterMetricRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        e -> e.getValue().getMeters().entrySet(),
                        (groupEntry, meter) -> new MeterMetricRow(groupEntry.getKey(), meter)),
                TYPE_METRICS_OPERATOR));
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create("timer",
                        "Timer metrics virtual table",
                        new TimerMetricRowWalker(),
                        Metrics.getMetricGroups().entrySet(),
                        e -> e.getValue().getTimers().entrySet(),
                        (groupEntry, timer) -> new TimerMetricRow(groupEntry.getKey(), timer)),
                TYPE_METRICS_OPERATOR));
        return tables;
    }
}

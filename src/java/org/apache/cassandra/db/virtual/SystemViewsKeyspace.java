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
import org.apache.cassandra.db.virtual.model.ThreadPoolRow;
import org.apache.cassandra.db.virtual.model.ThreadPoolRowWalker;
import org.apache.cassandra.db.virtual.model.TimerMetricRow;
import org.apache.cassandra.db.virtual.model.TimerMetricRowWalker;
import org.apache.cassandra.db.virtual.sysview.SystemViewCollectionAdapter;
import org.apache.cassandra.index.sai.virtual.StorageAttachedIndexTables;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public final class SystemViewsKeyspace extends VirtualKeyspace
{
    public static final String METRICS_PREFIX = "metrics_";
    public static final UnaryOperator<String> GROUP_NAME_MAPPER = name -> METRICS_PREFIX + name;
    public static final UnaryOperator<String> TYPE_NAME_MAPPER = name -> METRICS_PREFIX + "type_" + name;

    private SystemViewsKeyspace(Collection<VirtualTable> tables)
    {
        super(VIRTUAL_VIEWS, tables);
    }

    public static SystemViewsKeyspace defaults()
    {
        return builder()
                .add(new CachesTable(VIRTUAL_VIEWS))
                .add(new ClientsTable(VIRTUAL_VIEWS))
                .add(new SettingsTable(VIRTUAL_VIEWS))
                .add(new SystemPropertiesTable(VIRTUAL_VIEWS))
                .add(new SSTableTasksTable(VIRTUAL_VIEWS))
                // Fully backward/forward compatible with the legace ThreadPoolsTable under the same "system_views.thread_pools" name.
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("thread_pools",
                                "Thread pool metrics for all thread pools",
                                new ThreadPoolRowWalker(),
                                Metrics::allThreadPoolMetrics,
                                ThreadPoolRow::new),
                        UnaryOperator.identity()
                ))
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
                // Register virtual tables for all known metric group names.
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("all_group_names",
                                "All metric group names",
                                new MetricGroupRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                MetricGroupRow::new),
                        GROUP_NAME_MAPPER))
                // Register virtual tables for all metrics types similar to the JMX MBean structure,
                // e.g.: HistogramJmxMBean, MeterJmxMBean, etc.
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("counter",
                                "All metrics with type \"Counter\"",
                                new CounterMetricRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                e -> e.getValue().getCounters().entrySet(),
                                (groupEntry, counter) -> new CounterMetricRow(groupEntry.getKey(), counter)),
                        TYPE_NAME_MAPPER))
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("gauge",
                                "All metrics with type \"Gauge\"",
                                new GaugeMetricRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                e -> e.getValue().getGauges().entrySet(),
                                (groupEntry, gauge) -> new GaugeMetricRow(groupEntry.getKey(), gauge)),
                        TYPE_NAME_MAPPER))
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("histogram",
                                "All metrics with type \"Histogram\"",
                                new HistogramMetricRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                e -> e.getValue().getHistograms().entrySet(),
                                (groupEntry, histogram) -> new HistogramMetricRow(groupEntry.getKey(), histogram)),
                        TYPE_NAME_MAPPER))
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("meter",
                                "All metrics with type \"Meter\"",
                                new MeterMetricRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                e -> e.getValue().getMeters().entrySet(),
                                (groupEntry, meter) -> new MeterMetricRow(groupEntry.getKey(), meter)),
                        TYPE_NAME_MAPPER))
                .add(new VirtualTableSystemViewAdapter<>(
                        SystemViewCollectionAdapter.create("timer",
                                "All metrics with type \"Timer\"",
                                new TimerMetricRowWalker(),
                                () -> Metrics.getRegisters().entrySet(),
                                e -> e.getValue().getTimers().entrySet(),
                                (groupEntry, timer) -> new TimerMetricRow(groupEntry.getKey(), timer)),
                        TYPE_NAME_MAPPER))
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Map<String, VirtualTable> tables = new HashMap<>();

        public Builder add(VirtualTable table)
        {
            VirtualTable vt = tables.put(table.name(), table);
            assert vt == null : "Virtual table with name " + table.name() + " already exists";
            return this;
        }

        public Builder addAll(Collection<VirtualTable> tables)
        {
            tables.forEach(this::add);
            return this;
        }

        public SystemViewsKeyspace build()
        {
            return new SystemViewsKeyspace(Collections.unmodifiableCollection(tables.values()));
        }
    }
}

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
import org.apache.cassandra.db.virtual.model.ThreadPoolRow;
import org.apache.cassandra.db.virtual.model.ThreadPoolRowWalker;
import org.apache.cassandra.db.virtual.model.TimerMetricRow;
import org.apache.cassandra.db.virtual.model.TimerMetricRowWalker;
import org.apache.cassandra.index.sai.virtual.StorageAttachedIndexTables;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public final class SystemViewsKeyspace extends VirtualKeyspace
{
    public static final String METRICS_PREFIX = "metrics_";
    public static final UnaryOperator<String> GROUP_NAME_MAPPER = name -> METRICS_PREFIX + name;
    public static final UnaryOperator<String> TYPE_NAME_MAPPER = name -> METRICS_PREFIX + "type_" + name;
    public static SystemViewsKeyspace instance = new SystemViewsKeyspace();

    private SystemViewsKeyspace()
    {
        super(VIRTUAL_VIEWS, new ImmutableList.Builder<VirtualTable>()
                    .add(new CachesTable(VIRTUAL_VIEWS))
                    .add(new ClientsTable(VIRTUAL_VIEWS))
                    .add(new SettingsTable(VIRTUAL_VIEWS))
                    .add(new SystemPropertiesTable(VIRTUAL_VIEWS))
                    .add(new SSTableTasksTable(VIRTUAL_VIEWS))
                    // Fully backward/forward compatible with the legace ThreadPoolsTable under the same "system_views.thread_pools" name.
                    .add(CollectionVirtualTableAdapter.create("thread_pools",
                            "Thread pool metrics for all thread pools",
                            new ThreadPoolRowWalker(),
                            Metrics::allThreadPoolMetrics,
                            ThreadPoolRow::new,
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
                    .addAll(createMetricsVirtualTables())
                    .build());
    }

    private static List<VirtualTable> createMetricsVirtualTables()
    {
        return ImmutableList.<VirtualTable>builder()
                // Register virtual table of all known metric groups.
                .add(CollectionVirtualTableAdapter.create("all_group_names",
                        "All metric group names",
                        new MetricGroupRowWalker(),
                        () -> () -> Metrics.getAliases()
                                .values()
                                .stream()
                                .flatMap(Collection::stream)
                                .map(CassandraMetricsRegistry.MetricName::getSystemViewName)
                                .distinct()
                                .iterator(),
                        MetricGroupRow::new,
                        GROUP_NAME_MAPPER))
                // Register virtual tables of all metrics types similar to the JMX MBean structure,
                // e.g.: HistogramJmxMBean, MeterJmxMBean, etc.
                .add(CollectionVirtualTableAdapter.create("counter",
                        "All metrics with type \"Counter\"",
                        new CounterMetricRowWalker(),
                        () -> Metrics.getCounters().entrySet(),
                        CounterMetricRow::new,
                        TYPE_NAME_MAPPER))
                .add(CollectionVirtualTableAdapter.create("gauge",
                        "All metrics with type \"Gauge\"",
                        new GaugeMetricRowWalker(),
                        () -> Metrics.getGauges().entrySet(),
                        GaugeMetricRow::new,
                        TYPE_NAME_MAPPER))
                .add(CollectionVirtualTableAdapter.create("histogram",
                        "All metrics with type \"Histogram\"",
                        new HistogramMetricRowWalker(),
                        () -> Metrics.getHistograms().entrySet(),
                        HistogramMetricRow::new,
                        TYPE_NAME_MAPPER))
                .add(CollectionVirtualTableAdapter.create("meter",
                        "All metrics with type \"Meter\"",
                        new MeterMetricRowWalker(),
                        () -> Metrics.getMeters().entrySet(),
                        MeterMetricRow::new,
                        TYPE_NAME_MAPPER))
                .add(CollectionVirtualTableAdapter.create("timer",
                        "All metrics with type \"Timer\"",
                        new TimerMetricRowWalker(),
                        () -> Metrics.getTimers().entrySet(),
                        TimerMetricRow::new,
                        TYPE_NAME_MAPPER))
                .build();
    }

    public static String getMetricGroup(String metricName)
    {
        return Metrics.getAliases().get(metricName)
                .stream()
                .map(CassandraMetricsRegistry.MetricName::getSystemViewName)
                .findFirst()
                .orElse("unknown");
    }
}

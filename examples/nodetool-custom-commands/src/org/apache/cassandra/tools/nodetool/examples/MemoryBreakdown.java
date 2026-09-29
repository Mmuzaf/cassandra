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

import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.management.MBeanAccessor;
import org.apache.cassandra.management.MBeanAccessor.Props;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.AbstractCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static java.util.Comparator.comparingLong;

/**
 * Memory used by the node, split into on-heap and off-heap, with totals summed across tables (CASSANDRA-21447).
 * The report counts the row cache as off-heap, which holds for the default {@code OHCProvider}.
 */
@Command(name = "memorybreakdown",
         description = "Print the on-heap and off-heap memory used by memtables, SSTable structures, caches, " +
                       "buffer pools and Accord")
public class MemoryBreakdown extends AbstractCommand
{
    @Option(names = "--per-table", description = "List the tables under each per-table figure")
    private boolean perTable = false;

    @Option(names = "--bytes", description = "Print sizes in bytes")
    private boolean bytes = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        List<TableMemory> tables = tables(probe);

        Node root = new Node("Memory breakdown", "", 0);
        root.add(onHeap(probe, tables));
        root.add(offHeap(probe, tables));
        root.add(notAvailable());
        root.print(probe.output().out);
    }

    private Node onHeap(NodeProbe probe, List<TableMemory> tables)
    {
        Node section = new Node("On-heap", "", 0);
        section.add(tableFigure("memtables", tables, t -> t.memtableOnHeap));
        section.add(cache(probe, "key cache", "KeyCache"));
        section.add(cache(probe, "counter cache", "CounterCache"));
        section.add(accord(probe));

        MemoryUsage heap = probe.getHeapMemoryUsage();
        section.value = String.format("%s tracked, JVM heap used %s, max %s",
                                      size(section.childrenBytes()), size(heap.getUsed()), size(heap.getMax()));
        return section;
    }

    private Node offHeap(NodeProbe probe, List<TableMemory> tables)
    {
        Node section = new Node("Off-heap", "", 0);
        section.add(tableFigure("memtables", tables, t -> t.memtableOffHeap));
        section.add(tableFigure("bloom filters", tables, t -> t.bloomFilter));
        section.add(tableFigure("index summaries", tables, t -> t.indexSummary));
        section.add(tableFigure("compression metadata", tables, t -> t.compressionMetadata));
        section.add(cache(probe, "row cache", "RowCache"));
        section.add(bufferPool(probe, "networking buffer pool", "networking"));
        section.add(bufferPool(probe, "chunk cache buffer pool", "chunk-cache"))
               .add(cache(probe, "chunk cache", "ChunkCache"));
        section.value = size(section.childrenBytes()) + " tracked";
        return section;
    }

    private static Node notAvailable()
    {
        Node section = new Node("Not available", "", 0);
        section.add(new Node("SAI", "per-index metrics are not reachable through NodeProbe", 0));
        section.add(new Node("JVM direct buffers", "NodeProbe reads them only through getAndResetGCStats, " +
                                                   "which resets the GC counters", 0));
        return section;
    }

    private Node tableFigure(String label, List<TableMemory> tables, ToLongFunction<TableMemory> figure)
    {
        long total = tables.stream().mapToLong(figure).sum();
        Node node = new Node(label, size(total), total);
        if (perTable)
            tables.stream()
                  .filter(table -> figure.applyAsLong(table) > 0)
                  .sorted(comparingLong(figure).reversed())
                  .forEach(table -> node.add(new Node(table.name, size(figure.applyAsLong(table)), 0)));
        return node;
    }

    private Node cache(NodeProbe probe, String label, String cacheType)
    {
        Long used = metricIfRegistered(() -> probe.getCacheMetric(cacheType, "Size"));
        if (used == null)
            return new Node(label, "disabled", 0);

        long capacity = asLong(probe.getCacheMetric(cacheType, "Capacity"));
        return new Node(label, String.format("%s of %s", size(used), size(capacity)), used);
    }

    private Node bufferPool(NodeProbe probe, String label, String pool)
    {
        Long allocated = metricIfRegistered(() -> probe.getBufferPoolMetric(pool, "Size"));
        if (allocated == null)
            return new Node(label, "disabled", 0);

        long used = asLong(probe.getBufferPoolMetric(pool, "UsedSize"));
        long overflow = asLong(probe.getBufferPoolMetric(pool, "OverflowSize"));
        return new Node(label,
                        String.format("allocated %s, used %s, overflow %s", size(allocated), size(used), size(overflow)),
                        allocated + overflow);
    }

    private Node accord(NodeProbe probe)
    {
        MBeanAccessor accessor = probe.getMBeanAccessor();
        Props used = Props.metric(AccordCacheMetrics.ACCORD_CACHE, "UsedBytes");
        if (!accessor.isMBeanMetricRegistered(used))
            return new Node("Accord cache", "disabled", 0);

        long usedBytes = asLong(accessor.findMBeanGauge(used).getValue());
        long unreferenced = asLong(accessor.findMBeanGauge(Props.metric(AccordCacheMetrics.ACCORD_CACHE, "UnreferencedBytes")).getValue());
        return new Node("Accord cache", String.format("used %s, unreferenced %s", size(usedBytes), size(unreferenced)), usedBytes);
    }

    private static List<TableMemory> tables(NodeProbe probe)
    {
        List<TableMemory> tables = new ArrayList<>();
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> stores = probe.getColumnFamilyStoreMBeanProxies();
        while (stores.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> store = stores.next();
            String keyspace = store.getKey();
            String table = store.getValue().getTableName();
            tables.add(new TableMemory(keyspace + '.' + table,
                                       memtableOnHeap(probe, keyspace, table),
                                       asLong(probe.getColumnFamilyMetric(keyspace, table, "MemtableOffHeapSize")),
                                       asLong(probe.getColumnFamilyMetric(keyspace, table, "BloomFilterOffHeapMemoryUsed")),
                                       asLong(probe.getColumnFamilyMetric(keyspace, table, "IndexSummaryOffHeapMemoryUsed")),
                                       asLong(probe.getColumnFamilyMetric(keyspace, table, "CompressionMetadataOffHeapMemoryUsed"))));
        }
        return tables;
    }

    /** {@link NodeProbe#getColumnFamilyMetric} does not expose the on-heap memtable size. */
    private static long memtableOnHeap(NodeProbe probe, String keyspace, String table)
    {
        String type = table.contains(".") ? "IndexTable" : "Table";
        Props props = Props.columnFamily(type, keyspace, table, "MemtableOnHeapDataSize");
        return asLong(probe.getMBeanAccessor().findMBeanGauge(props).getValue());
    }

    private static Long metricIfRegistered(Supplier<Object> metric)
    {
        try
        {
            return asLong(metric.get());
        }
        catch (RuntimeException e)
        {
            if (Throwables.getRootCause(e) instanceof InstanceNotFoundException)
                return null;
            throw e;
        }
    }

    private static long asLong(Object value)
    {
        return ((Number) value).longValue();
    }

    private String size(long value)
    {
        return FileUtils.stringifyFileSize(value, !bytes);
    }

    private static final class TableMemory
    {
        private final String name;
        private final long memtableOnHeap;
        private final long memtableOffHeap;
        private final long bloomFilter;
        private final long indexSummary;
        private final long compressionMetadata;

        private TableMemory(String name,
                            long memtableOnHeap,
                            long memtableOffHeap,
                            long bloomFilter,
                            long indexSummary,
                            long compressionMetadata)
        {
            this.name = name;
            this.memtableOnHeap = memtableOnHeap;
            this.memtableOffHeap = memtableOffHeap;
            this.bloomFilter = bloomFilter;
            this.indexSummary = indexSummary;
            this.compressionMetadata = compressionMetadata;
        }
    }

    /** A line of the report. {@code bytes} is what the line adds to its parent's total. */
    private static final class Node
    {
        private static final int INDENT = 4;
        private static final String BRANCH = "\u251c\u2500\u2500 ";
        private static final String LAST_BRANCH = "\u2514\u2500\u2500 ";
        private static final String PIPE = "\u2502   ";
        private static final String SPACE = "    ";

        private final String label;
        private final long bytes;
        private final List<Node> children = new ArrayList<>();
        private String value;

        private Node(String label, String value, long bytes)
        {
            this.label = label;
            this.value = value;
            this.bytes = bytes;
        }

        private Node add(Node child)
        {
            children.add(child);
            return child;
        }

        private long childrenBytes()
        {
            return children.stream().mapToLong(child -> child.bytes).sum();
        }

        private void print(PrintStream out)
        {
            out.println(label);
            printChildren(out, "", labelWidth(0) + 2);
        }

        private int labelWidth(int depth)
        {
            int width = depth * INDENT + label.length();
            for (Node child : children)
                width = Math.max(width, child.labelWidth(depth + 1));
            return width;
        }

        private void printChildren(PrintStream out, String indent, int width)
        {
            for (int i = 0; i < children.size(); i++)
            {
                Node child = children.get(i);
                boolean last = i == children.size() - 1;
                String line = indent + (last ? LAST_BRANCH : BRANCH) + child.label;
                out.println(child.value.isEmpty() ? line : String.format("%-" + width + "s%s", line, child.value));
                child.printChildren(out, indent + (last ? SPACE : PIPE), width);
            }
        }
    }
}

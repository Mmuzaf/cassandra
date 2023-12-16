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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.proc.Column;
import org.apache.cassandra.db.virtual.proc.RowWalker;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.btree.BTree;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;
import static org.apache.cassandra.utils.FBUtilities.camelToSnake;

/**
 * This is a virtual table that iteratively builds rows using a data set provided by internal collection.
 * Some metric views might be too large to fit in memory, for example, virtual tables that contain metrics
 * for all the keyspaces registered in the cluster. Such a technique is also facilitates keeping the low
 * memory footprint of the virtual tables in general.
 * <p>
 * It doesn't require the input data set to be sorted, but it does require that the partition keys are
 * provided in the order of the partitioner of the table metadata.
 */
public class CollectionVirtualTableAdapter<R> implements VirtualTable
{
    private static final Pattern ONLY_ALPHABET_PATTERN = Pattern.compile("[^a-zA-Z1-9]");
    private static final List<Pair<String, String>> knownAbbreviations = Arrays.asList(Pair.create("CAS", "Cas"),
            Pair.create("CIDR", "Cidr"));
    private static final Map<Class<?>, ? extends AbstractType<?>> converters = ImmutableMap.<Class<?>, AbstractType<?>>builder()
            .put(String.class, UTF8Type.instance)
            .put(Integer.class, Int32Type.instance)
            .put(Integer.TYPE, Int32Type.instance)
            .put(Long.class, LongType.instance)
            .put(Long.TYPE, LongType.instance)
            .put(Float.class, FloatType.instance)
            .put(Float.TYPE, FloatType.instance)
            .put(Double.class, DoubleType.instance)
            .put(Double.TYPE, DoubleType.instance)
            .put(Boolean.class, BooleanType.instance)
            .put(Boolean.TYPE, BooleanType.instance)
            .put(Byte.class, ByteType.instance)
            .put(Byte.TYPE, ByteType.instance)
            .put(Short.class, ShortType.instance)
            .put(Short.TYPE, ShortType.instance)
            .put(UUID.class, UUIDType.instance)
            .build();
    private final RowWalker<R> walker;
    private final Iterable<R> data;
    private final TableMetadata metadata;

    private CollectionVirtualTableAdapter(String tableName,
                                         String description,
                                         RowWalker<R> walker,
                                         Iterable<R> data)
    {
        this.walker = walker;
        this.data = data;
        this.metadata = buildMetadata(tableName, description, walker);
    }

    public static <C, R> CollectionVirtualTableAdapter<R> create(
            String rawName,
            String description,
            RowWalker<R> walker,
            Supplier<Iterable<C>> container,
            Function<C, R> rowFunc)
    {
        return new CollectionVirtualTableAdapter<>(virtualTableNameStyle(rawName),
                description,
                walker,
                () -> StreamSupport.stream(container.get().spliterator(), false)
                        .map(rowFunc).iterator());
    }

    public static String virtualTableNameStyle(String camel)
    {
        // Process sub names in the full metrics group name separately and then join them.
        // For example: "ClientRequest.Write-EACH_QUORUM" will be converted to "client_request_write_each_quorum".
        String[] subNames = ONLY_ALPHABET_PATTERN.matcher(camel).replaceAll(".").split("\\.");
        return Arrays.stream(subNames)
                .map(CollectionVirtualTableAdapter::camelToSnakeWithAbbreviations)
                .reduce((a, b) -> a + '_' + b)
                .orElseThrow(() -> new IllegalArgumentException("Invalid table name: " + camel));
    }

    private static String camelToSnakeWithAbbreviations(String camel)
    {
        Pattern pattern = Pattern.compile("^[A-Z1-9_]+$");
        // Contains only uppercase letters, numbers and underscores, so it's already snake case.
        if (pattern.matcher(camel).matches())
            return camel.toLowerCase();

        // Some special cases must be handled manually.
        String modifiedCamel = camel;
        for (Pair<String, String> replacement : knownAbbreviations)
            modifiedCamel = modifiedCamel.replace(replacement.left, replacement.right);

        return camelToSnake(modifiedCamel);
    }

    private TableMetadata buildMetadata(String tableName, String description, RowWalker<R> walker)
    {
        TableMetadata.Builder builder = TableMetadata.builder(VIRTUAL_VIEWS, tableName)
                .comment(description)
                .kind(TableMetadata.Kind.VIRTUAL);

        List<AbstractType<?>> partitionKeyTypes = new ArrayList<>(walker.count(Column.Type.PARTITION_KEY));
        walker.visitMeta(
                new RowWalker.MetadataVisitor()
                {
                    @Override
                    public <T> void accept(Column.Type type, String name, Class<T> clazz)
                    {
                        switch (type)
                        {
                            case PARTITION_KEY:
                                partitionKeyTypes.add(converters.get(clazz));
                                builder.addPartitionKeyColumn(camelToSnake(name), converters.get(clazz));
                                break;
                            case CLUSTERING:
                                builder.addClusteringColumn(camelToSnake(name), converters.get(clazz));
                                break;
                            case REGULAR:
                                builder.addRegularColumn(camelToSnake(name), converters.get(clazz));
                                break;
                            default:
                                throw new IllegalStateException("Unknown column type: " + type);
                        }
                    }
                });

        if (partitionKeyTypes.size() == 1)
            builder.partitioner(new LocalPartitioner(partitionKeyTypes.get(0)));
        else if (partitionKeyTypes.size() > 1)
            builder.partitioner(new LocalPartitioner(CompositeType.getInstance(partitionKeyTypes)));

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
    {
        if (!data.iterator().hasNext())
            return EmptyIterators.unfilteredPartition(metadata);

        Object[] tree;
        try (BTree.FastBuilder<Row> builder = BTree.fastBuilder())
        {
            StreamSupport.stream(data.spliterator(), true)
                    .map(row -> makeRow(row, columnFilter))
                    .filter(entry -> entry.getKey().equals(partitionKey))
                    .filter(entry -> clusteringFilter.selects(entry.getValue().clustering()))
                    .map(Map.Entry::getValue)
                    .forEach(builder::add);
            tree = clusteringFilter.isReversed() ? builder.buildReverse() : builder.build();
        }

        return new SingletonUnfilteredPartitionIterator(new ImmutableBTreePartition(metadata, partitionKey,
                columnFilter.queriedColumns(), Rows.EMPTY_STATIC_ROW, tree, DeletionInfo.LIVE,
                EncodingStats.NO_STATS).unfilteredIterator());
    }

    /** {@inheritDoc} */
    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        return createPartitionIterator(metadata, new AbstractIterator<>()
        {
            private final Iterator<Map.Entry<DecoratedKey, UnfilteredRowIterator>> partitions = buildDataRange(dataRange, columnFilter).entrySet().iterator();

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                return partitions.hasNext() ? partitions.next().getValue() : endOfData();
            }

            private NavigableMap<DecoratedKey, UnfilteredRowIterator> buildDataRange(DataRange dataRange, ColumnFilter columnFilter)
            {
                Map<DecoratedKey, BTree.FastBuilder<Row>> buildTree = new ConcurrentHashMap<>();
                try
                {
                    StreamSupport.stream(data.spliterator(), true)
                            .map(row -> makeRow(row, columnFilter))
                            .filter(entry -> dataRange.keyRange().contains(entry.getKey()))
                            .forEach(entry -> buildTree.computeIfAbsent(entry.getKey(), key -> BTree.fastBuilder())
                                    .add(entry.getValue()));

                    return buildTree.entrySet()
                            .stream()
                            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                                    new ImmutableBTreePartition(metadata,
                                            entry.getKey(),
                                            columnFilter.queriedColumns(),
                                            Rows.EMPTY_STATIC_ROW,
                                            dataRange.clusteringIndexFilter(entry.getKey()).isReversed() ? entry.getValue().buildReverse() : entry.getValue().build(),
                                            DeletionInfo.LIVE,
                                            EncodingStats.NO_STATS).unfilteredIterator()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));
                }
                finally
                {
                    buildTree.forEach((key, builder) -> builder.close());
                }
            }
        });
    }

    private Map.Entry<DecoratedKey, Row> makeRow(R row, ColumnFilter columnFilter)
    {
        assert metadata.partitionKeyColumns().size() == walker.count(Column.Type.PARTITION_KEY) :
                "Invalid number of partition key columns";
        assert metadata.clusteringColumns().size() == walker.count(Column.Type.CLUSTERING) :
                "Invalid number of clustering columns";

        Map<Column.Type, Object[]> fiterable = new EnumMap<>(Column.Type.class);
        fiterable.put(Column.Type.PARTITION_KEY, new Object[metadata.partitionKeyColumns().size()]);
        if (walker.count(Column.Type.CLUSTERING) > 0)
            fiterable.put(Column.Type.CLUSTERING, new Object[metadata.clusteringColumns().size()]);

        Map<ColumnMetadata, Object> cells = new HashMap<>();

        walker.visitRow(row, new RowWalker.RowMetadataVisitor()
        {
            private int pIdx, cIdx = 0;

            @Override
            public <T> void accept(Column.Type type, String name, Class<T> clazz, T value)
            {
                switch (type)
                {
                    case PARTITION_KEY:
                        fiterable.get(type)[pIdx++] = value;
                        break;
                    case CLUSTERING:
                        fiterable.get(type)[cIdx++] = value;
                        break;
                    case REGULAR:
                    {
                        if (columnFilter.equals(ColumnFilter.NONE))
                            break;

                        // Push down the column filter to the walker, so we don't have to process the value if it's not queried
                        ColumnMetadata cm = metadata.getColumn(ByteBufferUtil.bytes(camelToSnake(name)));
                        if (columnFilter.queriedColumns().contains(cm) && Objects.nonNull(value))
                            cells.put(cm, value);

                        break;
                    }
                    default:
                        throw new IllegalStateException("Unknown column type: " + type);
                }
            }
        });

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(makeRowClustering(metadata, fiterable.get(Column.Type.CLUSTERING)));
        cells.forEach((column, value) -> rowBuilder.addCell(BufferCell.live(column, NO_DELETION_TIME, decompose(column.type, value))));

        return new AbstractMap.SimpleEntry<>(makeRowKey(metadata, fiterable.get(Column.Type.PARTITION_KEY)),
                rowBuilder.build());
    }

    private static Clustering<?> makeRowClustering(TableMetadata metadata, Object... clusteringValues)
    {
        if (clusteringValues == null || clusteringValues.length == 0)
            return Clustering.EMPTY;

        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);
        return Clustering.make(clusteringByteBuffers);
    }

    /**
     * @param table the table metadata
     * @param partitionKeyValues the partition key values
     * @return the decorated key
     */
    private static DecoratedKey makeRowKey(TableMetadata table, Object...partitionKeyValues)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeyValues);
        else
            key = decompose(table.partitionKeyType, partitionKeyValues[0]);
        return table.partitioner.decorateKey(key);
    }

    private static UnfilteredPartitionIterator createPartitionIterator(
            TableMetadata metadata,
            Iterator<UnfilteredRowIterator> partitions)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            public UnfilteredRowIterator next()
            {
                return partitions.next();
            }

            public boolean hasNext()
            {
                return partitions.hasNext();
            }

            public TableMetadata metadata()
            {
                return metadata;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncate is not supported by table " + metadata);
    }
}
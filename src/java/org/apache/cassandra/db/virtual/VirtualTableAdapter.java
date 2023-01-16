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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import org.yaml.snakeyaml.introspector.Property;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.sysview.SystemView;
import org.apache.cassandra.sysview.SystemViewWalker;
import org.apache.cassandra.sysview.ViewRow;

public class VirtualTableAdapter<T extends ViewRow> extends AbstractMutableVirtualTable
{
    private static final Map<Class<?>, AbstractType<?>> CLASS_TO_CASSANDRA_TYPE_MAP = new HashMap<Class<?>, AbstractType<?>>() {{
        put(String.class, UTF8Type.instance);
        put(Class.class, UTF8Type.instance);
        put(Enum.class, UTF8Type.instance);
        put(boolean.class, BooleanType.instance);
        put(byte.class, ByteType.instance);
        put(short.class, ShortType.instance);
        put(int.class, Int32Type.instance);
        put(long.class, LongType.instance);
        put(char.class, UTF8Type.instance);
        put(float.class, FloatType.instance);
        put(double.class, DoubleType.instance);
    }};
    private final SystemView<T> systemView;

    public VirtualTableAdapter(String keyspace, SystemView<T> systemView)
    {
        super(buildTableMetadata(keyspace, systemView));
        this.systemView = systemView;
    }

    // TODO: Implement data(DecoratedKey partitionKey) that will provide access through the adapter.

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        AttributeWithValueVisitor visitor = new AttributeWithValueVisitor();

        for (T viewRow : systemView)
        {
            EnumMap<ColumnMetadata.Kind, Map<String, Object>> data = visitor.getData(viewRow, systemView.walker()::visitAll);

            List<Object> primaryKeyvalues = new ArrayList<>();
            primaryKeyvalues.addAll(data.get(ColumnMetadata.Kind.PARTITION_KEY).values());
            primaryKeyvalues.addAll(data.get(ColumnMetadata.Kind.CLUSTERING).values());

            result.row(primaryKeyvalues.toArray());

            for (Map.Entry<String, Object> column : data.get(ColumnMetadata.Kind.REGULAR).entrySet())
                result.column(column.getKey(), column.getValue());
        }

        return result;
    }

    /**
     * Builds the {@link TableMetadata} to be provided to the superclass based on the specified system views.
     *
     * @param keyspace The name of the keyspace.
     * @param systemView The name of the system view.
     * @return TableMetadata.
     */
    private static TableMetadata buildTableMetadata(String keyspace, SystemView<?> systemView)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, systemView.name())
                                                     .comment(systemView.description())
                                                     .kind(TableMetadata.Kind.VIRTUAL);
        systemView.walker().visitAll(new SystemViewWalker.AttributeVisitor()
        {
            @Override
            public <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz)
            {
                switch (type)
                {
                    case PARTITION_KEY:
                        builder.partitioner(new LocalPartitioner(CLASS_TO_CASSANDRA_TYPE_MAP.getOrDefault(clazz, UTF8Type.instance)))
                               .addPartitionKeyColumn(name, CLASS_TO_CASSANDRA_TYPE_MAP.getOrDefault(clazz, UTF8Type.instance));
                        break;
                    case CLUSTERING:
                        builder.addClusteringColumn(name, CLASS_TO_CASSANDRA_TYPE_MAP.getOrDefault(clazz, UTF8Type.instance));
                        break;
                    case REGULAR:
                        builder.addRegularColumn(name, CLASS_TO_CASSANDRA_TYPE_MAP.getOrDefault(clazz, UTF8Type.instance));
                        break;
                    case STATIC:
                    default:
                        throw new IllegalArgumentException("Unknown column type: " + type);
                }
            }
        });

        return builder.build();
    }

    private static class AttributeWithValueVisitor implements SystemViewWalker.AttributeWithValueVisitor
    {
        private EnumMap<ColumnMetadata.Kind, Map<String, Object>> data = new EnumMap<>(ColumnMetadata.Kind.class);

        public <T> EnumMap<ColumnMetadata.Kind, Map<String, Object>> getData(T row, BiConsumer<T, SystemViewWalker.AttributeWithValueVisitor> action)
        {
            data = new EnumMap<>(ColumnMetadata.Kind.class);
            action.accept(row, this);
            return data;
        }

        @Override
        public <T> void accept(ColumnMetadata.Kind type, String name, Class<T> clazz, @Nullable T value)
        {
            if (value == null)
                data.computeIfAbsent(type, t -> new HashMap<>()).put(name, null);
            else if (clazz.isEnum())
                data.computeIfAbsent(type, t -> new HashMap<>()).put(name, ((Enum<?>) value).name());
            else if (clazz.isAssignableFrom(Class.class))
                data.computeIfAbsent(type, t -> new HashMap<>()).put(name, ((Class<?>) value).getName());
            else
                data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptBoolean(ColumnMetadata.Kind type, String name, boolean value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptChar(ColumnMetadata.Kind type, String name, char value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptByte(ColumnMetadata.Kind type, String name, byte value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptShort(ColumnMetadata.Kind type, String name, short value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptInt(ColumnMetadata.Kind type, String name, int value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptLong(ColumnMetadata.Kind type, String name, long value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptFloat(ColumnMetadata.Kind type, String name, float value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }

        @Override
        public void acceptDouble(ColumnMetadata.Kind type, String name, double value)
        {
            data.computeIfAbsent(type, t -> new HashMap<>()).put(name, value);
        }
    }
}

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

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.virtual.proc.RowWalker;
import org.apache.cassandra.db.virtual.sysview.SystemView;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;
import static org.apache.cassandra.utils.FBUtilities.camelToSnake;

public class VirtualTableSystemViewAdapter<R> extends AbstractVirtualTable
{
    private static final Pattern ONLY_ALPHABET_PATTERN = Pattern.compile("[^a-zA-Z]");
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
    private final SystemView<R> systemView;

    public VirtualTableSystemViewAdapter(SystemView<R> systemView, UnaryOperator<String> tableNameMapper)
    {
        super(createMetadata(systemView, tableNameMapper));
        this.systemView = systemView;
    }

    private static String virtualTableStyle(String name)
    {
        return ONLY_ALPHABET_PATTERN.matcher(camelToSnake(name))
                .replaceAll("_");
    }

    private static TableMetadata createMetadata(SystemView<?> systemView, UnaryOperator<String> nameMapper)
    {

        TableMetadata.Builder builder = TableMetadata.builder(VIRTUAL_VIEWS, virtualTableStyle(nameMapper.apply(systemView.name())))
                .comment(systemView.description())
                .kind(TableMetadata.Kind.VIRTUAL);
        systemView.walker().visitMeta(
                new RowWalker.MetadataVisitor()
                {
                    @Override
                    public <T> void accept(int index, String name, Class<T> clazz)
                    {
                        if (index == 0)
                        {
                            builder.partitioner(new LocalPartitioner(converters.get(clazz)));
                            builder.addPartitionKeyColumn(camelToSnake(name), converters.get(clazz));
                        }
                        else
                            builder.addRegularColumn(camelToSnake(name), converters.get(clazz));
                    }
                });

        return builder.build();
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        systemView.iterator().forEachRemaining(viewRow ->
                systemView.walker().visitRow(viewRow, new RowWalker.RowMetadataVisitor()
                {
                    @Override
                    public <T> void accept(int index, String name, Class<T> clazz, @Nullable T value)
                    {
                        if (index == 0)
                            result.row(value);
                        else
                            result.column(camelToSnake(name), value);
                    }
                }));

        return result;
    }
}

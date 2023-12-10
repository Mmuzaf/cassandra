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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public final class VirtualKeyspaceRegistry
{
    private final Map<String, VirtualKeyspace> virtualKeyspaces = new ConcurrentHashMap<>();
    public static final VirtualKeyspaceRegistry instance = new VirtualKeyspaceRegistry();

    private VirtualKeyspaceRegistry()
    {
        register(new VirtualKeyspace(VIRTUAL_VIEWS, Collections.emptyList()));
    }

    public void addToKeyspace(String ksName, Collection<VirtualTable> tables)
    {
        virtualKeyspaces.computeIfPresent(ksName,
                (name, oldKeyspace) -> {
                    Map<String, VirtualTable> newTables = oldKeyspace.tables()
                            .stream()
                            .collect(Collectors.toMap(VirtualTable::name, Function.identity()));
                    tables.forEach(t -> newTables.putIfAbsent(t.name(), t));
                    return new VirtualKeyspace(name, newTables.values());
                });
    }

    public void removeFromKeyspace(String ksName, Collection<VirtualTable> tables)
    {
        virtualKeyspaces.computeIfPresent(ksName,
                (name, oldKeyspace) -> {
                    Map<String, VirtualTable> newTables = oldKeyspace.tables()
                            .stream()
                            .collect(Collectors.toMap(VirtualTable::name, Function.identity()));
                    tables.forEach(t -> newTables.remove(t.name()));
                    return new VirtualKeyspace(name, newTables.values());
                });
    }

    public void register(VirtualKeyspace keyspace)
    {
        virtualKeyspaces.put(keyspace.name(), keyspace);
    }

    @Nullable
    public VirtualKeyspace getKeyspaceNullable(String name)
    {
        return virtualKeyspaces.get(name);
    }

    @Nullable
    public VirtualTable getTableNullable(TableId id)
    {
        return getTable(id);
    }

    @Nullable
    public KeyspaceMetadata getKeyspaceMetadataNullable(String name)
    {
        VirtualKeyspace keyspace = virtualKeyspaces.get(name);
        return null != keyspace ? keyspace.metadata() : null;
    }

    @Nullable
    public TableMetadata getTableMetadataNullable(TableId id)
    {
        VirtualTable table = getTable(id);
        return null != table ? table.metadata() : null;
    }

    public Iterable<KeyspaceMetadata> virtualKeyspacesMetadata()
    {
        return Iterables.transform(virtualKeyspaces.values(), VirtualKeyspace::metadata);
    }

    private VirtualTable getTable(TableId id)
    {
        return virtualKeyspaces.values().stream()
                .map(VirtualKeyspace::tables)
                .flatMap(Collection::stream)
                .filter(t -> t.metadata().id.equals(id))
                .findFirst()
                .orElse(null);
    }
}

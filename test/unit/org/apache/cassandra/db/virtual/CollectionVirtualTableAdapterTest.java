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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.model.CollectionEntry;
import org.apache.cassandra.db.virtual.model.CollectionEntryTestRow;
import org.apache.cassandra.db.virtual.model.CollectionEntryTestRowWalker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CollectionVirtualTableAdapterTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String VT_NAME = "collection_virtual_table";
    private final List<VirtualTable> tables = new ArrayList<>();
    private final List<CollectionEntry> collection = new ArrayList<>();

    private static void addSinglePartitionData(Collection<CollectionEntry> list)
    {
        list.add(new CollectionEntry("1984", "key", 3, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1984", "key", 2, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1984", "key", 1, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
    }

    private static void addMultiPartitionData(Collection<CollectionEntry> list)
    {
        addSinglePartitionData(list);
        list.add(new CollectionEntry("1985", "key", 3, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1985", "key", 2, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1985", "key", 1, "value",
                1, 1, 1, (short) 1, (byte) 1, true));
    }

    @Before
    public void config() throws Exception
    {
        tables.add(CollectionVirtualTableAdapter.create(
                VT_NAME,
                "The collection virtual table",
                new CollectionEntryTestRowWalker(),
                () -> collection,
                CollectionEntryTestRow::new,
                UnaryOperator.identity()));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, tables));
    }

    @After
    public void postCleanup()
    {
        collection.clear();
    }

    @Test
    public void testSelectAll()
    {
        addSinglePartitionData(collection);
        ResultSet result = executeNet(String.format("SELECT * FROM %s.%s", KS_NAME, VT_NAME));

        int index = 0;
        for (Row row : result)
        {
            assertEquals(collection.get(index).getPrimaryKey(), row.getString("primary_key"));
            assertEquals(collection.get(index).getSecondaryKey(), row.getString("secondary_key"));
            assertEquals(collection.get(index).getOrderedKey(), row.getLong("ordered_key"));
            assertEquals(collection.get(index).getIntValue(), row.getInt("int_value"));
            assertEquals(collection.get(index).getLongValue(), row.getLong("long_value"));
            assertEquals(collection.get(index).getValue(), row.getString("value"));
            assertEquals(collection.get(index).getDoubleValue(), row.getDouble("double_value"), 0.0);
            assertEquals(collection.get(index).getShortValue(), row.getShort("short_value"));
            assertEquals(collection.get(index).getByteValue(), row.getByte("byte_value"));
            assertEquals(collection.get(index).getBooleanValue(), row.getBool("boolean_value"));
            index++;
        }
        assertEquals(collection.size(), index);
    }

    @Test
    public void testSelectPartition()
    {
        addMultiPartitionData(collection);
        ResultSet result =  executeNet(String.format("SELECT * FROM %s.%s WHERE primary_key = ? AND secondary_key = ?", KS_NAME, VT_NAME),
                "1984", "key");

        AtomicInteger size = new AtomicInteger();
        result.forEach(row -> {
            assertEquals("1984", row.getString("primary_key"));
            assertEquals("key", row.getString("secondary_key"));
            size.incrementAndGet();
        });
        assertEquals(3, size.get());
    }

    @Test
    public void testSelectEmptyPartition()
    {
        addSinglePartitionData(collection);
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s WHERE primary_key = 'EMPTY'", KS_NAME, VT_NAME)));
    }

    @Test
    public void testSelectEmptyCollection()
    {
        collection.clear();
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s", KS_NAME, VT_NAME)));
    }
}

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
package org.apache.cassandra.cql3;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.ForwardingSSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryWithIndexedSSTableTest extends CQLTester
{
    @Test
    public void queryIndexedSSTableTest() throws Throwable
    {
        // That test reproduces the bug from CASSANDRA-10903 and the fact we have a static column is
        // relevant to that reproduction in particular as it forces a slightly different code path that
        // if there wasn't a static.

        int ROWS = 1000;
        int VALUE_LENGTH = 100;

        createTable("CREATE TABLE %s (k int, t int, s text static, v text, PRIMARY KEY (k, t))");

        // We create a partition that is big enough that the underlying sstable will be indexed
        // For that, we use a large-ish number of row, and a value that isn't too small.
        String text = TombstonesWithIndexedSSTableTest.makeRandomString(VALUE_LENGTH);
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?)", 0, i, text + i);

        flush();
        compact();

        // Sanity check that we're testing what we want to test, that is that we're reading from an indexed
        // sstable. Note that we'll almost surely have a single indexed sstable in practice, but it's theorically
        // possible for a compact strategy to yield more than that and as long as one is indexed we're pretty
        // much testing what we want. If this check ever fails on some specific setting, we'll have to either
        // tweak ROWS and VALUE_LENGTH, or skip the test on those settings.
        DecoratedKey dk = Util.dk(ByteBufferUtil.bytes(0));
        boolean hasIndexed = false;
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            class IndexEntryAccessor extends ForwardingSSTableReader
            {
                public IndexEntryAccessor(SSTableReader delegate)
                {
                    super(delegate);
                }

                public AbstractRowIndexEntry getRowIndexEntry(PartitionPosition key, Operator op, boolean updateCacheAndStats, SSTableReadsListener listener)
                {
                    return super.getRowIndexEntry(key, op, updateCacheAndStats, listener);
                }
            }

            IndexEntryAccessor accessor = new IndexEntryAccessor(sstable);
            AbstractRowIndexEntry indexEntry = accessor.getRowIndexEntry(dk, SSTableReader.Operator.EQ, false, SSTableReadsListener.NOOP_LISTENER);
            hasIndexed |= indexEntry != null && indexEntry.isIndexed();
        }
        assert hasIndexed;

        assertRowCount(execute("SELECT s FROM %s WHERE k = ?", 0), ROWS);
        assertRowCount(execute("SELECT s FROM %s WHERE k = ? ORDER BY t DESC", 0), ROWS);

        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ?", 0), 1);
        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ? ORDER BY t DESC", 0), 1);
    }

    @Test
    public void testSSTableReader() throws Exception
    {
        String keyspace = "harry";
        String cf = "table_1";
//        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner);
        Descriptor desc = Descriptor.fromFileWithComponent(new File("test/data/cassandra-18932/data1/harry/table_1-07c35a606c0a11eeae7a4f6ca489eb0c/nc-5-big-Data.db"), false).left;
        TableMetadata tm = TableMetadata.builder(keyspace, cf)
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addRegularColumn("regular0003", UTF8Type.instance)
                .addRegularColumn("regular0004", UTF8Type.instance)
                .addRegularColumn("regular0005", UTF8Type.instance)
                .addRegularColumn("regular0006", UTF8Type.instance)
                .addRegularColumn("regular0007", UTF8Type.instance)
                .partitioner(Murmur3Partitioner.instance)
                .build();

        SSTableReader reader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(tm));

        executeInternal("SELECT * FROM harry.table_1");

        File bloomFile = desc.fileFor(BigFormat.Components.FILTER);
        long bloomModified = bloomFile.lastModified();

        File summaryFile = desc.fileFor(BigFormat.Components.SUMMARY);
        long summaryModified = summaryFile.lastModified();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different

        // check that bloomfilter/summary ARE NOT regenerated and BF=AlwaysPresent when filter component is missing
        Set<Component> components = desc.discoverComponents();
        components.remove(BigFormat.Components.FILTER);
        // clear and create just one sstable for this test
        Keyspace keyspace0 = Keyspace.openWithoutSSTables(keyspace);
        ColumnFamilyStore store = keyspace0.getColumnFamilyStore(cf);

        SSTableReader target = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(tm));
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
            assertEquals(0, ((SSTableReaderWithFilter) target).getFilterOffHeapSize());
        }
        finally
        {
            target.selfRef().close();
        }

        // #### online tests ####
        // check that summary & bloomfilter are not regenerated when SSTable is opened and BFFP has been changed
        target = SSTableReader.open(store, desc, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter is recreated when it doesn't exist and this causes the summary to be recreated
        components = desc.discoverComponents();
        components.remove(BigFormat.Components.FILTER);
        components.remove(BigFormat.Components.SUMMARY);

        target = SSTableReader.open(store, desc, components, store.metadata);
        try {
            assertTrue("Bloomfilter was not recreated", bloomModified < bloomFile.lastModified());
            assertTrue("Summary was not recreated", summaryModified < summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that only the summary is regenerated when it is deleted
        components.add(BigFormat.Components.FILTER);
        summaryModified = summaryFile.lastModified();
        summaryFile.tryDelete();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
        bloomModified = bloomFile.lastModified();

        target = SSTableReader.open(store, desc, components, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertTrue("Summary was not recreated", summaryModified < summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that summary and bloomfilter is not recreated when the INDEX is missing
        components.add(BigFormat.Components.SUMMARY);
        components.remove(BigFormat.Components.PRIMARY_INDEX);

        summaryModified = summaryFile.lastModified();
        target = SSTableReader.open(store, desc, components, store.metadata, false, false);
        try
        {
            TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }
//        reader.runOnClose(() -> runOnCloseExecuted1.set(true));
//        reader.runOnClose(() -> runOnCloseExecuted2.set(true)); // second time to actually create lambda referencing to lambda, see runOnClose impl
        //noinspection UnusedAssignment
//        reader = null; // this is required, otherwise GC will not attempt to collect the created reader
        KeyIterator keyIterator = reader.keyIterator();

        while (keyIterator.hasNext())
        {
            DecoratedKey key = keyIterator.next();
            System.out.println(">>>>> " + key);
        }
    }
}

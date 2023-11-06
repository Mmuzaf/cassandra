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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.AbstractClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CQLBurnTest extends CQLTester
{
//    LTS: 462695. Pd -954500797334660271. Operation: Operation{, opId=0, opType=DELETE_SLICE} Statement
//    CompiledStatement{cql='DELETE FROM harry.table_1 USING TIMESTAMP 562695
//      WHERE pk0003 = ? AND pk0004 = ? AND pk0005 = ? AND ck0002 = ? AND ck0003 = ? AND ck0004 >= ? AND ck0004 < ?;',
//      bindings="ZinzDdUuABgDknItABgDknItABgDknItXEFrgBnOmPmPylWrwXHqjBHgeQrGfnZd1124124583",
//      "ZinzDdUuABgDknItABgDknItABgDknItABgDknItABgDknItzHqchghqCXLhVYKM22215251",
//      (float)3.2758E-41,
//      -325110980,
//      "ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaWGvFtKGKPLETZpMp103558722216718690109",
//      "ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaHHruatGSYPEdCtCM183223761074621719323664239",
//      "ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaQrShISWKhUkPNKtL15916716250240"}
//
//    LTS: 457625. Pd -954500797334660271. Operation: Operation{, opId=0, opType=INSERT} Statement
//    CompiledStatement{cql='INSERT INTO harry.table_1 (pk0003,pk0004,pk0005,ck0002,ck0003,ck0004,regular0004,regular0005)
//      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
//      USING TIMESTAMP 557625;',
//      bindings="ZinzDdUuABgDknItABgDknItABgDknItXEFrgBnOmPmPylWrwXHqjBHgeQrGfnZd1124124583",
//      "ZinzDdUuABgDknItABgDknItABgDknItABgDknItABgDknItzHqchghqCXLhVYKM22215251",
//      (float)3.2758E-41,
//      1448745370,
//      "ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyazHqchghqsCPBjcHM82",
//      "ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaZAdmsIGRcuJJMlAi198121167158",
//      "NMMtXmeEAdeXABdimTNkwIyBkpiSTrDeStFwlwzVFGTwdBRbqtPigrIaLhDJFhdN",
//      "ZNvVSxVtFGTwdBRbQDikZLmszNckJdrhdSeOWcEvEIBcqGeBfpdJQfkGlFWhGWqH13324347"}

    @Test
    public void testActiveTombstoneInIndex() throws Throwable
    {
        long awaitMs = 20_000;
        long endMs = Clock.Global.currentTimeMillis() + awaitMs;
        ExecutorService executor = Executors.newFixedThreadPool(16);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger values = new AtomicInteger();
        AtomicInteger timestamp = new AtomicInteger(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(16);
        int ROWS = 1000;
        int VALUE_LENGTH = 100;

        createTable("CREATE TABLE %s (k int, t int, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, t)) WITH caching = { 'keys' : 'NONE' }");
        String text = makeRandomString(VALUE_LENGTH);

        Runnable runnable = () ->
        {
            try
            {
                while (!Thread.currentThread().isInterrupted() && !stop.get() && error.get() == null)
                {
                    if (endMs - Clock.Global.currentTimeMillis() < 0)
                        stop.set(true);
                    execute("INSERT INTO %s (k, t, v1) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, values.incrementAndGet(), text, (long) timestamp.incrementAndGet());
                    Thread.sleep(ThreadLocalRandom.current().nextInt(0, 100));
                }
            }
            catch (Throwable t)
            {
                error.set(t);
            }
            finally
            {
                latch.countDown();
            }
        };

        for (int i = 0; i < 14; i++)
            executor.submit(runnable);

        Runnable deletion = () -> {
            try
            {
                while (!Thread.currentThread().isInterrupted() && !stop.get() && error.get() == null)
                {
                    if (endMs - Clock.Global.currentTimeMillis() < 0)
                        stop.set(true);

                    int vs = ThreadLocalRandom.current().nextInt(1, (int) (values.get() * 0.9));
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = 0 AND t >= ? AND t < ?", (long) timestamp.get(), 0, vs);
                }
            }
            catch (Throwable t)
            {
                error.set(t);
            }
            finally
            {
                latch.countDown();
            }
        };

        Runnable compact = () -> {
            try
            {
                while (!Thread.currentThread().isInterrupted() && !stop.get() && error.get() == null)
                {
                    if (endMs - Clock.Global.currentTimeMillis() < 0)
                        stop.set(true);

                    Thread.sleep(ThreadLocalRandom.current().nextInt(0, 4000));
                    compact();
                }
            }
            catch (Throwable t)
            {
                error.set(t);
            }
            finally
            {
                latch.countDown();
            }
        };

        // TODO delete-insert cell and/or columns at runtime
        executor.submit(deletion);
        executor.submit(compact);

        // Write a large-enough partition to be indexed.
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", 0, i, text);
        // Add v2 that should survive part of the deletion we later insert
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v2) VALUES (?, ?, ?) USING TIMESTAMP 3", 0, i, text);
        flush();

        // Now delete parts of this partition, but add enough new data to make sure the deletion spans index blocks
        int minDeleted1 = ROWS/10;
        int maxDeleted1 = 5 * ROWS/10;
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND t >= ? AND t < ?", minDeleted1, maxDeleted1);

        // Delete again to make a boundary
        int minDeleted2 = 4 * ROWS/10;
        int maxDeleted2 = 9 * ROWS/10;
        execute("DELETE FROM %s USING TIMESTAMP 4 WHERE k = 0 AND t >= ? AND t < ?", minDeleted2, maxDeleted2);

        // Add v3 surviving that deletion too and also ensuring the two deletions span index blocks
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s (k, t, v3) VALUES (?, ?, ?) USING TIMESTAMP 5", 0, i, text);
        flush();
        compact();

        latch.await(60L, java.util.concurrent.TimeUnit.SECONDS);
        if (error.get() != null)
            throw error.get();

        compact();
        UntypedResultSet rs = execute("SELECT * FROM %s ALLOW FILTERING");
        assertFalse(rs.isEmpty());

        // test deletions worked
//        verifyExpectedActiveTombstoneRows(ROWS, text, minDeleted1, minDeleted2, maxDeleted2);

        // Test again compacted. This is much easier to pass and doesn't actually test active tombstones in index
//        compact();
//        verifyExpectedActiveTombstoneRows(ROWS, text, minDeleted1, minDeleted2, maxDeleted2);
    }

    private void verifyExpectedActiveTombstoneRows(int ROWS, String text, int minDeleted1, int minDeleted2, int maxDeleted2) throws Throwable
    {
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v1 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v1 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted1));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v2 = ? ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v2 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS - (maxDeleted2 - minDeleted2));
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v3 = ? ALLOW FILTERING", 0, text), ROWS);
        assertRowCount(execute("SELECT t FROM %s WHERE k = ? AND v3 = ? ORDER BY t DESC ALLOW FILTERING", 0, text), ROWS);
        // test index yields the correct active deletions
        for (int i = 0; i < ROWS; ++i)
        {
            final String v1Expected = i < minDeleted1 || i >= maxDeleted2 ? text : null;
            final String v2Expected = i < minDeleted2 || i >= maxDeleted2 ? text : null;
            assertRows(execute("SELECT v1,v2,v3 FROM %s WHERE k = ? AND t >= ? LIMIT 1", 0, i),
                    row(v1Expected, v2Expected, text));
            assertRows(execute("SELECT v1,v2,v3 FROM %s WHERE k = ? AND t <= ? ORDER BY t DESC LIMIT 1", 0, i),
                    row(v1Expected, v2Expected, text));
        }
    }

    public static String makeRandomString(int length)
    {
        Random random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i++] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    private static final UTF8Type UTF8 = UTF8Type.instance;
    private static final DecimalType DECIMAL = DecimalType.instance;
    private static final IntegerType VARINT = IntegerType.instance;

    @Test
    public void testLowerBoundApplicableMultipleColumnsDesc()
    {
        Descriptor descriptor = Descriptor.fromFileWithComponent(new File("test/data/cassandra-18932/data1/harry/table_1-07c35a606c0a11eeae7a4f6ca489eb0c/nc-5-big-Data.db"), false).left;
        TableMetadata tm = TableMetadata.builder("harry", "table_1")
                .addPartitionKeyColumn("pk0003", UTF8Type.instance)
                .addPartitionKeyColumn("pk0004", AsciiType.instance)
                .addPartitionKeyColumn("pk0005", FloatType.instance)
                .addClusteringColumn("ck0002", IntegerType.instance)
                .addClusteringColumn("ck0003", UTF8Type.instance)
                .addClusteringColumn("ck0004",AsciiType.instance)
                .addRegularColumn("regular0003", FloatType.instance)
                .addRegularColumn("regular0004", AsciiType.instance)
                .addRegularColumn("regular0005", UTF8Type.instance)
                .addRegularColumn("regular0006", FloatType.instance)
                .addRegularColumn("regular0007", UTF8Type.instance)
                .partitioner(Murmur3Partitioner.instance)
                .build();

        SSTableReader sstable = SSTableReader.openNoValidation(null, descriptor, TableMetadataRef.forOfflineTools(tm));

//        DecoratedKey(-1562687481824787574, 004a5a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e49745845467267426e4f6d506d50796c5772775848716a42486765517247666e5a64313132343132343538330000485a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e49747a4871636867687143584c6856594b4d323232313532353100000400005b5100)
//        DecoratedKey key = tm.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        // keyRange = {Bounds@10861} "[DecoratedKey(-1562687481824787574, 004a5a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e49745845467267426e4f6d506d50796c5772775848716a42486765517247666e5a64313132343132343538330000485a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e49747a4871636867687143584c6856594b4d323232313532353100000400005b5100),min(-9223372036854775808)]"
        // left = {PreHashedDecoratedKey@10866} "DecoratedKey(-1562687481824787574, 004a5a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e49745845467267426e4f6d506d50796c5772775848716a42486765517247666e5a64313132343132343538330000485a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e49747a4871636867687143584c6856594b4d323232313532353100000400005b5100)"
        // right = {Token$KeyBound@10867} "min(-9223372036854775808)"

//        DecoratedKey(-1562687481824787574, 004a5a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e49745845467267426e4f6d506d50796c5772775848716a42486765517247666e5a64313132343132343538330000485a696e7a44645575414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e4974414267446b6e49747a4871636867687143584c6856594b4d323232313532353100000400005b5100)

//        {(-1110871748
//        :ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaFfLoPrEzlMDvLfXY18918213101196160
//        :ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyachTAyMjmsZMUPCzi23819065184175,
//        -1110871748:ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaFfLoPrEzlMDvLfXY18918213101196160]}

//        cqlsh> SELECT * FROM harry.table_1 WHERE pk0003 = 'ZinzDdUuABgDknItABgDknItABgDknItXEFrgBnOmPmPylWrwXHqjBHgeQrGfnZd1124124583'
//        AND pk0004 = 'ZinzDdUuABgDknItABgDknItABgDknItABgDknItABgDknItzHqchghqCXLhVYKM22215251'
//        AND pk0005 = 3.2758E-41 AND ck0002 = -1110871748
//        AND ck0003 = 'ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaFfLoPrEzlMDvLfXY18918213101196160'
//        AND ck0004 < 'ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyachTAyMjmsZMUPCzi23819065184175';

//        ByteBuffer[] clusteringKeyValues = new ByteBuffer[] {
//                UTF8.decompose(stringValue),
//                DECIMAL.decompose(decimalValue),
//                VARINT.decompose(varintValue)
//        };

//        BufferClusteringBound.create(ClusteringPrefix.Kind.EXCL_START_BOUND, )

        UnfilteredPartitionIterator partitionIterator = sstable.partitionIterator(ColumnFilter.all(tm),
                DataRange.allData(tm.partitioner),
                SSTableReadsListener.NOOP_LISTENER);

        assertTrue(partitionIterator.hasNext());
        while(partitionIterator.hasNext())
        {
            UnfilteredRowIterator iter = partitionIterator.next();
            while (iter.hasNext())
                System.out.println(">>>>>> data " + iter.next());
        }
        partitionIterator.close();

        // (min(-9223372036854775808),min(-9223372036854775808)]


        Slices.Builder slicesBuilder = new Slices.Builder(tm.comparator);
        slicesBuilder.add(Slice.ALL);
        Slices slices = slicesBuilder.build();
        DecoratedKey key = Util.dk(ByteBufferUtil.bytes(0));

        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(tm,
                FBUtilities.nowInSeconds(),
                ColumnFilter.all(tm),
                RowFilter.none(),
                DataLimits.NONE,
                key,
                new ClusteringIndexSliceFilter(slices, true));

        try (UnfilteredRowIteratorWithLowerBound iter = new UnfilteredRowIteratorWithLowerBound(key,
                sstable,
                slices,
                true,
                ColumnFilter.all(tm),
                Mockito.mock(SSTableReadsListener.class)))
        {
            while (iter.hasNext())
            {
                Unfiltered row = iter.next();
                System.out.println(">>>>>> " + row.toString(tm));
            }
        }
    }
//
//    private SSTableReader createSSTable(TableMetadata metadata, String keyspace, String table, String query)
//    {
//        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
//        for (int i = 0; i < 10; i++)
//            QueryProcessor.executeInternal(String.format(query, keyspace, table, i));
//        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
//        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));
//        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, key));
//        assertEquals(1, view.sstables.size());
//        return view.sstables.get(0);
//    }

    @Test
    public void testPagingUsingUppgerCommand() throws Exception
    {
        Descriptor descriptor = Descriptor.fromFileWithComponent(new File("test/data/cassandra-18932/data1/harry/table_1-07c35a606c0a11eeae7a4f6ca489eb0c/nc-5-big-Data.db"), false).left;
        TableMetadata tm = TableMetadata.builder("harry", "table_1")
            .addPartitionKeyColumn("pk0003", UTF8Type.instance)
            .addPartitionKeyColumn("pk0004", AsciiType.instance)
            .addPartitionKeyColumn("pk0005", FloatType.instance)
            .addClusteringColumn("ck0002", IntegerType.instance)
            .addClusteringColumn("ck0003", UTF8Type.instance)
            .addClusteringColumn("ck0004",AsciiType.instance)
            .addRegularColumn("regular0003", FloatType.instance)
            .addRegularColumn("regular0004", AsciiType.instance)
            .addRegularColumn("regular0005", UTF8Type.instance)
            .addRegularColumn("regular0006", FloatType.instance)
            .addRegularColumn("regular0007", UTF8Type.instance)
            .partitioner(Murmur3Partitioner.instance)
                .build();

        DecoratedKey key = Util.dk(ByteBufferUtil.bytes("ZinzDdUuABgDknItABgDknItABgDknItXEFrgBnOmPmPylWrwXHqjBHgeQrGfnZd1124124583"));
        ReadCommand readCommand = SinglePartitionReadCommand.create(tm, FBUtilities.nowInSeconds(), ColumnFilter.all(tm),
                RowFilter.none(), DataLimits.cqlLimits(1), key, new ClusteringIndexSliceFilter(Slices.ALL, false));

        // PartitionRangeReadCommand.withUpdatedLimitsAndDataRange(DataLimits.cqlLimits(1), DataRange.allData(tm.partitioner));


        UnfilteredPartitionIterator partitionIterator = readCommand.executeLocally(readCommand.executionController());
        assert partitionIterator.hasNext();
        UnfilteredRowIterator partition = partitionIterator.next();

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            System.out.println(">>>>>> " + unfiltered);
        }
    }
/*
  Breakpoint reached at org.apache.cassandra.io.sstable.AbstractSSTableIterator$AbstractReader.<init>(AbstractSSTableIterator.java:330)
  Breakpoint reached
  	at org.apache.cassandra.io.sstable.AbstractSSTableIterator$AbstractReader.<init>(AbstractSSTableIterator.java:330)
  	at org.apache.cassandra.io.sstable.format.big.SSTableReversedIterator$ReverseReader.<init>(SSTableReversedIterator.java:108)
  	at org.apache.cassandra.io.sstable.format.big.SSTableReversedIterator$ReverseIndexedReader.<init>(SSTableReversedIterator.java:284)
  	at org.apache.cassandra.io.sstable.format.big.SSTableReversedIterator.createReaderInternal(SSTableReversedIterator.java:75)
  	at org.apache.cassandra.io.sstable.format.big.SSTableReversedIterator.createReaderInternal(SSTableReversedIterator.java:53)
  	at org.apache.cassandra.io.sstable.AbstractSSTableIterator.createReader(AbstractSSTableIterator.java:202)
  	at org.apache.cassandra.io.sstable.AbstractSSTableIterator.<init>(AbstractSSTableIterator.java:130)
  	at org.apache.cassandra.io.sstable.format.big.SSTableReversedIterator.<init>(SSTableReversedIterator.java:68)
  	at org.apache.cassandra.io.sstable.format.big.BigTableReader.rowIterator(BigTableReader.java:143)
  	at org.apache.cassandra.io.sstable.format.big.BigTableReader.rowIterator(BigTableReader.java:135)
  	at org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound.initializeIterator(UnfilteredRowIteratorWithLowerBound.java:122)
  	at org.apache.cassandra.db.rows.LazilyInitializedUnfilteredRowIterator.maybeInit(LazilyInitializedUnfilteredRowIterator.java:48)
  	at org.apache.cassandra.db.rows.LazilyInitializedUnfilteredRowIterator.computeNext(LazilyInitializedUnfilteredRowIterator.java:94)
  	at org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound.computeNext(UnfilteredRowIteratorWithLowerBound.java:130)
  	at org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound.computeNext(UnfilteredRowIteratorWithLowerBound.java:54)
  	at org.apache.cassandra.utils.AbstractIterator.hasNext(AbstractIterator.java:47)
  	at org.apache.cassandra.utils.MergeIterator$Candidate.advance(MergeIterator.java:376)
  	at org.apache.cassandra.utils.MergeIterator$ManyToOne.advance(MergeIterator.java:188)
  	at org.apache.cassandra.utils.MergeIterator$ManyToOne.computeNext(MergeIterator.java:157)
  	at org.apache.cassandra.utils.AbstractIterator.hasNext(AbstractIterator.java:47)
  	at org.apache.cassandra.db.rows.UnfilteredRowIterators$UnfilteredRowMergeIterator.computeNext(UnfilteredRowIterators.java:534)
  	at org.apache.cassandra.db.rows.UnfilteredRowIterators$UnfilteredRowMergeIterator.computeNext(UnfilteredRowIterators.java:402)
  	at org.apache.cassandra.utils.AbstractIterator.hasNext(AbstractIterator.java:47)
  	at org.apache.cassandra.db.rows.UnfilteredRowIterator.isEmpty(UnfilteredRowIterator.java:67)
  	at org.apache.cassandra.db.SinglePartitionReadCommand.withSSTablesIterated(SinglePartitionReadCommand.java:888)
  	at org.apache.cassandra.db.SinglePartitionReadCommand.queryMemtableAndDiskInternal(SinglePartitionReadCommand.java:813)
  	at org.apache.cassandra.db.SinglePartitionReadCommand.queryMemtableAndDisk(SinglePartitionReadCommand.java:657)
  	at org.apache.cassandra.db.SinglePartitionReadCommand.queryStorage(SinglePartitionReadCommand.java:485)
  	at org.apache.cassandra.db.ReadCommand.executeLocally(ReadCommand.java:425)
  	at org.apache.cassandra.db.SinglePartitionReadQuery$Group.executeLocally(SinglePartitionReadQuery.java:238)
  	at org.apache.cassandra.db.SinglePartitionReadQuery$Group.executeInternal(SinglePartitionReadQuery.java:212)
  	at org.apache.cassandra.cql3.statements.SelectStatement.executeInternal(SelectStatement.java:540)
  	at org.apache.cassandra.cql3.statements.SelectStatement.executeLocally(SelectStatement.java:512)
  	at org.apache.cassandra.cql3.statements.SelectStatement.executeLocally(SelectStatement.java:105)
  	at org.apache.cassandra.cql3.QueryProcessor.executeInternal(QueryProcessor.java:445)
  	at org.apache.cassandra.cql3.CQLTester.executeFormattedQuery(CQLTester.java:1594)
  	at org.apache.cassandra.cql3.CQLTester.execute(CQLTester.java:1573)
  	at org.apache.cassandra.concurrent.CQLBurnTest.verifyExpectedActiveTombstoneRows(CQLBurnTest.java:108)
  	at org.apache.cassandra.concurrent.CQLBurnTest.testActiveTombstoneInIndex(CQLBurnTest.java:90)
  	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(NativeMethodAccessorImpl.java:-1)
  	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  	at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  	at java.lang.reflect.Method.invoke(Method.java:566)
  	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
  	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
  	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
  	at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
  	at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
  	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
  	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
  	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
  	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
  	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
  	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
  	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
  	at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
  	at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
  	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
  	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:69)
  	at com.intellij.rt.junit.IdeaTestRunner$Repeater$1.execute(IdeaTestRunner.java:38)
  	at com.intellij.rt.execution.junit.TestsRepeater.repeat(TestsRepeater.java:11)
  	at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:35)
  	at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:235)
  	at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:54)

 */

}
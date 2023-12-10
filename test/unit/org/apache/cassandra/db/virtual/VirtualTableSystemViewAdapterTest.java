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
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.model.MeterMetricTestRow;
import org.apache.cassandra.db.virtual.model.MeterMetricTestRowWalker;
import org.apache.cassandra.db.virtual.sysview.SystemViewCollectionAdapter;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VirtualTableSystemViewAdapterTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String TABLE_NAME = "tablemetricscounter823764";
    private final List<VirtualTable> tables = new ArrayList<>();

    @Before
    public void config() throws Exception
    {
        tables.add(new VirtualTableSystemViewAdapter<>(
                SystemViewCollectionAdapter.create(
                        "meter.metrics",
                        "Description of meter.metrics",
                        new MeterMetricTestRowWalker(),
                        () -> CassandraMetricsRegistry.Metrics.getCounters().entrySet(),
                        MeterMetricTestRow::new),
                UnaryOperator.identity()));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, tables));
        startJMXServer();
    }

    @Test
    public void testSelectAll()
    {
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(TABLE_NAME));
        assertFalse(metrics.get().findAny().isPresent());

        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b DURATION);", TABLE_NAME);
        execute("INSERT INTO %s (a, b) VALUES (1, PT0S)");

        assertTrue(metrics.get().findAny().isPresent());

        int paging = (int) (Math.random() * 100 + 1);
        ResultSet result = executeNetWithPaging("SELECT * FROM vts.meter_metrics", paging);

        System.out.println(">>>> " + result.all());
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        String property = "java.version";
        String q = "SELECT * FROM vts.meter_metrics WHERE name = '" + property + '\'';
        assertRowsNet(executeNet(q), new Object[] {property});

    }

    @Test
    public void testSelectEmpty() throws Throwable
    {
        String q = "SELECT * FROM vts.meter_metrics WHERE name = 'EMPTY'";
        assertRowsNet(executeNet(q));
    }
}

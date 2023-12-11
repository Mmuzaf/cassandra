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

package org.apache.cassandra.db.virtual.model;

import com.codahale.metrics.Histogram;
import org.apache.cassandra.db.virtual.proc.Column;

import java.util.Map;

import static org.apache.cassandra.db.virtual.SystemViewsKeyspace.getMetricGroup;


/**
 * Historgam metric representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class HistogramMetricRow
{
    private final Map.Entry<String, Histogram> histogramEntry;

    public HistogramMetricRow(Map.Entry<String, Histogram> histogramEntry)
    {
        this.histogramEntry = histogramEntry;
    }

    @Column(index = 1)
    public String scope()
    {
        return getMetricGroup(histogramEntry.getKey());
    }

    @Column(index = 0, type = Column.Type.PARTITION_KEY)
    public String name()
    {
        return histogramEntry.getKey();
    }

    @Column(index = 3)
    public int size()
    {
        return histogramEntry.getValue().getSnapshot().size();
    }

    @Column(index = 4)
    public double p75th()
    {
        return histogramEntry.getValue().getSnapshot().get75thPercentile();
    }

    @Column(index = 5)
    public double p95th()
    {
        return histogramEntry.getValue().getSnapshot().get95thPercentile();
    }

    @Column(index = 6)
    public double p98th()
    {
        return histogramEntry.getValue().getSnapshot().get98thPercentile();
    }

    @Column(index = 7)
    public double p99th()
    {
        return histogramEntry.getValue().getSnapshot().get99thPercentile();
    }

    @Column(index = 8)
    public double p999th()
    {
        return histogramEntry.getValue().getSnapshot().get999thPercentile();
    }

    @Column(index = 9)
    public double max()
    {
        return histogramEntry.getValue().getSnapshot().getMax();
    }

    @Column(index = 10)
    public double mean()
    {
        return histogramEntry.getValue().getSnapshot().getMean();
    }

    @Column(index = 11)
    public double min()
    {
        return histogramEntry.getValue().getSnapshot().getMin();
    }
}

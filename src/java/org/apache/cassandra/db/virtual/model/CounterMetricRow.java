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

import com.codahale.metrics.Counter;
import org.apache.cassandra.db.virtual.proc.Column;

import java.util.Map;

import static org.apache.cassandra.db.virtual.SystemViewsKeyspace.getMetricGroup;


/**
 * Counter metric representation for a {@link org.apache.cassandra.db.virtual.VirtualTableSystemViewAdapter}.
 */
public class CounterMetricRow
{
    private final Map.Entry<String, Counter> counterEntry;

    public CounterMetricRow(Map.Entry<String, Counter> counterEntry)
    {
        this.counterEntry = counterEntry;
    }

    @Column(index = 0, type = Column.Type.PARTITION_KEY)
    public String name()
    {
        return counterEntry.getKey();
    }

    @Column(index = 1)
    public String scope()
    {
        return getMetricGroup(counterEntry.getKey());
    }

    @Column(index = 2)
    public long value()
    {
        return counterEntry.getValue().getCount();
    }
}

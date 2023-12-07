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

import com.codahale.metrics.Meter;
import org.apache.cassandra.db.virtual.proc.Column;

import java.util.Map;

import static org.apache.cassandra.db.virtual.SystemViewsKeyspace.getMetricGroup;


/**
 * Meter metric representation for a {@link org.apache.cassandra.db.virtual.sysview.SystemView}.
 */
public class MeterMetricRow
{
    private final Map.Entry<String, Meter> meterEntry;

    public MeterMetricRow(Map.Entry<String, Meter> meterEntry)
    {
        this.meterEntry = meterEntry;
    }

    @Column(index = 0)
    public String name()
    {
        return meterEntry.getKey();
    }

    @Column(index = 1)
    public String group()
    {
        return getMetricGroup(meterEntry.getKey());
    }

    @Column(index = 2)
    public long count()
    {
        return meterEntry.getValue().getCount();
    }

    @Column(index = 3)
    public double fifteenMinuteRate()
    {
        return meterEntry.getValue().getFifteenMinuteRate();
    }

    @Column(index = 4)
    public double fiveMinuteRate()
    {
        return meterEntry.getValue().getFiveMinuteRate();
    }

    @Column(index = 5)
    public double meanRate()
    {
        return meterEntry.getValue().getMeanRate();
    }

    @Column(index = 6)
    public double oneMinuteRate()
    {
        return meterEntry.getValue().getOneMinuteRate();
    }
}

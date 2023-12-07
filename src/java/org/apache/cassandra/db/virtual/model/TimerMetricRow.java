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

import com.codahale.metrics.Timer;
import org.apache.cassandra.db.virtual.proc.Column;

import java.util.Map;

import static org.apache.cassandra.db.virtual.SystemViewsKeyspace.getMetricGroup;


/**
 * Timer metric representation for a {@link org.apache.cassandra.db.virtual.sysview.SystemView}.
 */
public class TimerMetricRow
{
    private final Map.Entry<String, Timer> timerEntry;

    public TimerMetricRow(Map.Entry<String, Timer> timerEntry)
    {
        this.timerEntry = timerEntry;
    }

    @Column(index = 1)
    public String group()
    {
        return getMetricGroup(timerEntry.getKey());
    }

    @Column(index = 0)
    public String name()
    {
        return timerEntry.getKey();
    }

    @Column(index = 2)
    public long count()
    {
        return timerEntry.getValue().getCount();
    }

    @Column(index = 3)
    public double fifteenMinuteRate()
    {
        return timerEntry.getValue().getFifteenMinuteRate();
    }

    @Column(index = 4)
    public double fiveMinuteRate()
    {
        return timerEntry.getValue().getFiveMinuteRate();
    }

    @Column(index = 5)
    public double meanRate()
    {
        return timerEntry.getValue().getMeanRate();
    }

    @Column(index = 6)
    public double oneMinuteRate()
    {
        return timerEntry.getValue().getOneMinuteRate();
    }
}

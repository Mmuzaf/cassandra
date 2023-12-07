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

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.virtual.proc.Column;

import java.util.Map;

import static org.apache.cassandra.db.virtual.SystemViewsKeyspace.getMetricGroup;


/**
 * Gauge metric representation for a {@link org.apache.cassandra.db.virtual.sysview.SystemView}.
 */
@SuppressWarnings("rawtypes")
public class GaugeMetricRow
{
    private final Map.Entry<String, Gauge> gaugeEntry;

    public GaugeMetricRow(Map.Entry<String, Gauge> gaugeEntry)
    {
        this.gaugeEntry = gaugeEntry;
    }

    @Column(index = 0)
    public String name()
    {
        return gaugeEntry.getKey();
    }

    @Column(index = 1)
    public String group()
    {
        return getMetricGroup(gaugeEntry.getKey());
    }

    @Column(index = 2)
    public String value()
    {
        return gaugeEntry.getValue().getValue().toString();
    }
}

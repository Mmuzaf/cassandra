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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.cassandra.db.virtual.proc.Column;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * Metric row representation for a {@link org.apache.cassandra.db.virtual.sysview.SystemView}.
 */
public class MetricRow
{
    private final Map.Entry<String, Metric> metricEntry;

    public MetricRow(Map.Entry<String, Metric> metricEntry)
    {
        this.metricEntry = metricEntry;
    }

    @Column(index = 0)
    public String name()
    {
        return metricEntry.getKey();
    }

    @Column(index = 1)
    public String scope()
    {
        Optional<CassandraMetricsRegistry.MetricName> fullMetric = CassandraMetricsRegistry.Metrics.getAliases()
                .get(metricEntry.getKey())
                .stream()
                .findFirst();
        return fullMetric.map(CassandraMetricsRegistry.MetricName::getScope).orElse("Undefined");
    }

    @Column(index = 2)
    public String type()
    {
        Class<?> clazz = metricEntry.getValue().getClass();
        if (Counter.class.isAssignableFrom(clazz))
            return "counter";
        else if (Gauge.class.isAssignableFrom(clazz))
            return "gauge";
        else if (Histogram.class.isAssignableFrom(clazz))
            return "histogram";
        else if (Meter.class.isAssignableFrom(clazz))
            return "meter";
        else if (Timer.class.isAssignableFrom(clazz))
            return "timer";
        else
            throw new IllegalStateException("Unknown metric type: " + metricEntry.getValue().getClass());
    }

    @Column(index = 3)
    public String value()
    {
        Metric metric = metricEntry.getValue();

        if (metric instanceof Counter)
            return Long.toString(((Counter) metric).getCount());
        else if (metric instanceof Gauge)
            return ((Gauge) metric).getValue().toString();
        else if (metric instanceof Histogram)
            return Arrays.toString(((Histogram) metric).getSnapshot().getValues());
        else if (metric instanceof Meter)
            return Long.toString(((Meter) metric).getCount());
        else if (metric instanceof Timer)
            return Long.toString(((Timer) metric).getCount());
        else
            throw new IllegalStateException("Unknown metric type: " + metric.getClass().getName());
    }
}

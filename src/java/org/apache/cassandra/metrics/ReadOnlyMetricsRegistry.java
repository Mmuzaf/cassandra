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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Metrics registry that provides read-only access to the metrics.
 */
public interface ReadOnlyMetricsRegistry extends MetricSet
{
    /** @return The description of the registry. */
    String getDescription();

    /** @return A map of metric name constructed by {@link com.codahale.metrics.MetricRegistry#name(String, String...)}
     * and all of its Cassandra aliases represented as MetricNames. */
    Map<String, Set<CassandraMetricsRegistry.MetricName>> getAliases();

    /** @return A map of all the metric registries that are registered with this registry. */
    SortedMap<String, ? extends ReadOnlyMetricsRegistry> getRegisters();
    @SuppressWarnings("rawtypes")
    SortedMap<String, Gauge> getGauges();
    SortedMap<String, Counter> getCounters();
    SortedMap<String, Histogram> getHistograms();
    SortedMap<String, Meter> getMeters();
    SortedMap<String, Timer> getTimers();
}

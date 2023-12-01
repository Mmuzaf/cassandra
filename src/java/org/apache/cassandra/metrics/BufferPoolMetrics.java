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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.utils.memory.BufferPool;

public class BufferPoolMetrics
{
    /** Total number of hits */
    public final Meter hits;

    /** Total number of misses */
    public final Meter misses;

    /** Total threshold for a certain type of buffer pool*/
    public final Gauge<Long> capacity;

    /** Total size of buffer pools, in bytes, including overflow allocation */
    public final Gauge<Long> size;

    /** Total size, in bytes, of active buffered being used from the pool currently + overflow */
    public final Gauge<Long> usedSize;

    /**
     * Total size, in bytes, of direct or heap buffers allocated by the pool but not part of the pool
     * either because they are too large to fit or because the pool has exceeded its maximum limit or because it's
     * on-heap allocation.
     */
    public final Gauge<Long> overflowSize;

    public BufferPoolMetrics(String scope, BufferPool bufferPool)
    {
        CassandraMetricsRegistry group = CassandraMetricsRegistry.getOrCreateGroup(
                new DefaultNameFactory("BufferPool", scope),
                "Metrics for buffer pool: \"" + scope + "\".");
        hits = group.meterMetric("Hits");
        misses = group.meterMetric("Misses");
        capacity = group.registerMetric("Capacity", bufferPool::memoryUsageThreshold);
        overflowSize = group.registerMetric("OverflowSize", bufferPool::overflowMemoryInBytes);
        usedSize = group.registerMetric("UsedSize", bufferPool::usedSizeInBytes);
        size = group.registerMetric("Size", bufferPool::sizeInBytes);
    }
}

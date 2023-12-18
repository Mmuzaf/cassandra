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

import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.ResizableThreadPool;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

import static java.lang.String.format;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.metrics.DefaultNameFactory.GROUP_NAME;

/**
 * Metrics for {@link ThreadPoolExecutor}.
 */
public class ThreadPoolMetrics
{
    public static final String TYPE_NAME = "ThreadPools";
    public static final String ACTIVE_TASKS = "ActiveTasks";
    public static final String PENDING_TASKS = "PendingTasks";
    public static final String COMPLETED_TASKS = "CompletedTasks";
    public static final String CURRENTLY_BLOCKED_TASKS = "CurrentlyBlockedTasks";
    public static final String TOTAL_BLOCKED_TASKS = "TotalBlockedTasks";
    public static final String MAX_POOL_SIZE = "MaxPoolSize";
    public static final String MAX_TASKS_QUEUED = "MaxTasksQueued";

    /** Number of active tasks. */
    public final Gauge<Integer> activeTasks;

    /** Number of tasks waiting to be executed. */
    public final Gauge<Integer> pendingTasks;

    /** Number of completed tasks. */
    public final Gauge<Long> completedTasks;

    /**
     * Number of tasks currently blocked, waiting to be accepted by
     * the executor (because all threads are busy and the backing queue is full).
     */
    public final Counter currentBlocked;

    /** Number of tasks that had blocked before being accepted (or rejected). */
    public final Counter totalBlocked;

    /** Maximum number of threads before it will start queuing tasks */
    public final Gauge<Integer> maxPoolSize;

    /** Maximum number of tasks queued before a task get blocked */
    public final Gauge<Integer> maxTasksQueued;

    public final String path;
    public final String poolName;

    /**
     * Create metrics for given ThreadPoolExecutor.
     *
     * @param executor Thread pool
     * @param path Type of thread pool
     * @param poolName Name of thread pool to identify metrics
     */
    public ThreadPoolMetrics(ResizableThreadPool executor, String path, String poolName)
    {
        this.path = path;
        this.poolName = poolName;

        totalBlocked = new Counter();
        currentBlocked = new Counter();
        activeTasks = executor::getActiveTaskCount;
        pendingTasks = executor::getPendingTaskCount;
        completedTasks = executor::getCompletedTaskCount;
        maxPoolSize = executor::getMaximumPoolSize;
        maxTasksQueued = executor::getMaxTasksQueued;
    }

    public ThreadPoolMetrics register()
    {
        Metrics.register(makeMetricName(path, poolName, ACTIVE_TASKS), activeTasks);
        Metrics.register(makeMetricName(path, poolName, PENDING_TASKS), pendingTasks);
        Metrics.register(makeMetricName(path, poolName, COMPLETED_TASKS), completedTasks);
        Metrics.register(makeMetricName(path, poolName, CURRENTLY_BLOCKED_TASKS), currentBlocked);
        Metrics.register(makeMetricName(path, poolName, TOTAL_BLOCKED_TASKS), totalBlocked);
        Metrics.register(makeMetricName(path, poolName, MAX_POOL_SIZE), maxPoolSize);
        Metrics.register(makeMetricName(path, poolName, MAX_TASKS_QUEUED), maxTasksQueued);
        return Metrics.register(this);
    }

    public void release()
    {
        Metrics.remove(makeMetricName(path, poolName, ACTIVE_TASKS));
        Metrics.remove(makeMetricName(path, poolName, PENDING_TASKS));
        Metrics.remove(makeMetricName(path, poolName, COMPLETED_TASKS));
        Metrics.remove(makeMetricName(path, poolName, CURRENTLY_BLOCKED_TASKS));
        Metrics.remove(makeMetricName(path, poolName, TOTAL_BLOCKED_TASKS));
        Metrics.remove(makeMetricName(path, poolName, MAX_POOL_SIZE));
        Metrics.remove(makeMetricName(path, poolName, MAX_TASKS_QUEUED));
        Metrics.remove(this);
    }

    private static MetricName makeMetricName(String path, String poolName, String metricName)
    {
        MetricNameFactory factory = Metrics.registerMetricFactory(new ThreadPoolMetricNameFactory(path, poolName));
        return factory.createMetricName(metricName);
    }

    private static class ThreadPoolMetricNameFactory implements MetricNameFactory
    {
        private final String path;
        private final String poolName;

        ThreadPoolMetricNameFactory(String path, String poolName)
        {
            this.path = path;
            this.poolName = poolName;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String mbeanName = format("%s:type=%s,path=%s,scope=%s,name=%s",
                    GROUP_NAME, TYPE_NAME, path, poolName, metricName);
            return new CassandraMetricsRegistry.MetricName(GROUP_NAME, TYPE_NAME, metricName, path + '.' + poolName, mbeanName);
        }
    }
}

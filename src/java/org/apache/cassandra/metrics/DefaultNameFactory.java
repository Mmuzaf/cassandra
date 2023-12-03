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


import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * MetricNameFactory that generates default MetricName of metrics.
 */
public class DefaultNameFactory extends AbstractMetricNameFactory
{
    private final String scope;

    public DefaultNameFactory(String type)
    {
        this(type, null);
    }

    public DefaultNameFactory(String type, String scope)
    {
        super(type);
        this.scope = scope;
    }

    public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
    {
        return createMetricNameLocal(type, metricName, scope);
    }

    public static CassandraMetricsRegistry.MetricName createMetricName(String type, String metricName, String scope)
    {
        return Metrics.regsiterMetricFactory(new DefaultNameFactory(type, scope),
                        "Metrics group for \"" + type + '"')
                .createMetricName(metricName);
    }

    protected static CassandraMetricsRegistry.MetricName createMetricNameLocal(String type, String metricName, String scope)
    {
        return new CassandraMetricsRegistry.MetricName(GROUP_NAME, type, metricName, scope, createDefaultMBeanName(type, metricName, scope));
    }

    protected static String createDefaultMBeanName(String type, String name, String scope)
    {
        final StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(GROUP_NAME);
        nameBuilder.append(":type=");
        nameBuilder.append(type);
        if (scope != null)
        {
            nameBuilder.append(",scope=");
            nameBuilder.append(scope);
        }
        if (name.length() > 0)
        {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }
        return nameBuilder.toString();
    }
}

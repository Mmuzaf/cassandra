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
import com.codahale.metrics.RatioGauge;
import org.apache.cassandra.cql3.QueryProcessor;

public class CQLMetrics
{
    private static final CassandraMetricsRegistry group = CassandraMetricsRegistry.getOrCreateGroup(
            new DefaultNameFactory("CQL"),
            "Metrics specific to CQL prepared statement caching.");
    public final Counter regularStatementsExecuted;
    public final Counter preparedStatementsExecuted;
    public final Counter preparedStatementsEvicted;
    public final Counter useStatementsExecuted;
    public final Gauge<Integer> preparedStatementsCount;
    public final Gauge<Double> preparedStatementsRatio;

    public CQLMetrics()
    {
        regularStatementsExecuted = group.counterMetric("RegularStatementsExecuted");
        preparedStatementsExecuted = group.counterMetric("PreparedStatementsExecuted");
        preparedStatementsEvicted = group.counterMetric("PreparedStatementsEvicted");
        useStatementsExecuted = group.counterMetric("UseStatementsExecuted");
        preparedStatementsCount = group.registerMetric("PreparedStatementsCount", QueryProcessor::preparedStatementsCount);
        preparedStatementsRatio = group.registerMetric("PreparedStatementsRatio", new RatioGauge()
        {
            public Ratio getRatio()
            {
                return Ratio.of(getNumerator(), getDenominator());
            }

            public double getNumerator()
            {
                return preparedStatementsExecuted.getCount();
            }

            public double getDenominator()
            {
                return regularStatementsExecuted.getCount() + preparedStatementsExecuted.getCount();
            }
        });
    }
}

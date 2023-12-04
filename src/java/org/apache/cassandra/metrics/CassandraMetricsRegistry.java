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
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.utils.MBeanWrapper;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static org.apache.cassandra.metrics.AbstractMetricNameFactory.GROUP_NAME;

/**
 * Makes integrating 3.0 metrics API with 2.0.
 * <p>
 * The 3.0 API comes with poor JMX integration
 * </p>
 */
public class CassandraMetricsRegistry extends MetricRegistry implements AliasedMetricSet
{
    /** A map of metric name constructed by {@link com.codahale.metrics.MetricRegistry#name(String, String...)} and
     * its full name in the way how it is represented in JMX. The map is used by {@link CassandraJmxMetricsExporter}
     * to export metrics to JMX. */
    private static final ConcurrentMap<String, Set<MetricName>> ALIASES = new ConcurrentHashMap<>();
    /** A map of metric name constructed by {@link org.apache.cassandra.metrics.MetricNameFactory} and the factory name. */
    private static final ConcurrentMap<MetricName, String> METRIC_TO_FACTORY_NAME_MAP = new ConcurrentHashMap<>();
    private static final Map<String, CassandraMetricsRegistry> REGISTERS = new ConcurrentHashMap<>();

    public static final CassandraMetricsRegistry Metrics = new CassandraMetricsRegistry("Root metrics registry",
            r -> new CassandraJmxMetricsExporter(name ->
                    ofNullable(r.getAliases().get(name)).orElse(Collections.emptySet())
                            .stream()
                            .map(MetricName::getMBeanName)
                            .collect(Collectors.toSet())));

    private final Map<String, ThreadPoolMetrics> threadPoolMetrics = new ConcurrentHashMap<>();
    private final String description;
    public final static TimeUnit DEFAULT_TIMER_UNIT = TimeUnit.MICROSECONDS;

    static
    {
        // Load all metric classes to ensure they register themselves.
        BatchMetrics metrics = BatchMetrics.instance;
    }

    private CassandraMetricsRegistry(
            String description,
            Function<CassandraMetricsRegistry, MetricRegistryListener> listenerFactory)
    {
        this.description = description;
        addListener(listenerFactory.apply(this));
    }

    public SortedMap<String, CassandraMetricsRegistry> getRegisters()
    {
        return REGISTERS.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, java.util.TreeMap::new));
    }

    public MetricNameFactory regsiterMetricFactory(MetricNameFactory factory, String description)
    {
        MetricNameFactory factoryWrapper = new MetricNameFactoryWrapper(factory,
                createdMetricName -> {
                    ALIASES.computeIfAbsent(createdMetricName.getMetricName(),
                                    k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                            .add(createdMetricName);
                    String old = METRIC_TO_FACTORY_NAME_MAP.putIfAbsent(createdMetricName, factory.groupName());
                    assert old == null || old.equals(factory.groupName());
                    return createdMetricName;
                });
        REGISTERS.computeIfAbsent(factoryWrapper.groupName(),
                name -> new CassandraMetricsRegistry(description,
                        r -> new DelegateMetricsListener(CassandraMetricsRegistry.Metrics)));
        return factoryWrapper;
    }

    private static CassandraMetricsRegistry getMetricGroupByName(MetricName metricName)
    {
        assertKnownMetric(metricName);
        return REGISTERS.get(METRIC_TO_FACTORY_NAME_MAP.get(metricName));
    }

    private static void assertKnownMetric(MetricName name)
    {
        if (!METRIC_TO_FACTORY_NAME_MAP.containsKey(name))
            throw new IllegalArgumentException(name + " is not a registered metric: " + Metrics.METRIC_TO_FACTORY_NAME_MAP.keySet());
    }

    @Override
    public Map<String, Set<MetricName>> getAliases()
    {
        return ALIASES;
    }

    public String getDescription()
    {
        return description;
    }

    public Counter counter(MetricName... name)
    {
        Arrays.asList(name).forEach(CassandraMetricsRegistry::assertKnownMetric);
        return getMetricGroupByName(name[0]).counter(name[0].getMetricName());
    }

    public Meter meter(MetricName name)
    {
        return getMetricGroupByName(name).meter(name.getMetricName());
    }

    public Meter meter(MetricName name, MetricName alias)
    {
        Meter meter = meter(name);
        registerAlias(name, alias);
        return meter;
    }

    public Histogram histogram(MetricName name, boolean considerZeroes)
    {
        return register(name, new ClearableHistogram(new DecayingEstimatedHistogramReservoir(considerZeroes)));
    }

    public Histogram histogram(MetricName name, MetricName alias, boolean considerZeroes)
    {
        Histogram histogram = histogram(name, considerZeroes);
        registerAlias(name, alias);
        return histogram;
    }

    public Timer timer(MetricName name)
    {
        return timer(name, DEFAULT_TIMER_UNIT);
    }

    public SnapshottingTimer timer(MetricName name, MetricName alias)
    {
        return timer(name, alias, DEFAULT_TIMER_UNIT);
    }

    private SnapshottingTimer timer(MetricName name, TimeUnit durationUnit)
    {
        return getMetricGroupByName(name).register(name, new SnapshottingTimer(CassandraMetricsRegistry.createReservoir(durationUnit)));
    }

    public SnapshottingTimer timer(MetricName name, MetricName alias, TimeUnit durationUnit)
    {
        SnapshottingTimer timer = getMetricGroupByName(name).timer(name, durationUnit);
        registerAlias(name, alias);
        return timer;
    }

    public static SnapshottingReservoir createReservoir(TimeUnit durationUnit)
    {
        SnapshottingReservoir reservoir;
        if (durationUnit != TimeUnit.NANOSECONDS)
        {
            SnapshottingReservoir underlying = new DecayingEstimatedHistogramReservoir(DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION,
                                                                           DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT,
                                                                           DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT);
            // fewer buckets should suffice if timer is not based on nanos
            reservoir = new ScalingReservoir(underlying,
                                             // timer update values in nanos.
                                             v -> durationUnit.convert(v, TimeUnit.NANOSECONDS));
        }
        else
        {
            // Use more buckets if timer is created with nanos resolution.
            reservoir = new DecayingEstimatedHistogramReservoir();
        }
        return reservoir;
    }

    public <T extends Metric> T register(MetricName name, T metric)
    {
        try
        {
            return getMetricGroupByName(name).register(name.getMetricName(), metric);
        }
        catch (IllegalArgumentException e)
        {
            Metric existing = Metrics.getMetrics().get(name.getMetricName());
            return (T)existing;
        }
    }

    public Collection<ThreadPoolMetrics> allThreadPoolMetrics()
    {
        return Collections.unmodifiableCollection(threadPoolMetrics.values());
    }

    public Optional<ThreadPoolMetrics> getThreadPoolMetrics(String poolName)
    {
        return ofNullable(threadPoolMetrics.get(poolName));
    }

    ThreadPoolMetrics register(ThreadPoolMetrics metrics)
    {
        threadPoolMetrics.put(metrics.poolName, metrics);
        return metrics;
    }

    void remove(ThreadPoolMetrics metrics)
    {
        threadPoolMetrics.remove(metrics.poolName, metrics);
    }

    public <T extends Metric> T register(MetricName name, MetricName aliasName, T metric)
    {
        T ret = register(name, metric);
        registerAlias(name, aliasName);
        return ret;
    }

    public <T extends Metric> T register(MetricName name, T metric, MetricName... aliases)
    {
        T ret = register(name, metric);
        for (MetricName aliasName : aliases)
        {
            registerAlias(name, aliasName);
        }
        return ret;
    }

    public boolean remove(MetricName name)
    {
        // TODO remove aliases from alias map
        return remove(name.getMetricName());
    }

    public boolean remove(MetricName name, MetricName... aliases)
    {
        return remove(name);
    }

    // TODO cleanup
    public void registerMBean(Metric metric, ObjectName name)
    {
        registerMBean(metric, name, MBeanWrapper.instance);
    }

    public void registerMBean(Metric metric, ObjectName name, MBeanWrapper mBeanServer)
    {
        AbstractBean mbean;

        if (metric instanceof Gauge)
            mbean = new JmxGauge((Gauge<?>) metric, name);
        else if (metric instanceof Counter)
            mbean = new JmxCounter((Counter) metric, name);
        else if (metric instanceof Histogram)
            mbean = new JmxHistogram((Histogram) metric, name);
        else if (metric instanceof Timer)
            mbean = new JmxTimer((Timer) metric, name, TimeUnit.SECONDS, DEFAULT_TIMER_UNIT);
        else if (metric instanceof Metered)
            mbean = new JmxMeter((Metered) metric, name, TimeUnit.SECONDS);
        else
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());

        if (mBeanServer != null && !mBeanServer.isRegistered(name))
            mBeanServer.registerMBean(mbean, name, MBeanWrapper.OnException.LOG);
    }

    private void registerAlias(MetricName existingName, MetricName aliasName)
    {
        Metric existing = Metrics.getMetrics().get(existingName.getMetricName());
        assert existing != null : existingName + " not registered";

        registerMBean(existing, aliasName.getMBeanName());
    }
    
    /**
     * Strips a single final '$' from input
     * 
     * @param s String to strip
     * @return a string with one less '$' at end
     */
    private static String withoutFinalDollar(String s)
    {
        int l = s.length();
        return (l!=0 && '$' == s.charAt(l-1))?s.substring(0,l-1):s;
    }

    public interface MetricMBean
    {
        ObjectName objectName();
    }

    private abstract static class AbstractBean implements MetricMBean
    {
        private final ObjectName objectName;

        AbstractBean(ObjectName objectName)
        {
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName()
        {
            return objectName;
        }
    }


    public interface JmxGaugeMBean extends MetricMBean
    {
        Object getValue();
    }

    private static class JmxGauge extends AbstractBean implements JmxGaugeMBean
    {
        private final Gauge<?> metric;

        private JmxGauge(Gauge<?> metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public Object getValue()
        {
            return metric.getValue();
        }
    }

    public interface JmxHistogramMBean extends MetricMBean
    {
        long getCount();

        long getMin();

        long getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        long[] getRecentValues();
    }

    private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean
    {
        private final Histogram metric;
        private long[] last = null;

        private JmxHistogram(Histogram metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public double get50thPercentile()
        {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }

        @Override
        public long getMin()
        {
            return metric.getSnapshot().getMin();
        }

        @Override
        public long getMax()
        {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean()
        {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev()
        {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile()
        {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile()
        {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile()
        {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile()
        {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile()
        {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values()
        {
            return metric.getSnapshot().getValues();
        }

        /**
         * Returns a histogram describing the values recorded since the last time this method was called.
         *
         * ex. If the counts are [0, 1, 2, 1] at the time the first caller arrives, but change to [1, 2, 3, 2] by the 
         * time a second caller arrives, the second caller will receive [1, 1, 1, 1].
         *
         * @return a histogram whose bucket offsets are assumed to be in nanoseconds
         */
        @Override
        public synchronized long[] getRecentValues()
        {
            long[] now = metric.getSnapshot().getValues();
            long[] delta = delta(now, last);
            last = now;
            return delta;
        }
    }

    public interface JmxCounterMBean extends MetricMBean
    {
        long getCount();
    }

    private static class JmxCounter extends AbstractBean implements JmxCounterMBean
    {
        private final Counter metric;

        private JmxCounter(Counter metric, ObjectName objectName)
        {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }
    }

    public interface JmxMeterMBean extends MetricMBean
    {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }

    private static class JmxMeter extends AbstractBean implements JmxMeterMBean
    {
        private final Metered metric;
        private final double rateFactor;
        private final String rateUnit;

        private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit)
        {
            super(objectName);
            this.metric = metric;
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = "events/" + calculateRateUnit(rateUnit);
        }

        @Override
        public long getCount()
        {
            return metric.getCount();
        }

        @Override
        public double getMeanRate()
        {
            return metric.getMeanRate() * rateFactor;
        }

        @Override
        public double getOneMinuteRate()
        {
            return metric.getOneMinuteRate() * rateFactor;
        }

        @Override
        public double getFiveMinuteRate()
        {
            return metric.getFiveMinuteRate() * rateFactor;
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return metric.getFifteenMinuteRate() * rateFactor;
        }

        @Override
        public String getRateUnit()
        {
            return rateUnit;
        }

        private String calculateRateUnit(TimeUnit unit)
        {
            final String s = unit.toString().toLowerCase(Locale.US);
            return s.substring(0, s.length() - 1);
        }
    }

    public interface JmxTimerMBean extends JmxMeterMBean
    {
        double getMin();

        double getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        long[] getRecentValues();

        String getDurationUnit();
    }

    static class JmxTimer extends JmxMeter implements JmxTimerMBean
    {
        private final Timer metric;
        private final String durationUnit;
        private long[] last = null;

        private JmxTimer(Timer metric,
                         ObjectName objectName,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit)
        {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }

        @Override
        public double get50thPercentile()
        {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public double getMin()
        {
            return metric.getSnapshot().getMin();
        }

        @Override
        public double getMax()
        {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean()
        {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev()
        {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile()
        {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile()
        {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile()
        {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile()
        {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile()
        {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values()
        {
            return metric.getSnapshot().getValues();
        }

        /**
         * Returns a histogram describing the values recorded since the last time this method was called.
         * 
         * ex. If the counts are [0, 1, 2, 1] at the time the first caller arrives, but change to [1, 2, 3, 2] by the 
         * time a second caller arrives, the second caller will receive [1, 1, 1, 1].
         * 
         * @return a histogram whose bucket offsets are assumed to be in nanoseconds
         */
        @Override
        public synchronized long[] getRecentValues()
        {
            long[] now = metric.getSnapshot().getValues();
            long[] delta = delta(now, last);
            last = now;
            return delta;
        }

        @Override
        public String getDurationUnit()
        {
            return durationUnit;
        }
    }

    /**
     * Used to determine the changes in a histogram since the last time checked.
     *
     * @param now The current histogram
     * @param last The previous value of the histogram
     * @return the difference between <i>now</i> and <i>last</i>
     */
    @VisibleForTesting
    static long[] delta(long[] now, long[] last)
    {
        long[] delta = new long[now.length];
        if (last == null)
        {
            last = new long[now.length];
        }
        for(int i = 0; i< now.length; i++)
        {
            delta[i] = now[i] - (i < last.length? last[i] : 0);
        }
        return delta;
    }

    /**
     * A value class encapsulating a metric's owning class and name.
     */
    public static class MetricName implements Comparable<MetricName>
    {
        public static final MetricName EMPTY = new MetricName(GROUP_NAME, "Root", "", "", "");
        private final String group;
        private final String type;
        private final String name;
        private final String scope;
        private final String mBeanName;

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param klass the {@link Class} to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         */
        public MetricName(Class<?> klass, String name)
        {
            this(klass, name, null);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group the group to which the {@link Metric} belongs
         * @param type  the type to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         */
        public MetricName(String group, String type, String name)
        {
            this(group, type, name, null);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param klass the {@link Class} to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         * @param scope the scope of the {@link Metric}
         */
        public MetricName(Class<?> klass, String name, String scope)
        {
            this(klass.getPackage() == null ? "" : klass.getPackage().getName(),
                    withoutFinalDollar(klass.getSimpleName()),
                    name,
                    scope);
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group the group to which the {@link Metric} belongs
         * @param type  the type to which the {@link Metric} belongs
         * @param name  the name of the {@link Metric}
         * @param scope the scope of the {@link Metric}
         */
        public MetricName(String group, String type, String name, String scope)
        {
            this(group, type, name, scope, createMBeanName(group, type, name, scope));
        }

        /**
         * Creates a new {@link MetricName} without a scope.
         *
         * @param group     the group to which the {@link Metric} belongs
         * @param type      the type to which the {@link Metric} belongs
         * @param name      the name of the {@link Metric}
         * @param scope     the scope of the {@link Metric}
         * @param mBeanName the 'ObjectName', represented as a string, to use when registering the
         *                  MBean.
         */
        public MetricName(String group, String type, String name, String scope, String mBeanName)
        {
            if (group == null || type == null)
            {
                throw new IllegalArgumentException("Both group and type need to be specified");
            }
            if (name == null)
            {
                throw new IllegalArgumentException("Name needs to be specified");
            }
            this.group = group;
            this.type = type;
            this.name = name;
            this.scope = scope;
            this.mBeanName = mBeanName;
        }

        /**
         * Returns the group to which the {@link Metric} belongs. For class-based metrics, this will be
         * the package name of the {@link Class} to which the {@link Metric} belongs.
         *
         * @return the group to which the {@link Metric} belongs
         */
        public String getGroup()
        {
            return group;
        }

        /**
         * Returns the type to which the {@link Metric} belongs. For class-based metrics, this will be
         * the simple class name of the {@link Class} to which the {@link Metric} belongs.
         *
         * @return the type to which the {@link Metric} belongs
         */
        public String getType()
        {
            return type;
        }

        /**
         * Returns the name of the {@link Metric}.
         *
         * @return the name of the {@link Metric}
         */
        public String getName()
        {
            return name;
        }

        public String getMetricName()
        {
            return MetricRegistry.name(group, type, name, scope);
        }

        /**
         * Returns the scope of the {@link Metric}.
         *
         * @return the scope of the {@link Metric}
         */
        public String getScope()
        {
            return scope;
        }

        /**
         * Returns {@code true} if the {@link Metric} has a scope, {@code false} otherwise.
         *
         * @return {@code true} if the {@link Metric} has a scope
         */
        public boolean hasScope()
        {
            return scope != null;
        }

        /**
         * Returns the MBean name for the {@link Metric} identified by this metric name.
         *
         * @return the MBean name
         */
        public ObjectName getMBeanName()
        {

            String mname = mBeanName;

            if (mname == null)
                mname = getMetricName();

            try
            {

                return new ObjectName(mname);
            } catch (MalformedObjectNameException e)
            {
                try
                {
                    return new ObjectName(ObjectName.quote(mname));
                } catch (MalformedObjectNameException e1)
                {
                    throw new RuntimeException(e1);
                }
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final MetricName that = (MetricName) o;
            return mBeanName.equals(that.mBeanName);
        }

        @Override
        public int hashCode()
        {
            return mBeanName.hashCode();
        }

        @Override
        public String toString()
        {
            return mBeanName;
        }

        @Override
        public int compareTo(MetricName o)
        {
            return mBeanName.compareTo(o.mBeanName);
        }

        private static String createMBeanName(String group, String type, String name, String scope)
        {
            final StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(ObjectName.quote(group));
            nameBuilder.append(":type=");
            nameBuilder.append(ObjectName.quote(type));
            if (scope != null)
            {
                nameBuilder.append(",scope=");
                nameBuilder.append(ObjectName.quote(scope));
            }
            if (name.length() > 0)
            {
                nameBuilder.append(",name=");
                nameBuilder.append(ObjectName.quote(name));
            }
            return nameBuilder.toString();
        }

        /**
         * If the group is empty, use the package name of the given class. Otherwise use group
         *
         * @param group The group to use by default
         * @param klass The class being tracked
         * @return a group for the metric
         */
        public static String chooseGroup(String group, Class<?> klass)
        {
            if (group == null || group.isEmpty())
            {
                group = klass.getPackage() == null ? "" : klass.getPackage().getName();
            }
            return group;
        }

        /**
         * If the type is empty, use the simple name of the given class. Otherwise use type
         *
         * @param type  The type to use by default
         * @param klass The class being tracked
         * @return a type for the metric
         */
        public static String chooseType(String type, Class<?> klass)
        {
            if (type == null || type.isEmpty())
            {
                type = withoutFinalDollar(klass.getSimpleName());
            }
            return type;
        }

        /**
         * If name is empty, use the name of the given method. Otherwise use name
         *
         * @param name   The name to use by default
         * @param method The method being tracked
         * @return a name for the metric
         */
        public static String chooseName(String name, Method method)
        {
            if (name == null || name.isEmpty())
            {
                name = method.getName();
            }
            return name;
        }
    }

    private static class CassandraJmxMetricsExporter implements MetricRegistryListener
    {
        private final MBeanWrapper mBeanWrapper = MBeanWrapper.instance;
        private final java.util.function.Function<String, Set<ObjectName>> mapper;

        public CassandraJmxMetricsExporter(java.util.function.Function<String, Set<ObjectName>> mapper)
        {
            this.mapper = mapper;
        }

        private void each(String metricName, Consumer<ObjectName> consumer)
        {
            mapper.apply(metricName).forEach(consumer);
        }

        @Override
        public void onGaugeAdded(String metricName, Gauge<?> gauge)
        {
            each(metricName, objName -> Metrics.registerMBean(gauge, objName, mBeanWrapper));
        }

        @Override
        public void onGaugeRemoved(String metricName)
        {
            each(metricName, objName -> mBeanWrapper.unregisterMBean(objName, MBeanWrapper.OnException.IGNORE));
        }

        @Override
        public void onCounterAdded(String metricName, Counter counter)
        {
            each(metricName, objName -> Metrics.registerMBean(counter, objName, mBeanWrapper));
        }

        @Override
        public void onCounterRemoved(String metricName)
        {
            each(metricName, objName -> mBeanWrapper.unregisterMBean(objName, MBeanWrapper.OnException.IGNORE));
        }

        @Override
        public void onHistogramAdded(String metricName, Histogram histogram)
        {
            each(metricName, objName -> Metrics.registerMBean(histogram, objName, mBeanWrapper));
        }

        @Override
        public void onHistogramRemoved(String metricName)
        {
            each(metricName, objName -> mBeanWrapper.unregisterMBean(objName, MBeanWrapper.OnException.IGNORE));
        }

        @Override
        public void onMeterAdded(String metricName, Meter meter)
        {
            each(metricName, objName -> Metrics.registerMBean(meter, objName, mBeanWrapper));
        }

        @Override
        public void onMeterRemoved(String metricName)
        {
            each(metricName, objName -> mBeanWrapper.unregisterMBean(objName, MBeanWrapper.OnException.IGNORE));
        }

        @Override
        public void onTimerAdded(String metricName, Timer timer)
        {
            each(metricName, objName -> Metrics.registerMBean(timer, objName, mBeanWrapper));
        }

        @Override
        public void onTimerRemoved(String metricName)
        {
            each(metricName, objName -> mBeanWrapper.unregisterMBean(objName, MBeanWrapper.OnException.IGNORE));
        }
    }

    private static class DelegateMetricsListener implements MetricRegistryListener
    {
        private final MetricRegistry delegate;

        public DelegateMetricsListener(MetricRegistry delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void onGaugeAdded(String name, Gauge<?> gauge)
        {
            delegate.gauge(name, () -> gauge);
        }

        @Override
        public void onGaugeRemoved(String name)
        {
            delegate.remove(name);
        }

        @Override
        public void onCounterAdded(String name, Counter counter)
        {
            delegate.counter(name, () -> counter);
        }

        @Override
        public void onCounterRemoved(String name)
        {
            delegate.remove(name);
        }

        @Override
        public void onHistogramAdded(String name, Histogram histogram)
        {
            delegate.histogram(name, () -> histogram);
        }

        @Override
        public void onHistogramRemoved(String name)
        {
            delegate.remove(name);
        }

        @Override
        public void onMeterAdded(String name, Meter meter)
        {
            delegate.meter(name, () -> meter);
        }

        @Override
        public void onMeterRemoved(String name)
        {
            delegate.remove(name);
        }

        @Override
        public void onTimerAdded(String name, Timer timer)
        {
            delegate.timer(name, () -> timer);
        }

        @Override
        public void onTimerRemoved(String name)
        {
            delegate.remove(name);
        }
    }

    private static class MetricNameFactoryWrapper implements MetricNameFactory
    {
        private final MetricNameFactory delegate;
        private final UnaryOperator<MetricName> handler;

        public MetricNameFactoryWrapper(MetricNameFactory delegate, UnaryOperator<MetricName> handler)
        {
            this.delegate = delegate;
            this.handler = handler;
        }

        @Override
        public MetricName createMetricName(String metricName)
        {
            return handler.apply(delegate.createMetricName(metricName));
        }

        @Override
        public String groupName()
        {
            return delegate.groupName();
        }
    }
}



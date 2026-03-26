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

package org.apache.cassandra.management;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.AuditLogManagerMBean;
import org.apache.cassandra.auth.AbstractCIDRAuthorizer;
import org.apache.cassandra.auth.AuthCache;
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.CIDRGroupsMappingManagerMBean;
import org.apache.cassandra.auth.CIDRPermissionsManagerMBean;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compression.CompressionDictionaryManagerMBean;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTable;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTableMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.hints.HintsServiceMBean;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.LocationInfo;
import org.apache.cassandra.locator.LocationInfoMBean;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.profiler.AsyncProfilerMBean;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.AsyncProfilerService;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.AutoRepairServiceMBean;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCInspectorMXBean;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.service.accord.AccordOperations;
import org.apache.cassandra.service.accord.AccordOperationsMBean;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotManagerMBean;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.CMSOperationsMBean;
import org.apache.cassandra.tools.RemoteJmxMBeanAccessor;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.service.CassandraDaemon.SKIP_GC_INSPECTOR;

/**
 * Server-side implementation of {@link MBeanAccessor} for in-process execution.
 *
 * <p>
 * This implementation provides direct access to MBean instances without using JMX,
 * eliminating the need for remote connections or JMX proxies. It is designed for
 * server-side command execution as part of CEP-38 Management API, where commands run
 * in the same JVM as the Cassandra daemon.
 *
 * <p>
 * Unlike {@link RemoteJmxMBeanAccessor}, this implementation:
 * <ul>
 *   <li>Directly accesses singleton instances (e.g., {@code StorageService.instance})</li>
 *   <li>Does not require network connections or JMX connectors</li>
 *   <li>Provides better performance by avoiding serialization/deserialization</li>
 *   <li>Has no connection state to manage</li>
 *   <li>Works directly with {@link Keyspace} and {@link ColumnFamilyStore} instances</li>
 * </ul>
 *
 * <p>
 * <b>Lazy Initialization</b>: MBean providers are initialized registered for known MBeans.
 *
 * @see MBeanAccessor
 * @see RemoteJmxMBeanAccessor
 * @since 5.1
 */
public class InternalNodeMBeanAccessor implements MBeanAccessor
{
    private final Map<Class<?>, MBeanProvider<?>> mBeanProviders = new ConcurrentHashMap<>();
    private final Map<Class<?>, Object> mBeanCache = new ConcurrentHashMap<>();
    private final Map<String, Object> metricCache = new ConcurrentHashMap<>();

    /**
     * Creates a new InternalNodeMBeanAccessor using direct instance access.
     */
    public InternalNodeMBeanAccessor()
    {
        initializeMBeanProviders();
    }

    /**
     * Initializes all statically known MBean instances.
     */
    private void initializeMBeanProviders()
    {
        registerMBeanProvider(AccordOperationsMBean.class, () -> AccordOperations.instance);
        registerMBeanProvider(ActiveRepairServiceMBean.class, ActiveRepairService::instance);
        registerMBeanProvider(AuditLogManagerMBean.class, () -> AuditLogManager.instance);
        registerMBeanProvider(AutoRepairServiceMBean.class, () -> AutoRepairService.instance);
        registerMBeanProvider(BatchlogManagerMBean.class, () -> BatchlogManager.instance);
        registerMBeanProvider(CMSOperationsMBean.class, () -> CMSOperations.instance);
        registerMBeanProvider(CacheServiceMBean.class, () -> CacheService.instance);
        registerMBeanProvider(CompactionManagerMBean.class, () -> CompactionManager.instance);
        registerMBeanProvider(DynamicEndpointSnitchMBean.class, this::resolveDynamicEndpointSnitch);
        registerMBeanProvider(FailureDetectorMBean.class, () -> (FailureDetectorMBean) FailureDetector.instance);
        registerMBeanProvider(GCInspectorMXBean.class, this::resolveGCInspector);
        registerMBeanProvider(GossiperMBean.class, () -> Gossiper.instance);
        registerMBeanProvider(GuardrailsMBean.class, () -> Guardrails.instance);
        registerMBeanProvider(HintsServiceMBean.class, () -> HintsService.instance);
        registerMBeanProvider(MemoryMXBean.class, ManagementFactory::getMemoryMXBean);
        registerMBeanProvider(MessagingServiceMBean.class, MessagingService::instance);
        registerMBeanProvider(RuntimeMXBean.class, ManagementFactory::getRuntimeMXBean);
        registerMBeanProvider(SnapshotManagerMBean.class, () -> SnapshotManager.instance);
        registerMBeanProvider(StorageProxyMBean.class, () -> StorageProxy.instance);
        registerMBeanProvider(StorageServiceMBean.class, () -> StorageService.instance);
        registerMBeanProvider(StreamManagerMBean.class, () -> StreamManager.instance);
        registerMBeanProvider(AsyncProfilerMBean.class, AsyncProfilerService::instance);

        // Utility MBeans are stateless and can be created on demand.
        // They query DatabaseDescriptor for the current state, so new instances are fine
        registerMBeanProvider(EndpointSnitchInfoMBean.class, EndpointSnitchInfo::new);
        registerMBeanProvider(LocationInfoMBean.class, LocationInfo::new);

        // AuthCache MBeans
        registerMBeanProvider(AuthorizationProxy.JmxPermissionsCacheMBean.class,
                              () -> findAuthCache(AuthorizationProxy.JmxPermissionsCacheMBean.class));
        registerMBeanProvider(NetworkPermissionsCacheMBean.class,
                              () -> findAuthCache(NetworkPermissionsCacheMBean.class));
        registerMBeanProvider(PasswordAuthenticator.CredentialsCacheMBean.class,
                              () -> findAuthCache(PasswordAuthenticator.CredentialsCacheMBean.class));
        registerMBeanProvider(PermissionsCacheMBean.class,
                              () -> findAuthCache(PermissionsCacheMBean.class));
        registerMBeanProvider(RolesCacheMBean.class,
                              () -> findAuthCache(RolesCacheMBean.class));

        // CIDR Auth MBeans
        registerMBeanProvider(CIDRFilteringMetricsTableMBean.class, () -> CIDRFilteringMetricsTable.instance);
        registerMBeanProvider(CIDRGroupsMappingManagerMBean.class, () -> AbstractCIDRAuthorizer.cidrGroupsMappingManager);
        registerMBeanProvider(CIDRPermissionsManagerMBean.class, () -> AbstractCIDRAuthorizer.cidrPermissionsManager);
    }

    /**
     * Gets DynamicEndpointSnitch from DatabaseDescriptor if it's a DynamicEndpointSnitch,
     * otherwise returns null.
     */
    private DynamicEndpointSnitchMBean resolveDynamicEndpointSnitch()
    {
        if (!DatabaseDescriptor.isDynamicEndpointSnitch())
            throw new IllegalStateException("DynamicEndpointSnitch has been requested but is not enabled");

        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        assert proximity instanceof DynamicEndpointSnitch;

        return (DynamicEndpointSnitchMBean) proximity;
    }

    private GCInspectorMXBean resolveGCInspector()
    {
        if (SKIP_GC_INSPECTOR)
            throw new IllegalStateException("GCInspector has been requested but is disabled via SKIP_GC_INSPECTOR flag");

        try
        {
            MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
            if (mbs == null)
                return null;

            ObjectName name = new ObjectName(GCInspector.MBEAN_NAME);
            if (mbs.isRegistered(name))
                return JMX.newMBeanProxy(mbs, name, GCInspectorMXBean.class);
        }
        catch (Exception e)
        {
            // Fall through to create a new instance
        }

        return null;
    }

    /** Finds an auth cache MBean instance from AuthCacheService. */
    private <T> T findAuthCache(Class<T> clazz)
    {
        Set<AuthCache<?, ?>> caches = AuthCacheService.instance.getCaches();
        if (caches.isEmpty())
            return null;

        AuthCacheFinder visitor = new AuthCacheFinder(clazz);
        for (AuthCache<?, ?> cache : caches)
        {
            cache.accept(visitor);
            Object found = visitor.getCache();
            if (found == null)
                continue;
            return clazz.cast(found);
        }
        return null;
    }

    private <T> void registerMBeanProvider(Class<T> clazz, MBeanProvider<T> locator)
    {
        Object prev = mBeanProviders.putIfAbsent(clazz, locator);
        assert prev == null : "MBean locator for " + clazz.getName() + " is already registered";
    }

    @Override
    public <T> T findMBean(Class<T> clazz)
    {
        Object cached = mBeanCache.get(clazz);
        if (cached != null)
            return clazz.cast(cached);

        Object prev =  mBeanCache.computeIfAbsent(clazz, k -> {
            MBeanProvider<?> provider = mBeanProviders.get(k);
            return provider == null ? null : provider.provide();
        });
        if (prev == null)
            throw new RuntimeException("MBean of type " + clazz.getName() + " is not registered");
        return clazz.cast(prev);
    }

    @Override
    public <T> T findMBeanMetric(Class<T> clazz, Props props)
    {
        try
        {
            assert clazz.isInterface() && CassandraMetricsRegistry.MetricMBean.class.isAssignableFrom(clazz);

            ObjectName objectName = buildObjectNameFromProps(props);
            String cacheKey = objectName.getCanonicalName();

            @SuppressWarnings("unchecked")
            T cached = (T) metricCache.get(cacheKey);
            if (cached != null)
                return cached;

            return clazz.cast(metricCache.computeIfAbsent(cacheKey, k -> JMX.newMBeanProxy(MBeanWrapper.instance.getMBeanServer(), objectName, clazz)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error accessing metric MBean: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isMBeanMetricRegistered(Props props)
    {
        try
        {
            // Use the internal MBean server to look up MBean by ObjectName. This leverages existing
            // JMX infrastructure and avoids the complexity of constructing metric names from Props.
            //
            // Alternatively, we could use CassandraMetricsRegistry to look up metrics by metric name
            // (e.g., "org.apache.cassandra.metrics.Keyspace.ReadLatency.mykeyspace"), but this requires
            // constructing the full metric name from Props, which is problematic.
            //
            // The reconstructing the scope problem: Different MetricNameFactory implementations
            // construct scopes differently:
            // - KeyspaceMetrics: scope = keyspace property
            // - TableMetrics: scope = keyspace + '.' + scope property
            // - DefaultNameFactory: scope = scope property
            // - SAI AbstractMetrics: scope = keyspace.table.index.scope (all combined)
            //
            // To construct metric names from Props, we would need to duplicate scope construction logic
            // from each factory or refactor to share it. Using ObjectName, in turn, avoids this. We query
            // MBeanServer using the ObjectName pattern already constructed by factories during registration.
            //
            // However, it will be beneficial to revisit this down the line for performance optimizations and
            // to avoid JMX entanglement, so we could switch it off for in-process access.

            ObjectName objectName = buildObjectNameFromProps(props);
            MBeanServer mbs = MBeanWrapper.instance.getMBeanServer();
            return mbs.isRegistered(objectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error checking metric MBean registration: " + e.getMessage(), e);
        }
    }

    private static ObjectName buildObjectNameFromProps(Props props) throws MalformedObjectNameException
    {
        return new ObjectName("org.apache.cassandra.metrics", new Hashtable<>(props.toMap()));
    }

    @Override
    public ColumnFamilyStoreMBean findColumnFamily(String type, String keyspace, String columnFamily)
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        if (ks == null)
            throw new IllegalArgumentException("Keyspace not found: " + keyspace);
        return ks.getColumnFamilyStore(columnFamily);
    }

    @Override
    public CompressionDictionaryManagerMBean findCompressionDictionary(String keyspace, String table)
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        if (ks == null)
            throw new IllegalArgumentException(String.format("Table %s.%s does not exist", keyspace, table));
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(table);
        return cfs.compressionDictionaryManager();
    }

    @Override
    public List<ThreadPoolInfo> threadPoolInfos()
    {
        List<ThreadPoolInfo> infos = new ArrayList<>();
        for (ThreadPoolMetrics metrics : Metrics.allThreadPoolMetrics())
            infos.add(new ThreadPoolInfo(metrics.path, metrics.poolName));
        return infos;
    }

    @Override
    public List<Map.Entry<String, ColumnFamilyStoreMBean>> findColumnFamilies(String type)
    {
        try
        {
            assert type.equals("IndexColumnFamilies") || type.equals("ColumnFamilies");

            List<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<>();

            for (Keyspace keyspace : Keyspace.all())
            {
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                {
                    if (type.equals("IndexColumnFamilies") && !cfs.isIndex())
                        continue;
                    if (type.equals("ColumnFamilies") && cfs.isIndex())
                        continue;

                    mbeans.add(new AbstractMap.SimpleImmutableEntry<>(keyspace.getName(), cfs));
                }
            }

            return mbeans;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error accessing column families", e);
        }
    }

    @Override
    public void close()
    {
        metricCache.clear();
        mBeanCache.clear();
    }

    /**
     * Functional interface for providing MBean instances lazily.
     * Used to defer MBean initialization until the MBean is actually accessed.
     *
     * @param <T> the MBean interface type
     */
    @FunctionalInterface
    public interface MBeanProvider<T>
    {
        /**
         * @return the MBean instance, or {@code null} if the MBean is not available
         * @throws RuntimeException if the MBean cannot be provided (e.g., not initialized yet)
         */
        T provide();
    }

    /** Visitor that finds a specific auth cache MBean type from AuthCacheService. */
    private static class AuthCacheFinder implements AuthCache.MBeanVisitor
    {
        private final Class<?> targetType;
        private Object foundCache;

        AuthCacheFinder(Class<?> targetType)
        {
            this.targetType = targetType;
        }

        @Override
        public void visitCredentials(PasswordAuthenticator.CredentialsCacheMBean cache)
        {
            if (targetType.equals(PasswordAuthenticator.CredentialsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitJmxPermissions(AuthorizationProxy.JmxPermissionsCacheMBean cache)
        {
            if (targetType.equals(AuthorizationProxy.JmxPermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitPermissions(PermissionsCacheMBean cache)
        {
            if (targetType.equals(PermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitNetwork(NetworkPermissionsCacheMBean cache)
        {
            if (targetType.equals(NetworkPermissionsCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visitRoles(RolesCacheMBean cache)
        {
            if (targetType.equals(RolesCacheMBean.class))
                foundCache = cache;
        }

        @Override
        public void visit(AuthCacheMBean cache)
        {
            // No-op. Used for caches without specific MBean types.
        }

        Object getCache()
        {
            return foundCache;
        }
    }
}
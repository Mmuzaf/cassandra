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

package org.apache.cassandra.sysview;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;

public class SystemViewRegistry implements Iterable<SystemView<?>>
{
    public static final SystemViewRegistry instance = new SystemViewRegistry();
    private static final Logger logger = LoggerFactory.getLogger(SystemViewRegistry.class);

    private final Map<String, SystemView<?>> systemViews = new HashMap<>();
    private final List<Consumer<SystemView<?>>> viewCreationListeners = new ArrayList<>();

    private SystemViewRegistry()
    {
        super();
    }

    /**
     * Registers {@link SystemViewAdapter} view which exports {@link Collection} content.
     *
     * @param name Name.
     * @param desc Description.
     * @param walker Row walker.
     * @param data Data.
     * @param rowFunc value to row function.
     * @param <R> View row type.
     * @param <D> Collection data type.
     */
    public <R extends ViewRow, D> void registerView(String name, String desc, SystemViewWalker<R> walker, Collection<D> data, Function<D, R> rowFunc)
    {
        registerView0(name, new SystemViewAdapter<>(name, desc, walker, data, rowFunc));
    }

    @Override
    public Iterator<SystemView<?>> iterator()
    {
        return systemViews.values().iterator();
    }

    public Collection<SystemView<?>> getAll()
    {
        return systemViews.values();
    }

    public void addSystemViewCreationListener(Consumer<SystemView<?>> lsnr)
    {
        viewCreationListeners.add(lsnr);
    }

    private void registerView0(String name, SystemView<?> sysView)
    {
        systemViews.put(name, sysView);
        notifyListeners(sysView, viewCreationListeners);
    }

    private static <T> void notifyListeners(T t, Collection<Consumer<T>> listeners)
    {
        if (listeners == null)
            return;

        for (Consumer<T> lsnr : listeners)
        {
            try
            {
                lsnr.accept(t);
            }
            catch (Exception e)
            {
                logger.warn("Listener error", e);
            }
        }
    }
}

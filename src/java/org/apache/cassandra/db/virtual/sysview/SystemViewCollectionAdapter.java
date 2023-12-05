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

package org.apache.cassandra.db.virtual.sysview;

import org.apache.cassandra.db.virtual.proc.RowWalker;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class SystemViewCollectionAdapter<R> implements SystemView<R>
{
    private final String name;
    private final String description;
    private final RowWalker<R> walker;
    private final Iterable<R> data;

    private SystemViewCollectionAdapter(
            String name,
            String description,
            RowWalker<R> walker,
            Iterable<R> data)
    {
        this.name = name;
        this.description = description;
        this.walker = walker;
        this.data = data;
    }

    public static <C, R> SystemViewCollectionAdapter<R> create(
            String name,
            String description,
            RowWalker<R> walker,
            Supplier<Iterable<C>> container,
            Function<C, R> rowFunc)
    {
        return new SystemViewCollectionAdapter<>(name,
                description,
                walker,
                () -> StreamSupport.stream(container.get().spliterator(), false)
                        .map(rowFunc).iterator());
    }

    public static <C, R, D> SystemViewCollectionAdapter<R> create(
            String name,
            String description,
            RowWalker<R> walker,
            Supplier<Iterable<C>> container,
            Function<C, Iterable<D>> extractor,
            BiFunction<C, D, R> rowFunc)
    {
        return new SystemViewCollectionAdapter<>(name,
                description,
                walker,
                () -> StreamSupport.stream(container.get().spliterator(), false)
                        .flatMap(c -> StreamSupport.stream(extractor.apply(c).spliterator(), true)
                                .map(d -> rowFunc.apply(c, d))).iterator());
    }

    @Override
    public RowWalker<R> walker()
    {
        return walker;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public long size()
    {
        return StreamSupport.stream(data.spliterator(), true).count();
    }

    @Override
    public Iterator<R> iterator()
    {
        return data.iterator();
    }
}

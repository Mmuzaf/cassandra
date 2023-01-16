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
import java.util.Iterator;
import java.util.function.Function;

public class SystemViewAdapter<R extends ViewRow, D> implements SystemView<R>
{
    private final String name;
    private final String description;
    private final SystemViewWalker<R> walker;
    private Collection<D> data;
    private final Function<D, R> rowFunc;

    public SystemViewAdapter(String name, String description, SystemViewWalker<R> walker, Collection<D> data, Function<D, R> rowFunc)
    {
        this.name = name;
        this.description = description;
        this.walker = walker;
        this.data = data;
        this.rowFunc = rowFunc;
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
    public SystemViewWalker<R> walker()
    {
        return walker;
    }

    @Override
    public Iterator<R> iterator()
    {
        Iterator<D> dataIter = data.iterator();

        return new Iterator<R>()
        {
            @Override
            public boolean hasNext()
            {
                return dataIter.hasNext();
            }

            @Override
            public R next()
            {
                return rowFunc.apply(dataIter.next());
            }
        };
    }

    @Override
    public int size()
    {
        return data.size();
    }
}

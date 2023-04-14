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

package org.apache.cassandra.distributed.shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.CassandraRelevantProperties;

public final class WithProperties implements AutoCloseable
{
    private final List<Property> properties = new ArrayList<>();

    public WithProperties()
    {
    }

    public WithProperties(String... kvs)
    {
        with(kvs);
    }

    public void with(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i <= kvs.length - 2; i = i + 2)
        {
            with(kvs[i], kvs[i + 1]);
        }
    }

    public void set(CassandraRelevantProperties prop, String value)
    {
        with(prop, () -> prop.setString(value));
    }

    public void set(CassandraRelevantProperties prop, String... values)
    {
        set(prop, Arrays.asList(values));
    }

    public void set(CassandraRelevantProperties prop, Collection<String> values)
    {
        set(prop, Joiner.on(",").join(values));
    }

    public void set(CassandraRelevantProperties prop, boolean value)
    {
        with(prop, () -> prop.setBoolean(value));
    }

    public void set(CassandraRelevantProperties prop, long value)
    {
        with(prop, () -> prop.setLong(value));
    }

    public void with(String key, String value)
    {
        String previous = System.setProperty(key, value);
        properties.add(new Property(key, previous));
    }

    private static String convert(Object value)
    {
        if (value == null)
            return null;
        if (value instanceof String)
            return (String) value;
        if (value instanceof Boolean)
            return Boolean.toString((Boolean) value);
        if (value instanceof Long)
            return Long.toString((Long) value);
        if (value instanceof Integer)
            return Integer.toString((Integer) value);
        throw new IllegalArgumentException("Unknown type " + value.getClass());
    }

    private void with(CassandraRelevantProperties prop, Supplier<Object> prev)
    {
        properties.add(new Property(prop.getKey(), convert(prev.get())));
    }

    @Override
    public void close()
    {
        Collections.reverse(properties);
        properties.forEach(s -> {
            if (s.value == null)
                System.getProperties().remove(s.key); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            else
                System.setProperty(s.key, s.value); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        });
        properties.clear();
    }

    private static final class Property
    {
        private final String key;
        private final String value;

        private Property(String key, String value)
        {
            this.key = key;
            this.value = value;
        }
    }
}

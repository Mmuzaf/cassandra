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

package org.apache.cassandra.config.sysview;

import java.util.Map;
import java.util.function.Function;

import org.yaml.snakeyaml.introspector.Property;

import org.apache.cassandra.sysview.ViewRow;

/**
 * Representation system view of all available configuration properties.
 */
public class ConfigPropertyViewRow implements ViewRow
{
    private final Map.Entry<String, Property> entry;
    private final Function<String, String> value;

    public ConfigPropertyViewRow(Map.Entry<String, Property> entry, Function<String, String> value)
    {
        this.entry = entry;
        this.value = value;
    }

    public String paramName()
    {
        return entry.getKey();
    }
    public Class<?> paramType()
    {
        return entry.getValue().getType();
    }
    public boolean isWritable() {
        return entry.getValue().isWritable();
    }
    public String defaultValue() {
        return "";
    }
    public String value() {
        return value.apply(entry.getKey());
    }
}

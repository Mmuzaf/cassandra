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

package org.apache.cassandra.sysview.walker;

import org.apache.cassandra.config.sysview.ConfigViewRow;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.sysview.SystemViewWalker;

/**
 * This class is auto-generated based on the view row {@link ConfigViewRow}.
 */
public class ConfigViewWalker implements SystemViewWalker<ConfigViewRow>
{
    @Override
    public int count()
    {
        return 5;
    }

    @Override
    public void visitAll(AttributeVisitor v)
    {
        v.accept(ColumnMetadata.Kind.PARTITION_KEY, "param_name", String.class);
        v.accept(ColumnMetadata.Kind.REGULAR, "param_type", Class.class);
        v.accept(ColumnMetadata.Kind.REGULAR, "is_writable", boolean.class);
        v.accept(ColumnMetadata.Kind.REGULAR, "default_value", String.class);
        v.accept(ColumnMetadata.Kind.REGULAR, "value", String.class);
    }

    @Override
    public void visitAll(ConfigViewRow row, AttributeWithValueVisitor v)
    {
        v.accept(ColumnMetadata.Kind.PARTITION_KEY, "param_name", String.class, row.paramName());
        v.accept(ColumnMetadata.Kind.REGULAR, "param_type", Class.class, row.paramType());
        v.acceptBoolean(ColumnMetadata.Kind.REGULAR, "is_writable", row.isWritable());
        v.accept(ColumnMetadata.Kind.REGULAR, "default_value", String.class, row.defaultValue());
        v.accept(ColumnMetadata.Kind.REGULAR, "value", String.class, row.value());
    }
}

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

package org.apache.cassandra.db.virtual.walker;

import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.model.CommandArgumentRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.CommandArgumentRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.CommandArgumentRow
 */
public class CommandArgumentRowWalker implements RowWalker<CommandArgumentRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "command", String.class);
        visitor.accept(Column.Type.CLUSTERING, "argument", String.class);
        visitor.accept(Column.Type.REGULAR, "arity", String.class);
        visitor.accept(Column.Type.REGULAR, "default_value", String.class);
        visitor.accept(Column.Type.REGULAR, "description", String.class);
        visitor.accept(Column.Type.REGULAR, "kind", String.class);
        visitor.accept(Column.Type.REGULAR, "required", Boolean.TYPE);
        visitor.accept(Column.Type.REGULAR, "type", String.class);
    }

    @Override
    public void visitRow(CommandArgumentRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "command", String.class, row::command);
        visitor.accept(Column.Type.CLUSTERING, "argument", String.class, row::argument);
        visitor.accept(Column.Type.REGULAR, "arity", String.class, row::arity);
        visitor.accept(Column.Type.REGULAR, "default_value", String.class, row::defaultValue);
        visitor.accept(Column.Type.REGULAR, "description", String.class, row::description);
        visitor.accept(Column.Type.REGULAR, "kind", String.class, row::kind);
        visitor.accept(Column.Type.REGULAR, "required", Boolean.TYPE, row::required);
        visitor.accept(Column.Type.REGULAR, "type", String.class, row::type);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
            case CLUSTERING:
                return 1;
            case REGULAR:
                return 6;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}

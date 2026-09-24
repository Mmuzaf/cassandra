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

import java.util.UUID;

import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.model.CommandExecutionRow;

/**
 * The {@link org.apache.cassandra.db.virtual.model.CommandExecutionRow} row metadata and data walker.
 *
 * @see org.apache.cassandra.db.virtual.model.CommandExecutionRow
 */
public class CommandExecutionRowWalker implements RowWalker<CommandExecutionRow>
{
    @Override
    public void visitMeta(MetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "execution_id", UUID.class);
        visitor.accept(Column.Type.REGULAR, "command", String.class);
        visitor.accept(Column.Type.REGULAR, "completed_at", Long.class);
        visitor.accept(Column.Type.REGULAR, "error", String.class);
        visitor.accept(Column.Type.REGULAR, "error_type", String.class);
        visitor.accept(Column.Type.REGULAR, "output", String.class);
        visitor.accept(Column.Type.REGULAR, "started_at", Long.TYPE);
        visitor.accept(Column.Type.REGULAR, "status", String.class);
    }

    @Override
    public void visitRow(CommandExecutionRow row, RowMetadataVisitor visitor)
    {
        visitor.accept(Column.Type.PARTITION_KEY, "execution_id", UUID.class, row::executionId);
        visitor.accept(Column.Type.REGULAR, "command", String.class, row::command);
        visitor.accept(Column.Type.REGULAR, "completed_at", Long.class, row::completedAt);
        visitor.accept(Column.Type.REGULAR, "error", String.class, row::error);
        visitor.accept(Column.Type.REGULAR, "error_type", String.class, row::errorType);
        visitor.accept(Column.Type.REGULAR, "output", String.class, row::output);
        visitor.accept(Column.Type.REGULAR, "started_at", Long.TYPE, row::startedAt);
        visitor.accept(Column.Type.REGULAR, "status", String.class, row::status);
    }

    @Override
    public int count(Column.Type type)
    {
        switch (type)
        {
            case PARTITION_KEY:
                return 1;
            case CLUSTERING:
                return 0;
            case REGULAR:
                return 7;
            default:
                throw new IllegalStateException("Unknown column type: " + type);
        }
    }
}

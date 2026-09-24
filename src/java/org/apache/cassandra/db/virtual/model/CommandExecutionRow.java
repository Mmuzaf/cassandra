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

package org.apache.cassandra.db.virtual.model;

import java.util.UUID;

import org.apache.cassandra.management.CommandInvokerService.ExecutionHistory;
import org.apache.cassandra.management.ManagementUtils;
import org.apache.cassandra.management.api.ExecutionErrorType;

/**
 * Management command execution representation for a
 * {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class CommandExecutionRow
{
    private final ExecutionHistory record;

    public CommandExecutionRow(ExecutionHistory record)
    {
        this.record = record;
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public UUID executionId()
    {
        return record.executionId();
    }

    @Column
    public String command()
    {
        return record.commandName();
    }

    @Column
    public String status()
    {
        return record.status().name();
    }

    @Column
    public long startedAt()
    {
        return record.startTime();
    }

    /** Null while the command is still running. */
    @Column
    public Long completedAt()
    {
        return record.endTime();
    }

    @Column
    public String error()
    {
        Throwable error = record.error();
        return error == null ? null : ManagementUtils.causeMessages(error);
    }

    @Column
    public String errorType()
    {
        ExecutionErrorType type = record.errorType();
        return type == null ? null : type.name();
    }

    /** Output produced so far. Null for synchronous executions, whose output went back to the caller. */
    @Column
    public String output()
    {
        return record.output();
    }
}

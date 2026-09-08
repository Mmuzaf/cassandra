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

import org.apache.cassandra.management.CommandInvokerService.CommandEntry;

/**
 * Management command representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class CommandRow
{
    private final CommandEntry entry;

    public CommandRow(CommandEntry entry)
    {
        this.entry = entry;
    }

    /** Full command name, as accepted by {@code INVOKE COMMAND}, e.g. {@code profile.start}. */
    @Column(type = Column.Type.PARTITION_KEY)
    public String command()
    {
        return entry.fullName();
    }

    @Column
    public String description()
    {
        return entry.command().description();
    }
}

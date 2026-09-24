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

package org.apache.cassandra.management.api;

/**
 * Marker for commands that block the calling thread long enough to occupy a management port thread,
 * such as repair, cleanup or decommission.
 * <ul>
 *   <li>{@code INVOKE COMMAND} returns as soon as the command is started. The result row carries the
 *   execution id and a hint pointing at {@code system_views.command_executions}.</li>
 *   <li>Progress is the output produced so far. Status and outcome are in that table.</li>
 *   <li>Every server API returns the execution id at once. Only nodetool over {@code static_mbean} blocks
 *   inside the JMX call, because it bypasses the command service. nodetool over {@code cql} and
 *   {@code command_mbean} blocks by polling on the client.</li>
 * </ul>
 */
public interface ProgressibleCommand
{
}

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
 * Command interface for all management commands.
 * <p>
 * Name of the command that is expected from caller derived from actual command class name.<br>
 * <ul>
 *     <li><b>Name format:</b> All words divided by capital letters except "Command" suffix will form hierarchical command name.</li>
 *     <li><b>Example:</b> {@code MyUsefullCommand} is name of command so {@code nodetool myusefull param1 param2}
 *     expected from user.</li>
 * </ul>
 * <p>
 * Other protocols must expose command similarly. Rest API must expect {@code /api-root/my-usefull?param1=value1&param2=value2} URI.
 *
 * @param <T> Command user object type.
 * @param <R> Command result type.
 */
public interface Command<T, R>
{
    String name();
    String description();
    Class<T> commandUserObject();
}

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

package org.apache.cassandra.tools.nodetool;

import java.util.Collection;

/**
 * Picocli-annotated nodetool commands shipped outside {@link NodetoolCommand}, found with {@link java.util.ServiceLoader}.
 * The server registers them in the command registry and nodetool adds them to its command tree, so the jar
 * must be on both classpaths.
 */
public interface NodetoolCommandsProvider
{
    /**
     * Classes annotated with {@link picocli.CommandLine.Command}. A leaf command must extend {@link AbstractCommand}.
     * A command group lists its leaves in {@code subcommands}.
     */
    Collection<Class<?>> commands();
}

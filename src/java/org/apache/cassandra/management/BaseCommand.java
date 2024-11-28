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

package org.apache.cassandra.management;

import picocli.CommandLine;

/**
 * Base class for all nodetool commands.
 */
public abstract class BaseCommand implements Runnable
{
    /** The command specification, used to access command-specific properties. */
    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec; // injected by picocli
    /** The ServiceBridge instance to interact with the Cassandra node. */
    protected ServiceBridge bridge;

    /**
     * The ServiceBridge instance is injected by the picocli framework during command execution and is used to
     * interact with the Cassandra node. This method is called by picocli and used depending on the execution strategy.
     * @param bridge The ServiceBridge instance to inject.
     */
    public void setBridge(ServiceBridge bridge)
    {
        this.bridge = bridge;
    }

    @Override
    public void run()
    {
        execute(bridge);
    }

    protected abstract void execute(ServiceBridge probe);
}

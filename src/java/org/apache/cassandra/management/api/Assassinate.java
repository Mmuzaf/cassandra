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

import org.apache.cassandra.management.BaseCommand;
import org.apache.cassandra.management.ServiceBridge;
import picocli.CommandLine;

import static org.apache.cassandra.management.CommandUtils.ssProxy;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@CommandLine.Command(name = "assassinate", description = "Forcefully remove a dead node without re-replicating any data. Use as a last resort if you cannot removenode")
public class Assassinate extends BaseCommand
{
    @CommandLine.Parameters(description = "IP address of the endpoint to assassinate", arity = "1")
    public String ip_address = EMPTY;

    @Override
    public void execute(ServiceBridge probe)
    {
        ssProxy(probe).assassinateEndpoint(ip_address);
    }
}

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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getauthcacheconfig", description = "Get configuration of Auth cache")
public class GetAuthCacheConfig extends NodeTool.NodeToolCmd
{
    @SuppressWarnings("unused")
    @Option(title = "cache-name",
            name = {"--cache-name"},
            description = "Name of Auth cache (required)",
            required = true)
    private String cacheName;

    @Override
    public void execute(NodeProbe probe)
    {
        AuthCacheMBean authCacheMBean = probe.getAuthCacheMBean(cacheName);

        probe.output().out.println("Validity Period: " + authCacheMBean.getValidity());
        probe.output().out.println("Update Interval: " + authCacheMBean.getUpdateInterval());
        probe.output().out.println("Max Entries: " + authCacheMBean.getMaxEntries());
        probe.output().out.println("Active Update: " + authCacheMBean.getActiveUpdate());
    }
}

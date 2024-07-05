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
import org.apache.cassandra.management.ServiceMBeanBridge;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "version", description = "Print cassandra version")
public class Version extends BaseCommand
{
    @Option(names = {"-v", "--verbose"}, description = "Include addtitional information")
    private boolean verbose = false;

    @Override
    protected void execute(ServiceMBeanBridge probe)
    {
        logger.out.println("ReleaseVersion: " + probe.ssProxy().getReleaseVersion());
        if (verbose) {
            logger.out.println("GitSHA: " + probe.ssProxy().getGitSHA());
        }
        
    }
}

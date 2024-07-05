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

import org.junit.Test;

public class VersionTest extends CQLToolRunnerTester
{
    private static final String SUBCOMMAND = "version";
    
    @Test
    public void testHelp()
    {
        String help = """
                      NAME
                              nodetool version - Print cassandra version
                      
                      SYNOPSIS
                              nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                                      [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                                      [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                                      [(-u <username> | --username <username>)] version [(-v | --verbose)]
                      
                      OPTIONS
                              -h <host>, --host <host>
                                  Node hostname or ip address
                      
                              -p <port>, --port <port>
                                  Remote jmx agent port number
                      
                              -pp, --print-port
                                  Operate in 4.0 mode with hosts disambiguated by port number
                      
                              -pw <password>, --password <password>
                                  Remote jmx agent password
                      
                              -pwf <passwordFilePath>, --password-file <passwordFilePath>
                                  Path to the JMX password file
                      
                              -u <username>, --username <username>
                                  Remote jmx agent username
                      
                              -v, --verbose
                                  Include addtitional information
                      """;
        
        invokeNodetool("help", SUBCOMMAND)
                .asserts()
                .success()
                .outputContains(help);
    }
    
    @Test
    public void testOutput()
    {
        invokeNodetool(SUBCOMMAND)
                .asserts()
                .success()
                .outputMatches("ReleaseVersion: \\d+.*");
    }
    
    @Test
    public void testVerboseOutput1()
    {
        invokeNodetool(SUBCOMMAND, "-v")
                .asserts()
                .success()
                .outputLinesMatchesAll(
                        "ReleaseVersion: \\d+.*",
                        "GitSHA: [0-9a-f]{40}.*"
                );
    }
    
    @Test
    public void testVerboseOutput2()
    {
        invokeNodetool(SUBCOMMAND, "--verbose")
                .asserts()
                .success()
                .outputLinesMatchesAll(
                        "ReleaseVersion: \\d+.*",
                        "GitSHA: [0-9a-f]{40}.*"
                );
    }
}

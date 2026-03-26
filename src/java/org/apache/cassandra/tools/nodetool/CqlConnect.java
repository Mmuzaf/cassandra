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

import java.io.IOException;

import com.google.common.base.Throwables;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.InitializationException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command options for NodeTool commands that are executed via CQL.
 */
@Command(name = "cqlconnect", description = "Connect to a Cassandra node via CQL")
public class CqlConnect extends AbstractCommand implements AutoCloseable
{
    private static final int DEFAULT_CQL_PORT = 11211;

    /** The command specification, used to access command-specific properties. */
    @Spec
    protected CommandSpec spec; // injected by picocli

    @Option(names = { "-h", "--host" }, description = "Node hostname or ip address", arity = "0..1")
    private String host = "127.0.0.1";

    @Option(names = { "-p", "--port" }, description = "Remote CQL native transport port number", arity = "0..1")
    private int port = DEFAULT_CQL_PORT;

    @Option(names =  { "--diagnostic" }, description = "Enable diagnostic output for troubleshooting connection issues")
    private boolean diagnostic = false;

    private volatile SimpleClient client;

    /**
     * Initialize the CQL connection to the Cassandra node using the provided options.
     */
    public void run()
    {
        if (client != null)
            return;

        try
        {
            if (diagnostic)
                output.printInfo("Connecting to %s:%s via CQL...%n", host, port);

            SimpleClient.Builder builder = SimpleClient.builder(host, port).protocolVersion(ProtocolVersion.V5);
            client = builder.build();
            client.connect(false);
        }
        catch (IOException e)
        {
            Throwable rootCause = Throwables.getRootCause(e);
            output.printError("nodetool: Failed to connect to '%s:%s' via CQL - %s: '%s'.%n", host, port,
                         rootCause.getClass().getSimpleName(), rootCause.getMessage());
            throw new InitializationException("Failed to connect to CQL", e);
        }
    }

    public SimpleClient client()
    {
        return client;
    }

    @Override
    protected void execute(NodeProbe probe)
    {
        assert probe == null;
        run();
    }

    @Override
    public void close() throws Exception
    {
        if (client != null)
        {
            try
            {
                client.close();
            }
            finally
            {
                client = null;
            }
        }
    }
}

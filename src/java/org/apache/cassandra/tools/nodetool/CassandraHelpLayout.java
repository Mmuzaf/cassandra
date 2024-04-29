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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import picocli.CommandLine;

/**
 * Help facotry for the Cassandra nodetool to generate the help output. This class is used to match
 * the command output with the previously available nodetool help output format.
 */
public class CassandraHelpLayout extends CommandLine.Help
{
    private static final String HEADER_HEADING = "NAME%n";
    private static final String SYNOPSIS_HEADING = "SYNOPSIS%n";

    public CassandraHelpLayout(CommandLine.Model.CommandSpec spec, ColorScheme scheme)
    {
        super(spec, scheme);
    }

    @Override
    public IOptionRenderer createDefaultOptionRenderer()
    {
        return new IOptionRenderer()
        {
            public Ansi.Text[][] render(CommandLine.Model.OptionSpec option, IParamLabelRenderer ignored, ColorScheme scheme)
            {
                // assume one line of description text (may contain embedded %n line separators)
                String[] description = option.description();
                Ansi.Text[] descriptionFirstLines = scheme.text(description[0]).splitLines();

                Ansi.Text EMPTY = Ansi.AUTO.new Text(100);
                List<Ansi.Text[]> result = new ArrayList<Ansi.Text[]>();
                result.add(new Ansi.Text[]{
                    scheme.optionText(String.valueOf(
                        option.command().usageMessage().requiredOptionMarker())),
                    scheme.optionText(option.shortestName()),
                    scheme.text(","), // we assume every option has a short and a long name
                    scheme.optionText(option.longestName()), // just the option name without parameter
                    descriptionFirstLines[0] });
                for (int i = 1; i < descriptionFirstLines.length; i++)
                {
                    result.add(new Ansi.Text[]{ EMPTY, EMPTY, EMPTY, EMPTY, descriptionFirstLines[i] });
                }
                return result.toArray(new Ansi.Text[result.size()][]);
            }
        };
    }

    @Override
    public String headerHeading(Object... params)
    {
        return createHeading(HEADER_HEADING, params);
    }

    @Override
    public String synopsisHeading(Object... params)
    {
        return createHeading(SYNOPSIS_HEADING, params);
    }

    @Override
    public String detailedSynopsis(int synopsisHeadingLength, Comparator<CommandLine.Model.OptionSpec> optionSort, boolean clusterBooleanOptions)
    {
        Set<CommandLine.Model.ArgSpec> argsInGroups = new HashSet<>();
        Ansi.Text groupsText = createDetailedSynopsisGroupsText(argsInGroups);
        Ansi.Text optionText = createDetailedSynopsisOptionsText(argsInGroups, optionSort, clusterBooleanOptions);
        Ansi.Text endOfOptionsText = createDetailedSynopsisEndOfOptionsText();
        Ansi.Text positionalParamText = createDetailedSynopsisPositionalsText(argsInGroups);
        Ansi.Text commandText = createDetailedSynopsisCommandText();

        return makeSynopsisFromParts(synopsisHeadingLength, optionText, groupsText, endOfOptionsText, positionalParamText, commandText);
    }

    @Override
    protected Ansi.Text createDetailedSynopsisOptionsText(Collection<CommandLine.Model.ArgSpec> done,
                                                          List<CommandLine.Model.OptionSpec> optionList,
                                                          Comparator<CommandLine.Model.OptionSpec> optionSort,
                                                          boolean clusterBooleanOptions)
    {
        Ansi.Text optionText = ansi().new Text(0);
        List<CommandLine.Model.OptionSpec> options = new ArrayList<>(optionList); // iterate in declaration order
        if (optionSort != null)
            options.sort(optionSort);

        options.removeAll(done);

        if (clusterBooleanOptions)
        { // cluster all short boolean options into a single string
            List<CommandLine.Model.OptionSpec> booleanOptions = new ArrayList<CommandLine.Model.OptionSpec>();
            StringBuilder clusteredRequired = new StringBuilder("-");
            StringBuilder clusteredOptional = new StringBuilder("-");
            for (CommandLine.Model.OptionSpec option : options)
            {
                if (option.hidden())
                    continue;

                boolean isFlagOption = option.typeInfo().isBoolean();
                if (isFlagOption && option.arity().max <= 0)
                { // #612 consider arity: boolean options may require a parameter
                    String shortestName = option.shortestName();
                    if (shortestName.length() == 2 && shortestName.startsWith("-"))
                    { // POSIX short option
                        // we don't want to show negatable options as clustered options in the synopsis
                        if (!option.negatable() || shortestName.equals(commandSpec().negatableOptionTransformer().makeSynopsis(shortestName, commandSpec())))
                        {
                            booleanOptions.add(option);
                            if (option.required())
                            {
                                clusteredRequired.append(shortestName.substring(1));
                            }
                            else
                            {
                                clusteredOptional.append(shortestName.substring(1));
                            }
                        }
                    }
                }
            }
            options.removeAll(booleanOptions);

            // initial length was 1
            if (clusteredRequired.length() > 1)
                optionText = optionText.concat(" ").concat(colorScheme().optionText(clusteredRequired.toString()));

            // initial length was 1
            if (clusteredOptional.length() > 1)
                optionText = optionText.concat(" [").concat(colorScheme().optionText(clusteredOptional.toString())).concat("]");
        }

//        for (CommandLine.Model.OptionSpec option : options)
//            optionText = concatOptionText(" ", optionText, colorScheme(), option, parameterLabelRenderer());

        return optionText;
    }

    //    headerHeading = "NAME%n",
//    synopsisHeading = "SYNOPSIS%n",
//    optionListHeading = "OPTIONS%n",
//    private static void usage()
//    {
//        CommandLine.Help.ColorScheme colorScheme = CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO);
//        CommandLine.Help help = getHelpFactory().create(getCommandSpec(), colorScheme)
//        StringBuilder result = new StringBuilder();
//        for (String key : getHelpSectionKeys())
//        {
//            IHelpSectionRenderer renderer = getHelpSectionMap().get(key);
//            if (renderer != null)
//            {
//                result.append(renderer.render(help));
//            }
//        }
//    }


//
//    usage: nodetool [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
//        [(-pp | --print-port)] [(-h <host> | --host <host>)]
//        [(-pw <password> | --password <password>)]
//        [(-u <username> | --username <username>)] [(-p <port> | --port <port>)]
//        <command> [<args>]
//
//    The most commonly used nodetool commands are:
//    abortbootstrap                      Abort a failed bootstrap
//    assassinate                         Forcefully remove a dead node without re-replicating any data.  Use as a last resort if you cannot removenode
//
//    --------------------------------------------------------------------------------
//
//    mmuzaf@Maxims-MBP bin % ./nodetool help cleanup
//    NAME
//            nodetool cleanup - Triggers the immediate cleanup of keys no longer
//            belonging to a node. By default, clean all keyspaces
//
//    SYNOPSIS
//            nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
//                    [(-pp | --print-port)] [(-pw <password> | --password <password>)]
//                    [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
//                    [(-u <username> | --username <username>)] cleanup
//                    [(-j <jobs> | --jobs <jobs>)] [--] [<keyspace> <tables>...]
//
//    OPTIONS
//            -h <host>, --host <host>
//                Node hostname or ip address
//
//            -j <jobs>, --jobs <jobs>
//                Number of sstables to cleanup simultanously, set to 0 to use all
//                available compaction threads
//
//            -p <port>, --port <port>
//                Remote jmx agent port number
//
//            -pp, --print-port
//                Operate in 4.0 mode with hosts disambiguated by port number
//
//            -pw <password>, --password <password>
//                Remote jmx agent password
//
//            -pwf <passwordFilePath>, --password-file <passwordFilePath>
//                Path to the JMX password file
//
//            -u <username>, --username <username>
//                Remote jmx agent username
//
//            --
//                This option can be used to separate command-line options from the
//                list of argument, (useful when arguments might be mistaken for
//                command-line options
//
//            [<keyspace> <tables>...]
//                The keyspace followed by one or many tables
}

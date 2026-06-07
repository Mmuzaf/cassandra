# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
A script to generate the nodetool AsciiDoc documentation.

The raw ``nodetool help`` text files are produced beforehand by the ``gen-nodetool-text`` Ant target (which runs
``NodetoolHelpDump.java`` against the built Cassandra jar in a single JVM) into
``modules/cassandra/examples/TEXT/NODETOOL``. This script only consumes those text files and turns them into the
AsciiDoc pages; it does not start a JVM itself.
"""
from __future__ import print_function

import glob
import os
import re
import sys


outdir = "modules/cassandra/pages/managing/tools/nodetool"
examplesdir = "modules/cassandra/examples/TEXT/NODETOOL"
helpfilename = outdir + "/nodetool.txt"
# Raw top-level "nodetool help" listing produced by the Ant gen-nodetool-text step.
rootfilename = examplesdir + "/nodetool.txt"
command_re = re.compile("(    )([_a-z]+)")
commandADOCContent = "= {0}\n\n== Usage\n[source,plaintext]\n----\ninclude::cassandra:example$TEXT/NODETOOL/{0}.txt[]\n----\n"

if(os.environ.get("SKIP_NODETOOL") == "1"):
    sys.exit(0)

# The nodetool help text must be generated first by the Ant 'gen-nodetool-text' target.
txt_files = glob.glob(examplesdir + "/*.txt")
if not txt_files:
    sys.exit("ERROR: no nodetool help text files found in '%s'. "
             "Run 'ant gen-nodetool-text' (or 'ant gen-asciidoc') to generate them first." % examplesdir)
if not os.path.exists(rootfilename):
    sys.exit("ERROR: '%s' is missing. "
             "Run 'ant gen-nodetool-text' (or 'ant gen-asciidoc') to generate the nodetool help text first."
             % rootfilename)

# create the documentation directory
if not os.path.exists(outdir):
    os.makedirs(outdir)


def read_text(path):
    with open(path, "r") as f:
        return f.read()


# create the base help file used to discover the commands and to render the main usage page
def create_help_file(root_help):
    with open(helpfilename, "w+") as output_file:
        # Wrap the initial "usage: ..." block
        usage_block = re.search(r'usage:.*?\n\n', root_help, re.DOTALL | re.MULTILINE)
        if not usage_block:
            raise ValueError("No usage block matched in nodetool help output")
        output_file.write("[source,console]\n----\n")
        output_file.write(usage_block.group(0))
        output_file.write("----\n")
        output_file.write(root_help.replace(usage_block.group(0), ''))


# for a given command, create the ADOC file that includes its (already generated) help text
def create_adoc(cmdName):
    cmdFilename = examplesdir + "/" + cmdName + ".txt"
    if not os.path.exists(cmdFilename):
        print("WARNING: no help text file for command '%s' (%s), skipping" % (cmdName, cmdFilename))
        return
    adocFilename = outdir + "/" + cmdName + ".adoc"
    with open(adocFilename, "w") as adocFile:
        adocFile.write(commandADOCContent.format(cmdName, cmdName, cmdName))


# create base file from the raw top-level listing produced by the Ant step
create_help_file(read_text(rootfilename))

# create the main usage page
with open(outdir + "/nodetool.adoc", "w+") as output:
    with open(helpfilename, "r+") as helpfile:
        output.write("= Nodetool\n\n== Usage\n\n")
        for commandLine in helpfile:
            command = command_re.sub(r'\nxref:cassandra:managing/tools/nodetool/\2.adoc[\2] - ', commandLine)
            output.write(command)

# create the command usage pages
with open(helpfilename, "r+") as helpfile:
    for commandLine in helpfile:
        command = command_re.match(commandLine)
        if not command:
            continue
        create_adoc(command.group(0).strip())

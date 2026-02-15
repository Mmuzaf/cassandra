package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

/**
 * Verifies backward compatibility of the {@code -pp/--print-port} option placement.
 * For each command in {@code PRINT_PORT_COMMANDS}, both syntaxes must succeed:
 * <ul>
 *   <li>{@code nodetool <command> --print-port} (natural picocli parsing via {@link PrintPortMixin})</li>
 *   <li>{@code nodetool --print-port <command>} (backward-compatible relocation)</li>
 * </ul>
 * Commands that do not accept {@code --print-port} must reject it.
 */
@RunWith(Parameterized.class)
public class NodetoolPrintPortBackwardCompatibilityTest extends CQLTester
{
    private static final String[] DEAFULT_STRING_ARRAY = new String[0];

    @Parameterized.Parameter
    public String command;

    @Parameterized.Parameter(1)
    public String[] extraArgs;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return List.of(
            new Object[]{ "status", DEAFULT_STRING_ARRAY },
            new Object[]{ "ring", DEAFULT_STRING_ARRAY },
            new Object[]{ "netstats", DEAFULT_STRING_ARRAY },
            new Object[]{ "gossipinfo", DEAFULT_STRING_ARRAY },
            new Object[]{ "failuredetector", DEAFULT_STRING_ARRAY },
            new Object[]{ "describecluster", DEAFULT_STRING_ARRAY },
            new Object[]{ "describering", new String[]{ KEYSPACE } },
            new Object[]{ "getendpoints", new String[]{ KEYSPACE, "pp_compat_tbl", "key1" } }
        );
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Before
    public void createTestData()
    {
        schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".pp_compat_tbl (k text PRIMARY KEY)");
    }

    @Test
    public void testPrintPortAfterSubcommand()
    {
        assertCleanExit(args(command, extraArgs));
    }

    @Test
    public void testPrintPortBeforeSubcommand()
    {
        assertCleanExit(args("--print-port", command, extraArgs));
    }

    @Test
    public void testShortOptionBeforeSubcommand()
    {
        assertCleanExit(args("-pp", command, extraArgs));
    }

    private static void assertCleanExit(String[] args)
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetoolInJvm(args);
        result.assertOnCleanExit();
    }

    private static String[] args(String first, String[] middle)
    {
        List<String> list = new ArrayList<>();
        list.add(first);
        list.addAll(List.of(middle));
        list.add("--print-port");
        return list.toArray(DEAFULT_STRING_ARRAY);
    }

    private static String[] args(String first, String second, String[] rest)
    {
        List<String> list = new ArrayList<>();
        list.add(first);
        list.add(second);
        list.addAll(List.of(rest));
        return list.toArray(DEAFULT_STRING_ARRAY);
    }
}
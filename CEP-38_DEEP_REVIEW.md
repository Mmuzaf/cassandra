# Deep Review: CEP-38 CQL Management API (CASSANDRA-19476)

**Branch reviewed:** `cassandra-19476-bug-hunting`
**Base:** `trunk`
**Reference:** [CEP-38: CQL Management API](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-38%3A+CQL+Management+API)
**Method:** Deep file-focused review against the 444-pattern catalog, with parallel verification subagents and runnable reproducers for the top candidates.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Findings — ranked by severity](#findings--ranked-by-severity)
   - [P0 / Finding 1 — Server-side stack traces leaked to clients](#finding-1--server-side-stack-traces-leaked-to-clients-in-error-messages)
   - [P0 / Finding 2 — `INVOKE COMMAND` round-trip broken for many value types](#finding-2--invoke-command-round-trip-is-broken-for-many-real-value-types-in-the-cql-strategy)
   - [P0 / Finding 3 — `Dispatcher.shutdown()` permanently disables shared executors](#finding-3--dispatchershutdown-permanently-disables-the-shared-transport-executors)
   - [P1 / Finding 4 — Element-type loss in `TypeConverterRegistry`](#finding-4--element-type-loss-in-typeconverterregistryconverttoarrayorcollection)
   - [P1 / Finding 5 — MBean leak on duplicate command name](#finding-5--mbean-leak-on-duplicate-command-name)
   - [P1 / Finding 6 — Protocol-selector precedence is wrong](#finding-6--protocol-selector-precedence-is-wrong-when-env-is-explicitly-set-to-the-default)
   - [P2 / Finding 7 — JMX exposed before `started=true`](#finding-7--commandinvokerservicestart-exposes-jmx-before-startedtrue)
   - [P2 / Finding 8 — `BoundedExecutionHistory` size drift + dead state](#finding-8--boundedexecutionhistory-size-drift--dead-state)
   - [P2 / Finding 9 — Management transport unsafe under concurrent start/destroy](#finding-9--nativetransportmanagementservice-is-not-safe-under-concurrent-startdestroy)
   - [P2 / Finding 10 — No audit for wrong-port `INVOKE COMMAND`](#finding-10--no-client-keyspace-gating-or-auditing-for-executecommandstatement-on-the-regular-port)
   - [P2 / Finding 11 — `getJsonSchema` mis-emits aliases and overwrites array `type`](#finding-11--getjsonschema-mis-emits-aliases-for-the-primary-name-and-overwrites-array-type)
   - [P2 / Finding 12 — Audit log uses raw command name](#finding-12--auditing-exposes-commandname-directly-without-sanitization)
3. [Cross-cutting observations](#cross-cutting-observations)
4. [Wiki vs. implementation discrepancies](#wiki-vs-implementation-discrepancies)
5. [Recommended priority](#recommended-priority)
6. [Reproducers](#reproducers)
   - [Repro 1 — `TypeConverterRegistryRepro.java`](#repro-1--typeconverterregistryreproja)
   - [Repro 2 — `ProtocolPrecedenceRepro.java`](#repro-2--protocolprecedencereprojava)
   - [Repro 3 — `CqlBuilderRoundTripRepro.java`](#repro-3--cqlbuilderroundtripreprojava)

---

## Executive Summary

The patch introduces a CQL-based management API (`INVOKE COMMAND <name> WITH k=v AND …`) on a dedicated port (`11211` by default), routes those requests to a separate `managementExecutor`, and adapts existing picocli-based `nodetool` commands so they can be invoked via three protocols: CQL, JMX (per-command MBeans), or legacy static MBeans.

The new code is well-organized, but in current form it has:

- **3 P0 issues** (release-blocking): client-visible stack traces, broken CQL round-trip for common value types (`List<Integer>`, `Map`, `Set`, `Double`…), and a daemon-restart-breaking executor shutdown.
- **3 P1 issues** (latent / high-confidence): collection element-type loss, MBean leak on name conflict, protocol-selector precedence ambiguity.
- **6 P2 issues** (minor): startup ordering invariants, dead state with concurrency drift, lifecycle synchronization, audit/sanitization gaps, JSON schema bugs.

Two of the top candidates have runnable reproducers; the third has a JUnit source listing usable as-is.

---

## Findings — ranked by severity

### Finding 1 — Server-side stack traces leaked to clients in error messages

**Confidence:** High · **Domain:** Resources / Information disclosure · **Priority:** P0

**Locations:**
- `src/java/org/apache/cassandra/cql3/statements/ExecuteCommandStatement.java:135` and `:140`
- `src/java/org/apache/cassandra/management/CommandMBeanAdapter.java:185, :190`
- `src/java/org/apache/cassandra/management/CommandInvokerService.java:211, :218, :226`

**What's wrong.** `ExecuteCommandStatement.execute()` rethrows internal failures as
`InvalidRequestException(Throwables.getStackTraceAsString(e), e.getCause())` and
`CommandRequestExecutionException(executionId, Throwables.getStackTraceAsString(e), e.getCause())`. `ErrorMessage.encode()` writes the message verbatim to the wire (V5 has no separate "details" field). The full server stack trace, including all internal package/class names and line numbers, is sent to any CQL client. The same pattern is used inside `CommandMBeanAdapter.invoke()` for the JMX path (`throw new IllegalArgumentException(Throwables.getStackTraceAsString(e))`).

**Convention check.** Searching the codebase, no other `CQLStatement.execute()` builds a user-facing error from `getStackTraceAsString` — they all pass `e.getMessage()` only. This pattern is unique to the new patch.

**Impact.**
- Information disclosure to any client reaching the management or regular CQL port.
- Drivers tend to copy the message into client-side logs verbatim — log volume blows up on errors.
- Cassandra has historically classified "stack trace in client error" as a CVE-class issue elsewhere.

**Fix.** Use `e.getMessage()` (or a one-line description) for the wire message; keep the full stack on the server-side log only. Server-side log already happens in `CommandInvokerService.invokeCommand` (`logger.error("Command '{}' (execution ID: {}) execution failed", ...)`).

---

### Finding 2 — `INVOKE COMMAND` round-trip is broken for many real value types in the CQL strategy

**Confidence:** High · **Domain:** Logic / Cross-cutting (writer/parser asymmetry) · **Priority:** P0

**Locations:**
- `src/java/org/apache/cassandra/tools/nodetool/strategy/CqlCommandExecutionStrategy.java:222` (`appendCqlValue`)
- `src/antlr/Parser.g:1396` (`commandPropertyValue`) and `:1404` (`commandListValue`)

**What's wrong.** The grammar accepts only `STRING_LITERAL | INTEGER | BOOLEAN | commandListValue` at the top level, and inside lists only `STRING_LITERAL`. The writer does not match:

| # | Value type | Writer output | Parser verdict |
|---|---|---|---|
| 1 | `List<Integer>` / `List<Long>` | `[7000, 7001]` | rejected (only STRING_LITERAL inside lists) |
| 2 | `List<Boolean>` | `[true, false]` | rejected |
| 3 | Mixed list (`["a", 1, true]`) | `['a', 1, true]` | rejected on first non-string |
| 4 | `byte[]` / `int[]` / `boolean[]` | `[1, 2, 3]` | rejected |
| 5 | `Set<…>` | `{ ... }` | grammar has no `{}` rule for `commandPropertyValue` — unreachable |
| 6 | `Map<…,…>` | `{'k': v}` | unreachable |
| 7 | `null` element inside list | `NULL` | rejected |
| 8 | `Double` / `Float` / `BigDecimal` | `1.0` | INTEGER lexer rule does not accept floats |

**Impact.** Functional break. `nodetool` invocations that use these types will fail with a confusing `SyntaxException` from the server when the user picks the CQL strategy. `List<Long>` is common in nodetool today (e.g. `disablebinary`, `compact`).

**CQL injection assessment.** All string paths pass through `appendWithSingleQuotes` (escapes `'` → `''`) and keys/command names through `ColumnIdentifier.maybeQuote` (escapes `"` → `""`). String escaping is correct against the lexer rules; user input cannot break out of the literal. The injection risk is bounded.

**Fix.**
- In the writer's `List`/`Set`/array branches, force `appendWithSingleQuotes(String.valueOf(item))` and reject nested collections; **or**
- Extend the grammar so `commandListValue` accepts `commandPropertyValue` and add productions for `FLOAT`, map, and set; **or**
- Reject unsupported types eagerly with `IllegalArgumentException` at `appendCqlValue` entry (right now they reach the parser and produce opaque errors).

Also: explicitly skip `null` list elements rather than recursing into "NULL".

---

### Finding 3 — `Dispatcher.shutdown()` permanently disables the SHARED transport executors

**Confidence:** High · **Domain:** Concurrency / Lifecycle · **Priority:** P0

**Locations:**
- `src/java/org/apache/cassandra/transport/Dispatcher.java:654-659` (now shuts down `requestExecutor`, `authExecutor`, **and** `managementExecutor`)
- `src/java/org/apache/cassandra/service/CassandraDaemon.java:841` (called from `destroyClientTransports()`)
- `src/java/org/apache/cassandra/service/NativeTransportService.java` (the previous `Dispatcher.shutdown()` site moved away)

**What's wrong.** The three executors are `static final` fields produced by `SHARED.newExecutor(...)` (an `SEPExecutor` registered in `SharedExecutorPool`). Once shut down, they cannot be replaced from the same field. Any in-JVM dtest or jsvc-managed restart that calls `destroyClientTransports()` and then re-initializes will route to terminated executors that reject tasks. The pre-patch code had this fragility for `requestExecutor` only; the patch widens it to two more executors and to a method now invoked from a more central location.

**Trigger.** In-JVM dtests that shut and restart the daemon; jsvc managed restart.

**Fix.** Either (a) remove `Dispatcher.shutdown()` from `CassandraDaemon.destroyClientTransports()` (keep it only on JVM exit); or (b) make the executors lazily re-creatable (volatile fields rebuilt under a lock).

---

### Finding 4 — Element-type loss in `TypeConverterRegistry.convertToArrayOrCollection`

**Confidence:** High · **Domain:** Logic / Type symmetry · **Priority:** P1

**Location:** `src/java/org/apache/cassandra/management/picocli/TypeConverterRegistry.java:138-149`

**What's wrong.** The array branch (lines 125-136) recursively converts each element via `convertValueBasic(item, componentType)`. The Collection branch does `targetCollection.addAll(sourceCollection)` with no element-level conversion. Confirmed by reproducer (see [Repro 1](#repro-1--typeconverterregistryreproja)):

```
[OK ] int[] target produced ints: [1, 2, 3]
[BUG] List target element[0] runtime class = java.lang.String, value = 1
[BUG] Latent ClassCastException on consumer
```

**Impact today.** Latent — no current nodetool command exposes `List<Integer>` / `Set<Long>` etc. through a picocli-bound field; all `List<String>` consumers are unaffected. The moment any future command adds `@Option List<Integer> dcs`, CQL `INVOKE COMMAND … WITH dcs = ['1','2']` will give the command a `List<String>` masquerading as `List<Integer>` and CCE inside the command body.

**Fix.** Thread the element type through (e.g. via `auxiliaryTypes()[0]`) and recurse per element.

---

### Finding 5 — MBean leak on duplicate command name

**Confidence:** High · **Domain:** Resources / Crash safety · **Priority:** P1

**Location:** `src/java/org/apache/cassandra/management/CommandInvokerService.java:311-322`

**What's wrong.** `registerMBean(commandMBean, objectName, OnException.LOG)` runs first, then `commandMBeanNames.putIfAbsent(...)` is checked. On `prev != null` we throw, but the MBean is already registered AND the map still maps `fullCommandName → previous ObjectName`. `unregisterCommandMBeans()` then unregisters only the previous one — the new one leaks until JVM exit. The surrounding `catch (Exception ex)` further swallows the `IllegalStateException` to a `WARN`, so the operator may not even notice.

**Trigger.** A registry containing two leaves with the same `fullCommandName`. Not currently reachable from `CassandraCommandRegistry`, but the code defends against it incorrectly. This becomes reachable as soon as a third-party `CommandsProvider` (loaded via ServiceLoader, see `ManagementUtils.loadService`) registers a name conflict.

**Fix.** Reverse the order: `putIfAbsent` first, register the MBean only if the put was successful; or unregister-on-failure.

---

### Finding 6 — Protocol-selector precedence is wrong when env is explicitly set to the default

**Confidence:** High · **Domain:** Logic · **Priority:** P1

**Location:** `src/java/org/apache/cassandra/tools/nodetool/strategy/ProtocolAwareExecutionStrategy.java:67-73`

```java
Type defaultStrategy = ...;
Type strategyEnv = CASSANDRA_CLI_EXECUTION_PROTOCOL.getEnum(true, Type.class, defaultStrategy.name());
Type strategySys = CASSANDRA_CLI_EXECUTION_PROTOCOL.getEnum(true, Type.class);
return strategyEnv != defaultStrategy ? strategyEnv : strategySys;
```

There is no way to distinguish "env unset" from "env=STATIC_MBEAN explicitly set". An operator who exports `CASSANDRA_CLI_EXECUTION_PROTOCOL=static_mbean` to enforce the legacy strategy is silently overridden by `-Dcassandra.cli.execution.protocol=cql`. Verified by reproducer (see [Repro 2](#repro-2--protocolprecedencereprojava)) — `envExplicitlyDefault_sysNonDefault_envIgnored` would fail.

**Impact.** Operator confusion / surprising precedence; no functional break, but it violates the typical "explicit env wins over property" convention.

**Fix.** Check `System.getenv(ENV_KEY) != null` directly to gate env override; or pick a single source-of-truth and document it.

Also note: `bin/nodetool` lines ~48-49 do their own `tr [:upper:]` lowercase normalization on the env var — multiple places that translate this independently is a footgun.

---

### Finding 7 — `CommandInvokerService.start()` exposes JMX before `started=true`

**Confidence:** High · **Impact:** Low today · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/management/CommandInvokerService.java:90-102`

`registerCommandMBeansRecursively(...)` registers per-command MBeans **before** `MBeanWrapper.instance.registerMBean(this, MBEAN_NAME)` and `started = true`. `invokeCommand` correctly checks `started`, but `getCommandNames`, `getCommandCount`, and `getCommandMBeanName` do not. Today they are read-only and the registry is constructed in the constructor, so this happens to be safe; the invariant is fragile and undocumented.

**Fix.** Either (a) gate read-only methods on `started`, or (b) reorder so `started=true` is set before any MBean registration.

---

### Finding 8 — `BoundedExecutionHistory` size drift + dead state

**Confidence:** High · **Impact:** Low (dead state) · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/management/CommandInvokerService.java:452-474`

`add()` does `dq.offer(info); if (size.incrementAndGet() > maxSize) { ExecutionHistory removed = dq.pollFirst(); if (removed != null) size.decrementAndGet(); }`. When `pollFirst()` returns null (concurrent eviction interleaving), `size` stays inflated forever. More importantly, `executionHistory` is never read by any code path — pure write-only state.

**Fix.** Either remove the field (preferred — it's dead) or expose it via JMX (`@Operation getRecentExecutions()`), and fix the size-drift race.

---

### Finding 9 — `NativeTransportManagementService` is not safe under concurrent start/destroy

**Confidence:** Medium · **Impact:** Theoretical today · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/service/NativeTransportManagementService.java:92-122`

Only `initialize()` is synchronized. Concurrent callers can: (1) NPE in `start()` after `destroy()` nulled the `server` field; (2) lose the freshly bound channel if `start()` runs between `stop()` and `server = null`. Today only `CassandraDaemon` drives it single-threaded, but the class doesn't document the no-concurrent-calls invariant.

**Fix.** Synchronize `start()`, `stop()`, `destroy()`, or document and assert single-threaded usage.

---

### Finding 10 — No client-keyspace gating or auditing for `ExecuteCommandStatement` on the regular port

**Confidence:** Medium · **Impact:** UX / hardening · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/cql3/statements/ExecuteCommandStatement.java:113`

`execute()` checks `if (!clientState.isInternal && !clientState.isManagement())`, which forbids running `INVOKE COMMAND` on the regular native port. Good. But on the regular port, the parser still accepts the statement, then the runtime throws `InvalidRequestException` with `Throwables.getStackTraceAsString(e)` if the command is unknown (because `validate()` runs first and throws), and otherwise rejects with the message above. Reading the code paths, there is no audit-log entry for the "wrong port" rejection; AuditLogContext is built only in `getAuditLogContext()` (entry type `EXECUTE_COMMAND`).

**Fix.** Audit the rejection too; treat "regular-port INVOKE COMMAND" as a noteworthy event (operator ran a privileged statement on the wrong port).

---

### Finding 11 — `getJsonSchema` mis-emits aliases for the primary name and overwrites array `type`

**Confidence:** Medium · **Impact:** API discovery output is wrong · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/management/CommandMBeanAdapter.java:256-289`

`buildJsonSchemaProperty` first sets `prop.put("type", getJsonType(arg.type()))`. Later, for arrays/Lists, it unconditionally `prop.put("type", "array")` — overwriting any earlier non-array type (so a `String[]` is fine, but a `List<Foo>` will lose its element-type hint).

Also `arg.names()` for `PicocliParameterMetadata` returns `[paramLabel]` (a single element matching `paramLabel`), so `Arrays.stream(arg.names()).filter(n -> !n.equals(arg.paramLabel()))` becomes empty — but the code still writes `"aliases": []`. For options, names like `["--foo","-f","foo"]` are emitted as aliases including the un-prefixed `"foo"`, which conflicts with the primary key `properties` entry.

**Fix.** Don't overwrite `type` after computing it. Don't emit empty `aliases`. Filter primary name and dash-stripped form together.

---

### Finding 12 — Auditing exposes `commandName` directly without sanitization

**Confidence:** Low · **Impact:** UX · **Priority:** P2

**Location:** `src/java/org/apache/cassandra/cql3/statements/ExecuteCommandStatement.java:165` (`AuditLogContext(EXECUTE_COMMAND, commandName)`)

The raw user-supplied command name (which can contain anything `noncol_ident` accepts) flows to audit logs. If audit logs are downstream-consumed by tools that interpret control characters or quotes, this could produce confused log lines. Low risk.

---

## Cross-cutting observations

- **Asymmetric exception translation.** `ExecuteCommandStatement.execute()` translates `CommandValidationException → InvalidRequestException(stack)`, but `CommandMBeanAdapter.invoke()` translates `CommandValidationException → IllegalArgumentException(stack)`. Two different boundary errors produce different errors with different message formats. Standardize.
- **`CommandInvokerService.invokeCommand` catches `Throwable`** and wraps `Exception` and `Throwable` separately, but the `catch (Throwable e)` block then re-throws `CommandExecutionException` only — a `VirtualMachineError` (e.g. OOME) would get wrapped and swallowed. Subclasses of `Throwable` should be re-thrown via `JVMStabilityInspector.inspectThrowable(...)` per Cassandra convention.
- **The `MBEAN_DOMAIN` constant is duplicated** in `CommandInvokerService.java:71` and `CommandMBeanExecutionStrategy.java:51` — should share a single source.

---

## Wiki vs. implementation discrepancies

- The CEP page describes a `CommandResource`/`EXECUTE`/`DESCRIBE` permission model. The implementation **explicitly disables auth on the management port** (TODO comment in `ExecuteCommandStatement.authorize` and `CommandInvokerService.invokeCommand`) — so when authentication is enabled, all `INVOKE COMMAND` requests are rejected with `UnauthorizedException`. This is documented in the code as "temporary", but the production yaml ships `start_native_transport_management: false` — operators who do enable it without an `AllowAllAuthenticator` get a non-functional management port.
- The CEP page also describes `DESCRIBE COMMANDS` as a CQL statement; this is **not implemented**. Discovery is only via JMX (`getCommandNames`, `getJsonSchema`).
- The CEP describes per-command UUID tracking with async result polling; the implementation is **synchronous** (`invokeCommand` blocks the request thread), and the UUID is returned in the result row but there is no way to retrieve a past execution's status.

---

## Recommended priority

1. **P0 / Finding 1** — fix wire-message stack traces before merge. Easy, small.
2. **P0 / Finding 2** — fix CQL builder/grammar mismatch; without it the CQL strategy is unsafe to ship as default.
3. **P0 / Finding 3** — decide whether `Dispatcher.shutdown()` belongs in `destroyClientTransports()`.
4. **P1 / Findings 4–6** — fix the latent type-loss, MBean leak, and precedence semantics.
5. **P2 / Findings 7–12** — address as part of normal review tightening.

---

## Reproducers

The first two reproducers were executed against the project's compiled classes; the third is included as drop-in JUnit source.

### Repro 1 — `TypeConverterRegistryRepro.java`

Drives **Finding 4**: array branch coerces elements; `List`/`Set` branch does not.

Suggested location: drop into `test/unit/org/apache/cassandra/management/TypeConverterRegistryReproTest.java` after wrapping the body in `@Test` methods. The standalone form below uses reflection so it can run without JUnit.

```java
/*
 * Reproducer for TypeConverterRegistry#convertToArrayOrCollection asymmetry
 * (CASSANDRA-19476 / CEP-38 CQL Management API).
 *
 *   if (targetType.isArray())                  // lines ~125-136
 *       for (Object item : sourceCollection)
 *           Array.set(targetArray, index++, convertValueBasic(item, componentType));
 *
 *   // Collection branch -- lines ~138-149
 *   targetCollection.addAll(sourceCollection); // <-- NO PER-ELEMENT CONVERSION
 *
 * Inbound CQL list ['1','2','3'] (List<String> after CQL deserialization) into:
 *   - int[]/Integer[] field   -> elements ARE coerced to Integer
 *   - List<Integer>  field    -> elements stay String  (BUG)
 */
import java.lang.reflect.Method;
import java.util.List;

public class TypeConverterRegistryRepro
{
    public static void main(String[] args) throws Exception
    {
        Class<?> reg = Class.forName("org.apache.cassandra.management.picocli.TypeConverterRegistry");
        Method convert = reg.getDeclaredMethod("convertValueBasic", Object.class, Class.class);
        convert.setAccessible(true);

        List<String> input = List.of("1", "2", "3");

        // ---- Case A: array target (works) ------------------------------------------
        Object arrResult = convert.invoke(null, input, int[].class);
        int[] arr = (int[]) arrResult;
        check("array length", 3, arr.length);
        check("array[0]", 1, arr[0]);
        check("array[1]", 2, arr[1]);
        check("array[2]", 3, arr[2]);
        System.out.println("[OK ] int[] target produced ints: " + java.util.Arrays.toString(arr));

        // ---- Case B: Collection target (the bug) -----------------------------------
        Object listResult = convert.invoke(null, input, List.class);
        List<?> list = (List<?>) listResult;
        check("list size", 3, list.size());

        Class<?> firstClass = list.get(0).getClass();
        System.out.println("[BUG] List target element[0] runtime class = " + firstClass.getName()
                           + ", value = " + list.get(0));
        if (firstClass != String.class)
            throw new AssertionError("Expected element to remain String (the bug); got " + firstClass);

        @SuppressWarnings("unchecked")
        List<Integer> claimedIntegers = (List<Integer>) list;
        try
        {
            int sum = claimedIntegers.get(0) + claimedIntegers.get(1); // CCE here
            throw new AssertionError("Expected ClassCastException but got sum=" + sum);
        }
        catch (ClassCastException expected)
        {
            System.out.println("[BUG] Latent ClassCastException on consumer: " + expected.getMessage());
        }

        System.out.println();
        System.out.println("BUG CONFIRMED: TypeConverterRegistry.convertToArrayOrCollection");
        System.out.println("  Array branch  (lines ~125-136): converts elements via convertValueBasic.");
        System.out.println("  Collection br (lines ~138-149): does NOT -- raw addAll().");
    }

    private static void check(String label, int expected, int actual)
    {
        if (expected != actual)
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
    }

    /* JUnit-style equivalent (would belong under test/unit/.../management/):
     *
     * @Test
     * public void arrayTargetCoercesElements() throws Exception {
     *     int[] arr = (int[]) TypeConverterRegistry.convertValueBasic(List.of("1","2","3"), int[].class);
     *     assertThat(arr).containsExactly(1, 2, 3);
     * }
     *
     * @Test
     * public void listTargetDoesNotCoerceElements_BUG() throws Exception {
     *     Object out = TypeConverterRegistry.convertValueBasic(List.of("1","2","3"), List.class);
     *     assertThat(((List<?>) out).get(0)).isInstanceOf(String.class); // currently passes
     * }
     */
}
```

**Run:**
```
javac -cp build/classes/main -d /tmp/repro /tmp/claude/TypeConverterRegistryRepro.java
java  -cp build/classes/main:/tmp/repro TypeConverterRegistryRepro
```

---

### Repro 2 — `ProtocolPrecedenceRepro.java`

Drives **Finding 6**: `env=static_mbean` (default) + `sys=cql` ⇒ env silently ignored. Java has no portable env-mutation API, so the env read is faked by a `Map`; the `resolve()` body mirrors `ProtocolAwareExecutionStrategy.getExecutionStrategyTypeFromEnvAndSys()` line-for-line.

```java
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Minimal JUnit test that reproduces the precedence logic in
 * org.apache.cassandra.tools.nodetool.strategy.ProtocolAwareExecutionStrategy
 *     #getExecutionStrategyTypeFromEnvAndSys()
 *
 * Java does not let us mutate System.getenv portably, so the env read is
 * faked by a Map. The control flow mirrors the production method 1:1:
 *
 *   defaultStrategy = STATIC_MBEAN
 *   strategyEnv     = envGetEnum(defaultStrategy.name())
 *   strategySys     = sysGetEnum()
 *   return strategyEnv != defaultStrategy ? strategyEnv : strategySys;
 */
public class ProtocolPrecedenceRepro
{
    public enum Type { CQL, STATIC_MBEAN, COMMAND_MBEAN }

    static final String ENV_KEY = "CASSANDRA_CLI_EXECUTION_PROTOCOL";
    static final String SYS_KEY = "cassandra.cli.execution.protocol";
    static final String DEFAULT = "static_mbean";

    private final Map<String, String> fakeEnv = new HashMap<>();

    @Before public void clear()   { fakeEnv.clear(); System.clearProperty(SYS_KEY); }
    @After  public void cleanup() { System.clearProperty(SYS_KEY); }

    // Mirrors CassandraRelevantEnv#getEnum(true, T.class, defaultVal)
    private Type envGetEnum(String defaultVal)
    {
        String v = fakeEnv.get(ENV_KEY);
        v = (v == null) ? defaultVal : v;
        return Type.valueOf(v.toUpperCase(Locale.ROOT));
    }

    // Mirrors CassandraRelevantProperties#getEnum(true, T.class)
    private Type sysGetEnum()
    {
        String v = System.getProperty(SYS_KEY, DEFAULT);
        return Type.valueOf(v.toUpperCase(Locale.ROOT));
    }

    // Exact copy of getExecutionStrategyTypeFromEnvAndSys() body.
    private Type resolve()
    {
        Type defaultStrategy = Type.valueOf(DEFAULT.toUpperCase(Locale.ROOT));
        Type strategyEnv = envGetEnum(defaultStrategy.name());
        Type strategySys = sysGetEnum();
        return strategyEnv != defaultStrategy ? strategyEnv : strategySys;
    }

    @Test public void bothUnset_returnsDefault()
    {
        assertEquals(Type.STATIC_MBEAN, resolve());
    }

    @Test public void envSet_sysUnset_envWins()
    {
        fakeEnv.put(ENV_KEY, "cql");
        assertEquals(Type.CQL, resolve());
    }

    @Test public void envUnset_sysSet_sysWins()
    {
        System.setProperty(SYS_KEY, "cql");
        assertEquals(Type.CQL, resolve());
    }

    /** Suspected bug: env explicitly set to default is silently overridden by sys. */
    @Test public void envExplicitlyDefault_sysNonDefault_envIgnored()
    {
        fakeEnv.put(ENV_KEY, "static_mbean");   // operator explicitly forces default
        System.setProperty(SYS_KEY, "cql");     // sys says cql
        Type actual = resolve();
        System.out.println("env=static_mbean & sys=cql => " + actual);
        // Operator intent: env was explicitly set => STATIC_MBEAN.
        // Actual: sys wins => CQL (assertion FAILS, demonstrating the bug)
        assertEquals("BUG: explicit env=default loses to sys", Type.STATIC_MBEAN, actual);
    }

    @Test public void envNonDefault_sysNonDefault_envWins()
    {
        fakeEnv.put(ENV_KEY, "cql");
        System.setProperty(SYS_KEY, "command_mbean");
        assertEquals(Type.CQL, resolve());
    }
}
```

The fourth test (`envExplicitlyDefault_sysNonDefault_envIgnored`) is the demonstration of the bug — it fails with the production logic.

---

### Repro 3 — `CqlBuilderRoundTripRepro.java`

Drives **Finding 2**: writer/grammar asymmetry. Each `_Breaks` test produces CQL the parser then rejects; the trailing tests confirm escaping is correct (no injection). Drop into `test/unit/org/apache/cassandra/tools/nodetool/strategy/CqlBuilderRoundTripRepro.java` (you'll need a small `TestArgsFixture.fromMap` helper that wraps a `Map` into a `CommandExecutionArgs`).

```java
package org.apache.cassandra.tools.nodetool.strategy;

import java.lang.reflect.Method;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ExecuteCommandStatement;
import org.apache.cassandra.management.api.CommandExecutionArgs;

import static org.junit.Assert.*;

public class CqlBuilderRoundTripRepro
{
    private static Method build;

    @BeforeClass public static void setUp() throws Exception {
        build = CqlCommandExecutionStrategy.class.getDeclaredMethod(
                "buildCqlCommandString", String.class, CommandExecutionArgs.class);
        build.setAccessible(true);
    }

    private static String render(String cmd, Map<String,Object> opts) throws Exception {
        // TODO: replace with the project's CommandExecutionArgs builder.
        CommandExecutionArgs args = TestArgsFixture.fromMap(opts);
        return (String) build.invoke(null, cmd, args);
    }

    private static Map<String,Object> parse(String cql) throws Exception {
        ExecuteCommandStatement.Raw r =
            (ExecuteCommandStatement.Raw) QueryProcessor.parseStatement(cql);
        return r.args();
    }

    @Test public void listOfIntegersBreaks() throws Exception {
        String cql = render("foo", Map.of("ports", List.of(7000, 7001)));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #1 */ }
    }

    @Test public void listOfBooleansBreaks() throws Exception {
        String cql = render("foo", Map.of("flags", List.of(true, false)));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #2 */ }
    }

    @Test public void mixedListBreaks() throws Exception {
        String cql = render("foo", Map.of("xs", Arrays.asList("a", 1, true)));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #3 */ }
    }

    @Test public void byteArrayBreaks() throws Exception {
        String cql = render("foo", Map.of("payload", new byte[]{1,2,3}));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #4 */ }
    }

    @Test public void mapValueBreaks() throws Exception {
        String cql = render("foo", Map.of("m", Map.of("k","v")));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #5 */ }
    }

    @Test public void setValueBreaks() throws Exception {
        String cql = render("foo", Map.of("s", Set.of("a")));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #6 */ }
    }

    @Test public void doubleValueBreaks() throws Exception {
        String cql = render("foo", Map.of("d", 1.5d));
        try { parse(cql); fail("expected parse error: " + cql); }
        catch (Exception e) { /* BUG #7 */ }
    }

    @Test public void integerLookingStringStaysString() throws Exception {
        String cql = render("foo", Map.of("k", "1"));
        assertEquals("1", parse(cql).get("k"));
    }

    @Test public void embeddedSingleQuoteRoundTrips() throws Exception {
        String s = "O'Brien; DROP TABLE x;--";
        assertEquals(s, parse(render("foo", Map.of("k", s))).get("k"));
    }

    @Test public void backslashRoundTrips() throws Exception {
        String s = "C:\\Users\\x";
        assertEquals(s, parse(render("foo", Map.of("k", s))).get("k"));
    }

    @Test public void weirdKeyRoundTrips() throws Exception {
        Map<String,Object> opts = new LinkedHashMap<>();
        opts.put("weird\"key", "v");
        opts.put("key with space", "w");
        opts.put("--keyspace", "x");
        Map<String,Object> back = parse(render("foo", opts));
        assertEquals("v", back.get("weird\"key"));
        assertEquals("w", back.get("key with space"));
        assertEquals("x", back.get("--keyspace"));
    }

    @Test public void injectionStringIsContained() throws Exception {
        String s = "'; INVOKE COMMAND drop WITH x = '1";
        assertEquals(s, parse(render("foo", Map.of("k", s))).get("k"));
    }
}
```

---

*Report generated 2026-05-15. Reviewer: Claude. Tools: deep-review skill (Phase 0–3) plus five parallel verification subagents (logic+types, boundary I/O, concurrency+lifecycle, resources+serialization, absence/completeness). Two reproducers (`/tmp/claude/TypeConverterRegistryRepro.java`, `/tmp/claude/ProtocolPrecedenceRepro.java`) executed; the third is drop-in JUnit source.*

# CASSANDRA-19476: CQL-based Management API for nodetool commands

## Executive Summary

This patch introduces a **native CQL transport-based management interface** as an alternative to JMX for executing `nodetool` commands. It adds an `INVOKE COMMAND` CQL statement, a dedicated management transport listener (default port `11211`), a transport-agnostic command registry built from picocli metadata, and **three pluggable execution strategies** (CQL, command-MBean, static-MBean) so that `nodetool` can transparently operate over either protocol.

The patch also refactors `NodeProbe` — the long-standing JMX-tied facade — into an `MBeanAccessor` abstraction with two implementations: `RemoteJmxMBeanAccessor` (for the existing JMX path) and `InternalNodeMBeanAccessor` (for in-process execution on the server). This decouples command implementations from the transport.

**Scope:** 161 files, +9,230 / -1,179. Adds ~30 new classes under `org.apache.cassandra.management.*`, a new CQL grammar production, a new daemon-managed server, and a parallel test track exercising every nodetool command over CQL.

---

## High-level Architecture

```
                     ┌────────────────────────────────────────────────┐
                     │                  nodetool CLI                   │
                     │  (picocli, parses args, picks strategy)         │
                     └───┬────────────────┬─────────────────────┬─────┘
                         │                │                     │
        ProtocolAware    │                │                     │
        ExecutionStrategy│                │                     │
        (env / -D switch)│                │                     │
                         ▼                ▼                     ▼
              ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
              │ Cql              │ │ CommandMBean     │ │ StaticMBean      │
              │ ExecutionStrategy│ │ ExecutionStrategy│ │ ExecutionStrategy│
              │ (NEW default)    │ │ (NEW, JMX+JSON)  │ │ (legacy JMX path)│
              └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
                       │                    │                    │
                  CQL frame                JMX RMI               JMX RMI
                  port 11211               port 7199             port 7199
                       │                    │                    │
        ┌──────────────▼──────┐    ┌────────▼─────────┐  ┌───────▼────────┐
        │ NativeTransport     │    │ CommandMBean     │  │ Static          │
        │ ManagementService   │    │ Adapter (per cmd)│  │ MBeans          │
        │ (Netty Server,      │    │ DynamicMBean     │  │ (StorageService,│
        │  management flag)   │    │  invoke(JSON)    │  │  Compaction…)   │
        └──────────┬──────────┘    └────────┬─────────┘  └────────┬────────┘
                   │                        │                     │
         Dispatcher (managementExecutor)    │                     │
         + ManagementRequestProcessor       │                     │
                   │                        │                     │
                   ▼                        ▼                     │
        ┌────────────────────────────────────────┐                │
        │       CommandInvokerService            │                │
        │  (singleton, MBean, registry owner,    │                │
        │   bounded execution history, UUIDs,    │                │
        │   captured Output)                     │                │
        └──────────────────┬─────────────────────┘                │
                           │                                       │
                           ▼                                       │
                  ┌──────────────────┐                             │
                  │ CommandRegistry  │  ◄── ServiceLoader          │
                  │ (CassandraCmdReg)│      CommandsProvider       │
                  │  picocli adapter │      (PicocliCommandsProv)  │
                  └────────┬─────────┘                             │
                           │                                       │
                           ▼                                       │
                  ┌──────────────────┐                             │
                  │ Command<R>       │  ── execute(args, ctx)      │
                  │ (picocli leaf or │                             │
                  │  registry node)  │                             │
                  └────────┬─────────┘                             │
                           │                                       │
                           ▼                                       │
                  ┌──────────────────┐    ┌──────────────────┐     │
                  │ NodeProbe        │◄───┤ MBeanAccessor    │◄────┘
                  │ (now a thin      │    │  - RemoteJmx…    │
                  │  facade over an  │    │  - InternalNode… │
                  │  MBeanAccessor)  │    └──────────────────┘
                  └──────────────────┘
```

The same `Command<R>` instance can run on the server (in-process via `InternalNodeMBeanAccessor`) **or** on the client (over JMX via `RemoteJmxMBeanAccessor`). The transport is selected at the edges — never inside the command body.

---

## Detailed Analysis

### 1. New CQL surface: `INVOKE COMMAND`

Added to `Parser.g` / `Lexer.g`:

```cql
INVOKE COMMAND <commandName> [WITH "key1" = <value1> AND "key2" = <value2> ...];
```

* Values are restricted to `STRING_LITERAL`, `INTEGER`, `BOOLEAN`, or a homogeneous list of strings (`commandListValue`).
* `K_INVOKE` and `K_COMMAND` are added as **basic unreserved keywords** to avoid breaking existing schemas.
* Duplicate keys are rejected with a parser-level `addRecognitionError("Duplicate argument: …")`.
* The parser produces an `ExecuteCommandStatement.Raw(commandName, args)`, which is added as `st58` in the top-level `cqlStatement` rule.

`ExecuteCommandStatement.Raw` (in `cql3.statements`) is a `CQLStatement` that:

1. **Authorizes** — currently rejects unless `AllowAllAuthenticator` is active (see *Failure Modes / Security Posture*).
2. **Validates** — looks up the command in `CommandInvokerService.instance.getRegistry()`.
3. **Executes** — guards with `clientState.isInternal || clientState.isManagement()`, builds args via `CommandExecutionArgsSerde.fromMap`, calls `CommandInvokerService.invokeCommand`, and returns a 2-column `ResultSet`:

```
   execution_id (uuid)  |  output (text)
   ─────────────────────┼───────────────────────────────
   <UUID per execution> |  captured stdout/stderr text
```

Exception translation:

```
CommandAuthorizationException  → UnauthorizedException
CommandValidationException     → InvalidRequestException
CommandExecutionException      → CommandRequestExecutionException (carries executionId)
```

### 2. The Management Transport server

`NativeTransportManagementService` (new, in `org.apache.cassandra.service`) is an independent `CassandraDaemon.Server`:

```
┌─────────────────────────────────────────────────────────────────┐
│ NativeTransportManagementService                                 │
│  - own EventLoopGroup (Epoll or Nio)                             │
│  - own Server bound to rpc_management_address /                  │
│    rpc_management_interface, port = native_transport_management_ │
│    port (default 11211)                                          │
│  - same TLS encryption policy as the regular native transport    │
│  - Server.Builder().withManagementConnectionFlag(true)           │
└─────────────────────────────────────────────────────────────────┘
```

Lifecycle is wired into `CassandraDaemon`:

```
       setup()                    start()
         │                          │
         │  new NTMS()              │  startManagementTransport()
         │  CommandInvokerService   │     if start_native_transport_management:
         │     .instance.start()    │        nativeTransportManagementService.start()
         │                          │  …then regular native transport
         ▼                          ▼
 initializeClientTransports()   nativeTransportManagementService.start()
                                (must run BEFORE the regular native transport)
```

Shutdown calls `CommandInvokerService.instance.stop()` to unregister the per-command MBeans.

### 3. Connection tagging & dispatcher routing

`ServerConnection` gained an `isManagementConnection` flag, set at pipeline build time when the parent `Server` was created with `withManagementConnectionFlag(true)`. The flag travels with every request and is read by `Dispatcher.dispatch`:

```
Client TCP frame
        │
        ▼
   PipelineConfigurator ──► Server (mgmt flag) ──► ServerConnection(isManagement=true)
        │
        ▼
   Dispatcher.dispatch(channel, request, …)
        │
        ├── auth message  ───────►  authExecutor                  (existing)
        ├── ServerConnection.isManagementConnection()
        │       └─► managementExecutor                            (NEW)
        │             └─► ManagementRequestProcessor              (NEW)
        │                   └─► isManagementRequestAllowed(req)?  (NEW gate)
        │                         ├── ExecuteCommandStatement.Raw    → allow
        │                         ├── SELECT on system / virtual_*   → allow (driver discovery)
        │                         ├── USE system_*                   → allow (driver discovery)
        │                         ├── STARTUP/CREDENTIALS/AUTH/      → allow (protocol)
        │                         │   OPTIONS/REGISTER
        │                         └── EXECUTE/PREPARE/BATCH/other    → reject with
        │                                                             InvalidRequestException
        │                                                             "Only INVOKE COMMAND … allowed"
        └── default ─────────────► requestExecutor                 (existing)
```

This means a management connection is **structurally incapable** of running a regular CQL DML/DDL — even an authenticated, authorized client cannot use the management port for normal queries.

The new executor pool is sized by `native_transport_management_max_threads` (default `2`), independently from the regular request pool — a deliberate **bulkhead**: management traffic cannot starve normal traffic and vice versa.

### 4. `CommandInvokerService` — server-side execution coordinator

Singleton modeled after `StorageService` / `SnapshotManager`. Manages:

* The `CommandRegistry` (loaded once at start-up).
* MBean registration of every leaf command under
  `org.apache.cassandra.management:type=Command,name="<full name>"`.
* A bounded (max 100) `ExecutionHistory` deque of `(executionId, commandName, start, end, success/error)`.

#### State machine

```
                           ┌─────────────┐
                           │  CREATED    │
                           │ (started=false)
                           └──────┬──────┘
                              start()    (synchronized)
                                  │
                                  ▼
                      registerCommandMBeansRecursively()
                      MBeanWrapper.register(SERVICE)
                                  │
                                  ▼
                           ┌─────────────┐
                  ┌────────│   STARTED   │◄──────┐
                  │        │ (started=t) │       │
                  │        └──────┬──────┘       │
                  │           invokeCommand(..)  │
                  │                              │
                  │                              │
                stop()                           │
                  │                              │
                  ▼                              │
                unregisterCommandMBeans()        │
                MBeanWrapper.unregister          │
                  │                              │
                  ▼                              │
                ┌─────────────┐                  │
                │   STOPPED   │──── start() ─────┘
                │ (started=f) │
                └─────────────┘
```

#### Per-invocation flow

```
caller (CQL or JMX adapter)
   │
   ▼
invokeCommand(name, argsSupplier)
   │
   ├─ if !started:           IllegalStateException
   ├─ findRegistryCommand:   IllegalArgumentException("Command not found")
   │
   ├─ executionId = UUID.randomUUID()
   ├─ captured     = new CapturingOutput()                     (ByteArrayOutputStream-backed)
   ├─ ctx          = ServerCommandExecutionContext(new NodeProbe(InternalNodeMBeanAccessor, captured))
   ├─ history.add(record)                                       (bounded 100, ConcurrentLinkedDeque)
   │
   ├─ AUTH GATE: if authenticator.requireAuthentication() → CommandAuthorizationException
   │
   ├─ args = argsSupplier.get()    // deferred — only after gates pass
   ├─ validateArguments(args, metadata)                          // required options/parameters
   │
   ├─ command.execute(args, ctx)   // picocli body writes to captured stdout/stderr
   │
   └─ returns CommandResult(executionId, capturedOutput, startTime, durationMillis)

Exception fan-out:
   CommandAuthorizationException                → re-thrown as-is
   IllegalStateException / IllegalArgumentException → CommandValidationException
   Exception                                    → CommandExecutionException(executionId)
   Throwable (Errors)                           → CommandExecutionException
```

Notes:

* `argsSupplier` is **deferred**: argument deserialization happens after auth and started-state checks, avoiding wasted parsing on rejected requests.
* `validateArguments` only enforces *required* options/parameters; type coercion happens earlier in `CommandExecutionArgsSerde`.

### 5. `MBeanAccessor` and the `NodeProbe` refactor

Before:

```
nodetool command body ──► NodeProbe (concrete, owns JMXConnector,
                                     RMI factories, MBeanServerConnection)
```

After:

```
nodetool command body ──► NodeProbe (thin facade, holds Output + MBeanAccessor)
                                 │
                       ┌─────────┴────────────┐
                       ▼                      ▼
        RemoteJmxMBeanAccessor      InternalNodeMBeanAccessor
        (client-side, JMX RMI       (server-side, talks to the local
         + SSL plumbing)             MBeanServer in the same JVM)
```

* `MBeanAccessor` exposes `findMBean`, `findMBeanMetric`, `findColumnFamily`, `findCompressionDictionary`, `threadPoolInfos`, plus typed metric helpers (`findMBeanCounter`, `findMBeanGauge`, `findMBeanMeter`, `findMBeanTimer`, `findMBeanHistogram`).
* `Props` is a small builder (`type/path/keyspace/table/scope/name`) that replaces ad-hoc `ObjectName` string concatenation throughout the command tree.
* `NodeProbe` shrinks from ~2000 lines of glue to a transport-agnostic façade: `1029 +/-` change net but the *kind* of code inside changes drastically — most JMX-specific logic moves to `RemoteJmxMBeanAccessor`, and metric-lookup boilerplate becomes `accessor.findMBeanGauge(Props.metric(...))`.

### 6. Command abstraction & registry

```
┌────────────────────────────────────────────────────────────┐
│ Command<R> (interface)                                     │
│   metadata() : CommandMetadata                              │
│   execute(args, ctx) : R                                    │
│   default name(), description()                             │
└────────────────────────────────────────────────────────────┘
            ▲                          ▲
            │ implements               │ implements
            │                          │
┌───────────┴──────────┐      ┌────────┴────────────┐
│ PicocliCommandAdapter│      │ PicocliCommand      │
│ (leaf, wraps an      │      │ RegistryAdapter     │
│  AbstractCommand     │      │ (subtree, also      │
│  picocli class)      │      │  implements         │
└──────────────────────┘      │  CommandRegistry)   │
                              └─────────────────────┘

CommandsProvider (SPI, ServiceLoader-driven)
   └── PicocliCommandsProvider:
         walks `new CommandLine(NodetoolCommand.class)`
         and produces a Command<?> per top-level subcommand,
         recursively wrapping subcommand groups as registries.

CassandraCommandRegistry
   └── ConcurrentHashMap<String, Command<?>>
   └── EXCLUDES UNSUPPORTED_COMMANDS = {"repair", "consensus_admin"}
       (long-running, awaiting a future ProgressCommand interface)
   └── on duplicate name: IllegalStateException("Command name conflict: …")
```

Argument metadata (`OptionMetadata`, `ParameterMetadata`, `ArgumentMetadata`) is extracted from picocli at registry build time, so the rest of the system never imports picocli.

### 7. Three execution strategies, one CLI

`bin/nodetool` is updated to read `CASSANDRA_CLI_EXECUTION_PROTOCOL` (env) and `-Dcassandra.cli.execution.protocol` (sys-prop). Default is the legacy static-MBean strategy; selecting `cql` switches to the CQL transport and reads `native_transport_management_port` from `cassandra.yaml` for the connection port.

```
Strategy            │ Connect class    │ Wire format        │ Server-side entry
────────────────────┼──────────────────┼────────────────────┼────────────────────────────
STATIC_MBEAN (legacy)│ JmxConnect      │ JMX → static MBeans│ StorageServiceMBean, etc.
COMMAND_MBEAN       │ JmxConnect      │ JMX → JSON to       │ CommandMBeanAdapter →
                    │                  │ CommandMBeanAdapter │ CommandInvokerService
CQL (default)       │ CqlConnect      │ INVOKE COMMAND CQL  │ ExecuteCommandStatement →
                    │                  │ string              │ CommandInvokerService
```

`CqlCommandExecutionStrategy.buildCqlCommandString` is the client-side serializer:

```
INVOKE COMMAND <name>
  WITH "<opt-1>" = <cql-value>
   AND "<opt-2>" = <cql-value>
   AND "param0"   = <cql-value>      (positional → param<index>)
   AND ...;
```

Values are quoted/escaped via `CqlBuilder.appendWithSingleQuotes` and `ColumnIdentifier.maybeQuote` (the latter because option names like `keyspace`/`table` collide with CQL reserved words).

The result row is decoded back into `(executionId, output)`, the `output` is printed verbatim to the picocli command's stdout, and `executionId` is appended only when `cassandra.cli.execution.show_execution_id=true`.

---

## Configuration surface

New keys in `cassandra.yaml` / `cassandra_latest.yaml`:

| Key                                          | Default     | Purpose                                                            |
|----------------------------------------------|-------------|--------------------------------------------------------------------|
| `start_native_transport_management`          | `false`     | Master switch for the management transport server.                 |
| `rpc_management_address`                     | (rpc_address fallback) | Bind address for the management Netty server.            |
| `rpc_management_interface`                   | unset       | Bind by interface name; mutually exclusive with `rpc_management_address`. |
| `rpc_management_interface_prefer_ipv6`       | `false`     | Address-family selection on multi-family interfaces.               |
| `native_transport_management_port`           | `11211`     | Port for the management Netty server.                              |
| `native_transport_management_max_threads`    | `2`         | Size of `managementExecutor` (independent from request pool).      |

System properties / env:

* `CASSANDRA_CLI_EXECUTION_PROTOCOL` (env) and `cassandra.cli.execution.protocol` (sys) — selects the nodetool strategy: `STATIC_MBEAN` (default), `COMMAND_MBEAN`, or `CQL`.
* `cassandra.cli.execution.show_execution_id` — print the per-invocation UUID.
* `cassandra.start_native_transport_management` — daemon-level override for the master switch.

---

## Sequence — `nodetool info` over CQL

```
nodetool CLI               nodetool process              Cassandra node (mgmt port)
   │                              │                              │
   │ exec nodetool info           │                              │
   │ -Dcassandra.cli.execution    │                              │
   │  .protocol=cql               │                              │
   ├─────────────────────────────►│                              │
   │                              │ ProtocolAwareExecutionStrategy
   │                              │  resolves type=CQL           │
   │                              │ CqlCommandExecutionStrategy  │
   │                              │  picocli parses args,        │
   │                              │  CqlConnect.run()            │
   │                              │                              │
   │                              │ STARTUP / OPTIONS            │
   │                              ├─────────────────────────────►│
   │                              │                              │ ServerConnection
   │                              │                              │  isManagement=true
   │                              │                              │ ManagementRequestProcessor
   │                              │                              │  isManagementRequestAllowed
   │                              │                              │  → STARTUP allowed
   │                              │◄─────────────────────────────┤ READY
   │                              │                              │
   │                              │ "INVOKE COMMAND info;"       │
   │                              │ (CqlBuilder serialized)      │
   │                              ├─────────────────────────────►│
   │                              │                              │ Dispatcher.dispatch
   │                              │                              │  managementExecutor
   │                              │                              │ ManagementRequestProcessor
   │                              │                              │  isAllowed → ExecuteCommandStatement
   │                              │                              │ Raw.execute()
   │                              │                              │   findRegistryCommand("info")
   │                              │                              │   CommandInvokerService.invoke
   │                              │                              │     auth gate (AllowAll only)
   │                              │                              │     args = fromMap({}, metadata)
   │                              │                              │     command.execute(args, ctx)
   │                              │                              │       (picocli Info writes to
   │                              │                              │        captured Output)
   │                              │                              │   CommandResult(uuid, text)
   │                              │                              │ ResultSet.Rows[1]:
   │                              │                              │  (uuid, text)
   │                              │◄─────────────────────────────┤
   │                              │ println(output)              │
   │                              │ (optional execution id)      │
   │◄─────────────────────────────┤                              │
   │ exit code 0                  │                              │
```

---

## Assumptions

1. **Single managed registry per node.** `CommandInvokerService.instance` is a static singleton; `CassandraCommandRegistry` is constructed once via `ServiceLoader<CommandsProvider>`. The patch enforces uniqueness with `putIfAbsent` + `IllegalStateException` on conflict. Holds as long as no provider returns duplicate names.
2. **Picocli metadata is the source of truth.** `ExecuteCommandStatement` and the JMX adapter both call `CommandExecutionArgsSerde.fromMap(args, command.metadata())`, which casts metadata to `PicocliCommandMetadata`. Any future `Command<R>` not backed by picocli must update the serde.
3. **All currently in-scope commands fit the captured-`Output` model.** `Command.execute` returns `R` but `PicocliCommandAdapter` returns `Void`; the actual result is the captured byte buffer. Long-running / streaming output commands (`repair`, `consensus_admin`) are explicitly excluded — see `UNSUPPORTED_COMMANDS`.
4. **The management port is firewalled.** The yaml comments emphasize this. There is currently no built-in authentication gate (see below); operational isolation is the only protection.
5. **Driver compatibility on the management port.** `isManagementRequestAllowed` whitelists `SELECT` on system/virtual keyspaces and `USE system_*`. This relies on the assumption that drivers do not issue *other* metadata queries (DESCRIBE, prepared statement caches across non-system schemas, etc.) on the management connection.
6. **`AllowAllAuthenticator` is the only supported authenticator (initially).** Both the CQL statement and the service-level invoker reject `requireAuthentication() == true`. This is documented as a temporary gate awaiting a follow-up CASSANDRA ticket.
7. **Bulkheading.** `managementExecutor` defaults to 2 threads — enough to keep nodetool responsive but small enough to bound resource use. It assumes the host can absorb 2 concurrent management commands without contention with their own internal callers (e.g. compaction throttling).
8. **MBean naming uniqueness.** Per-command MBean is registered as `…:type=Command,name="<full name>"`. Conflict at registration time logs a warning *and* throws — but the throw is inside `try/catch (Exception)` that only logs, so on collision the command is silently absent from JMX (see *Failure Modes*).

---

## Failure Modes & Risks

### Security posture

* **Auth gate is binary.** Anything beyond `AllowAllAuthenticator` blocks *all* command execution. There is no role-level granularity; legitimate authenticated operators cannot run nodetool over CQL until the follow-up lands. Risk: operators flip `start_native_transport_management: true` *and* run with `AllowAllAuthenticator` to use it, which is exactly the unsafe combination on a non-isolated network.
* **Driver discovery whitelist.** `SELECT` on system / virtual keyspaces is allowed unconditionally on the management port. If a future virtual table exposes sensitive data (credentials cache contents, session info), it would be readable from the management port without further checks.
* **TLS posture is inherited.** `NativeTransportManagementService` reuses `getNativeProtocolEncryptionOptions().tlsEncryptionPolicy()`. If the regular native transport runs unencrypted, so will the management transport — there is no separate "management TLS required" toggle.

### Lifecycle & ordering

* `startManagementTransport()` runs *before* the regular native transport, but *after* `setup()`'s `CommandInvokerService.instance.start()`. If `setup()` returns successfully but `start()` is never called (e.g. a tool path that calls `setup()` only), the management server stays initialized-but-not-started; `isRunning()` returns `false`. Operators who automate "is mgmt up?" checks need to use the latter, not just port-bound state.
* `CommandInvokerService.shutdown()` is wired only into `CassandraDaemon.stop()`. Restart paths in tests need to call `start()` again — the synchronized guard handles re-entry but `started=false` after `stop()` is required.
* MBean registration failure inside `registerCommandMBeansRecursively` is `catch(Exception ex){ logger.warn(...) }`. A name collision throws inside the try block; the command becomes invisible over JMX **but stays present in the registry**, so CQL invocation still works. This silent divergence may confuse `getCommandMBeanName(...)` callers.

### Concurrency

* `ExecutionHistory.endTime/success/error` are `volatile`, but `completed()`/`failed()` write multiple volatiles non-atomically. A reader observing `endTime != 0 && success == false && error == null` is briefly possible. History is only used for diagnostics, so this is benign — but should be noted if anyone consumes it for auditing.
* `BoundedExecutionHistory.add` increments `size` *after* offering. Under heavy concurrent submissions, the deque can transiently exceed `maxSize` before the eviction loop catches up. Memory bounded by N concurrent threads × entry size — fine in practice given default 2-thread management executor.
* `commandMBeanNames.putIfAbsent` returning non-null throws inside the try block; this is the *intended* uniqueness guard, but it interleaves with `MBeanWrapper.registerMBean(... LOG)` which can succeed even when our local map already has the entry. The cleanup path on failure does **not** call `unregisterMBean`, so a duplicate MBean could persist if the throw races registration. Low likelihood (registry is built once, single thread), but worth a quick audit.

### Wire & parser edge cases

* `commandPropertyValue` accepts only `STRING_LITERAL | INTEGER | BOOLEAN | list-of-strings`. **Floats, bigints, blobs, UUIDs are not directly representable.** `CqlCommandExecutionStrategy.appendCqlValue` falls back to `appendWithSingleQuotes(value.toString())` for unknown types, which stringifies but loses type fidelity. Any command using a non-string typed argument relies on `TypeConverterRegistry` to round-trip via string.
* List values are parsed as `List<String>` only; nested numeric/boolean lists become strings on the wire. Sets are serialized client-side as `{...}` (CQL set literal) but the parser only accepts `[...]` lists — round-trips are asymmetric. The patch hides this because every nodetool command currently passes string-coerced values, but the asymmetry is there.
* Duplicate keys in `WITH` clauses are caught at parse time (good). Missing required options surface as `CommandValidationException` only after auth — not a leak risk, but error UX is "auth-then-validation" rather than "validation-first".

### Throughput / resource

* `managementExecutor` default is 2 threads. A single long-running command (think `gcstats -H`, `tablestats -H` on huge schemas) will block half the pool. `repair` is excluded on purpose, but other commands can still hold a thread for tens of seconds.
* `BoundedExecutionHistory` evicts **oldest** on overflow; a flood of requests can wash out diagnostic context for a slow failing command before an operator sees it.
* `CapturingOutput` accumulates the full output into a `ByteArrayOutputStream` before returning — large outputs (e.g. `tablestats` on a 10k-table cluster) are materialized in memory twice (once captured, once as the CQL `text` column).

### Testing surface

The patch ships:
* Unit tests for the parser (`ExecuteCommandStatementParseTest`), the args serde (`PicocliCommandArgsConverterTest`), the registry/service (`CommandServiceTest`), and the dispatcher gate (`MessageManagementDispatcherTest`).
* In-JVM dtests `ManagementRequestServingTest`, `ManagementTransportMultiNodeTest`, and `ManagementTransportProtocolTest` that exercise multi-node behavior of the management port.
* `CQLNodetoolProtocolTester` + per-command `*Test` updates so each existing nodetool unit test runs over both JMX and CQL paths (`SnapshotTest`, `CompactionStatsTest`, `TableStatsTest`, `InfoTest`, …).
* `NodetoolClassHierarchyTest` — guards the picocli class layout that the registry walker depends on.

---

## Key Insights

* **The decoupling is the real change.** The CQL transport is the user-visible feature, but the load-bearing refactor is `NodeProbe → MBeanAccessor`. After this patch, every nodetool command can run server-side without an RMI hop. Future work (streaming progress, structured results, server-side scheduling) becomes possible because `Command<R>` no longer leaks JMX/RMI types.
* **Two protocols, one registry.** The same picocli-derived metadata drives the CQL grammar's argument shape, the JMX `CommandMBeanAdapter` JSON contract, and the legacy `StaticMBean` strategy. There is one source of truth for "what is the option `--concurrent-compactors`?".
* **The dispatcher is the security boundary.** `isManagementRequestAllowed` is the choke point that turns the management port into "INVOKE COMMAND only". It must be kept in lock-step with the CQL grammar — any future statement type that should be allowed has to be added explicitly.
* **Today's safety relies on `AllowAllAuthenticator + firewall`.** This is documented in cassandra.yaml and enforced in code, but the combination is *only* safe if operators actually firewall port `11211`. The yaml comments lean on this hard.
* **Watch the silent-failure paths during MBean registration.** Conflicts log + throw but are caught upstream as warnings; CQL stays functional while JMX visibility silently drops. Worth tightening before GA.
* **`repair` and `consensus_admin` are explicitly out of scope.** A `ProgressCommand` follow-up is needed before the management API can claim feature parity with `nodetool` over JMX.

---

## Suggested Reviewer Focus

1. **`Dispatcher.isManagementRequestAllowed`** — security boundary; verify the whitelist is exhaustive and that no allowed shape can escalate (e.g. parsing succeeding on a crafted query that confuses the gate).
2. **`ExecuteCommandStatement.Raw` auth + validate ordering** — confirm we never execute before the AllowAll gate, and that the `clientState.isInternal || clientState.isManagement()` invariant cannot be bypassed by replaying the same statement on the regular native port.
3. **`CommandInvokerService.invokeCommand` exception fan-out** — confirm `Throwable` -> `CommandExecutionException` is acceptable (we swallow `Error` semantics into a checked-style boundary).
4. **`CassandraCommandRegistry.UNSUPPORTED_COMMANDS`** — should be a public constant or annotation rather than a static set, so `repair`/`consensus_admin` can opt in once `ProgressCommand` lands.
5. **`NativeTransportManagementService` lifecycle** — verify `destroy()` ordering vs in-flight `managementExecutor` tasks; today `stop(false)` does not drain.
6. **`BoundedExecutionHistory`** — consider whether the increment-then-evict race is acceptable, or replace with a bounded blocking queue.
7. **MBean registration silent-divergence** — decide whether to fail-fast on conflict instead of logging and continuing.

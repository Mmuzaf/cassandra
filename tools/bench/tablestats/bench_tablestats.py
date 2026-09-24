#!/usr/bin/env python3
"""Time `nodetool tablestats` over JMX vs CQL as the table count grows; emit CSV (ms) + SVG chart.

Usage: ./bench_tablestats.py [--max 200] [--step 20] [--reps 3] [--jmx-port 7100] [--keep]
Requires a running local node on 127.0.0.1 (JMX 7199, CQL 9042, management CQL 11211).
"""
import argparse, csv, statistics, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent


TIMEOUT = 120  # seconds per nodetool/cqlsh call


def run(cmd, retries=1):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        if retries > 0:
            print(f"  timed out after {TIMEOUT}s, retrying: {' '.join(cmd)}", file=sys.stderr, flush=True)
            return run(cmd, retries - 1)
        sys.exit(f"timed out twice: {' '.join(cmd)}\n"
                 "while it hangs, inspect with: jstack $(pgrep -f org.apache.cassandra.tools.NodeTool)")
    if r.returncode != 0:
        sys.exit(f"failed: {' '.join(cmd)}\n{r.stderr}")
    return r.stdout


def timed(cmd, reps):
    samples = []
    for _ in range(reps):
        t = time.perf_counter()
        run(cmd)
        samples.append((time.perf_counter() - t) * 1000)
    return statistics.median(samples)


def create_tables(cqlsh, host, ks, start, end):
    stmts = [f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"]
    stmts += [f"CREATE TABLE IF NOT EXISTS {ks}.t{i} (k int PRIMARY KEY, v text);" for i in range(start, end)]
    run([cqlsh, host, "-e", " ".join(stmts)])
    time.sleep(5)  # let schema flushes/compactions settle before timing


def svg(rows, path):
    W, H, L, B = 720, 420, 70, 50
    xs = [r["tables"] for r in rows]
    ymax = max(max(r["jmx"], r["cql"]) for r in rows) * 1.1 or 1
    xmax = max(xs) or 1
    px = lambda x: L + (W - L - 20) * x / xmax
    py = lambda y: H - B - (H - B - 20) * y / ymax
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="12">',
           f'<rect width="{W}" height="{H}" fill="white"/>',
           f'<line x1="{L}" y1="{H-B}" x2="{W-20}" y2="{H-B}" stroke="black"/>',
           f'<line x1="{L}" y1="20" x2="{L}" y2="{H-B}" stroke="black"/>',
           f'<text x="{W/2}" y="{H-12}" text-anchor="middle">extra tables</text>',
           f'<text x="15" y="{H/2}" transform="rotate(-90 15 {H/2})" text-anchor="middle">nodetool tablestats wall time, ms</text>']
    for i in range(6):
        y = ymax * i / 5
        out.append(f'<text x="{L-6}" y="{py(y)+4}" text-anchor="end">{y:.0f}</text>')
        out.append(f'<line x1="{L}" y1="{py(y)}" x2="{W-20}" y2="{py(y)}" stroke="#ddd"/>')
    for x in xs:
        out.append(f'<text x="{px(x)}" y="{H-B+16}" text-anchor="middle">{x}</text>')
    for j, (name, color) in enumerate({"jmx": "#d62728", "cql": "#1f77b4"}.items()):
        pts = " ".join(f"{px(r['tables'])},{py(r[name])}" for r in rows)
        out.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/>')
        out += [f'<circle cx="{px(r["tables"])}" cy="{py(r[name])}" r="3" fill="{color}"/>' for r in rows]
        out.append(f'<rect x="{L+10}" y="{28+j*18}" width="12" height="12" fill="{color}"/>')
        out.append(f'<text x="{L+28}" y="{38+j*18}">{name.upper()}</text>')
    out.append("</svg>")
    path.write_text("\n".join(out))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=200)
    ap.add_argument("--step", type=int, default=20)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--jmx-port", default="7199", help="ccm node1 uses 7100")
    ap.add_argument("--keyspace", default="bench_tablestats")
    ap.add_argument("--nodetool", default=str(HERE / "bin/nodetool"))
    ap.add_argument("--cqlsh", default=str(HERE / "bin/cqlsh"))
    ap.add_argument("--out", default="tablestats_bench")
    ap.add_argument("--keep", action="store_true", help="do not drop the benchmark keyspace")
    a = ap.parse_args()

    run([a.cqlsh, a.host, "-e", f"DROP KEYSPACE IF EXISTS {a.keyspace};"])  # start from a clean slate
    steps = range(0, a.max + 1, a.step)
    rows, created = [], 0
    for n in steps:
        if n > created:
            create_tables(a.cqlsh, a.host, a.keyspace, created, n)
            created = n
        row = {"tables": n}
        # -p means JMX port in default mode but CQL port in cql mode, so only pass it for jmx
        row["jmx"] = timed([a.nodetool, "-h", a.host, "-p", a.jmx_port, "tablestats"], a.reps)
        row["cql"] = timed([a.nodetool, "-Dcassandra.cli.execution.protocol=cql", "-h", a.host, "tablestats"], a.reps)
        row["speedup"] = row["jmx"] / row["cql"] if row["cql"] else 0
        rows.append(row)
        print(f"tables={n:5d}  jmx={row['jmx']:8.0f}ms  cql={row['cql']:8.0f}ms  x{row['speedup']:.1f}", flush=True)

    with open(f"{a.out}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["tables", "jmx", "cql", "speedup"])
        w.writeheader(); w.writerows(rows)
    svg(rows, Path(f"{a.out}.svg"))
    print(f"wrote {a.out}.csv and {a.out}.svg", flush=True)

    if not a.keep:  # results are saved first: dropping hundreds of tables is slow and may time out
        run([a.cqlsh, a.host, "-e", f"DROP KEYSPACE IF EXISTS {a.keyspace};"])


if __name__ == "__main__":
    main()

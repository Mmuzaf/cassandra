#!/usr/bin/env python3
"""
Analyze TODO/FIXME additions and removals across git history, aggregated by year.

Usage:
    python3 analyze_todos.py                         # Analyze current branch (HEAD)
    python3 analyze_todos.py --all                   # Analyze all branches
    python3 analyze_todos.py --project MyProject     # Override project name prefix

Output:
    - Statistics table to stderr
    - Markdown with Mermaid charts to stdout

Example:
    python3 analyze_todos.py > todo_analysis.md
"""

import subprocess
import re
import sys
import math
from collections import defaultdict

TODO_PATTERN = re.compile(r'\b(TODO|FIXME)\b', re.IGNORECASE)
COMMIT_DATE_PATTERN = re.compile(r'^COMMIT (\d{4})')


# ---------------------------------------------------------------------------
# Project name
# ---------------------------------------------------------------------------

def get_project_name():
    if '--project' in sys.argv:
        idx = sys.argv.index('--project')
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]

    result = subprocess.run(
        ['git', 'remote', 'get-url', 'origin'],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        url = result.stdout.strip()
        # Handles both:
        #   https://github.com/owner/repo.git
        #   git@github.com:owner/repo.git
        m = re.search(r'[:/]([^/]+)/([^/]+?)(?:\.git)?$', url)
        if m:
            return f"{m.group(1)}:{m.group(2)}"

    return 'Project'


# ---------------------------------------------------------------------------
# Git analysis
# ---------------------------------------------------------------------------

def analyze_git_history(all_branches=False):
    added = defaultdict(int)
    removed = defaultdict(int)

    cmd = [
        'git', 'log', '--no-merges', '--format=COMMIT %ai',
        '-G', '[Tt][Oo][Dd][Oo]|[Ff][Ii][Xx][Mm][Ee]',
        '-p',
    ]
    if all_branches:
        cmd.insert(2, '--all')

    scope = "all branches" if all_branches else "current branch (HEAD)"
    print(f"Streaming git history ({scope})...", file=sys.stderr)
    print("This may take several minutes for large repositories.\n", file=sys.stderr)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding='utf-8',
        errors='replace',
    )

    current_year = None
    commits = 0

    for line in process.stdout:
        line = line.rstrip('\n')

        m = COMMIT_DATE_PATTERN.match(line)
        if m:
            current_year = int(m.group(1))
            commits += 1
            if commits % 5000 == 0:
                print(f"  Processed {commits:,} commits...", file=sys.stderr)
            continue

        if current_year is None:
            continue

        if line.startswith('+') and not line.startswith('+++'):
            if TODO_PATTERN.search(line[1:]):
                added[current_year] += 1
        elif line.startswith('-') and not line.startswith('---'):
            if TODO_PATTERN.search(line[1:]):
                removed[current_year] += 1

    process.wait()
    print(f"Done. Analyzed {commits:,} commits total.", file=sys.stderr)
    return added, removed


# ---------------------------------------------------------------------------
# LOC counting (second git pass, all commits, numstat)
# ---------------------------------------------------------------------------

def count_loc_per_year(all_branches=False):
    """Count lines added/removed per year across all commits using --numstat."""
    cmd = ['git', 'log', '--no-merges', '--format=COMMIT %ai', '--numstat']
    if all_branches:
        cmd.insert(2, '--all')

    print("Counting lines of code per year...", file=sys.stderr)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding='utf-8',
        errors='replace',
    )

    loc_added   = defaultdict(int)
    loc_removed = defaultdict(int)
    current_year = None

    for line in process.stdout:
        line = line.rstrip('\n')
        m = COMMIT_DATE_PATTERN.match(line)
        if m:
            current_year = int(m.group(1))
            continue
        if current_year is None:
            continue
        # numstat lines: "<added>\t<removed>\t<filename>"
        # binary files show "-\t-\t<filename>", skip those
        parts = line.split('\t')
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            loc_added[current_year]   += int(parts[0])
            loc_removed[current_year] += int(parts[1])

    process.wait()
    print("Done counting LOC.", file=sys.stderr)
    return loc_added, loc_removed


# ---------------------------------------------------------------------------
# Mermaid init block builder
# ---------------------------------------------------------------------------

def _fmt(d):
    """Serialize a dict to Mermaid's pseudo-JSON init format."""
    parts = []
    for k, v in d.items():
        if isinstance(v, dict):
            parts.append(f"'{k}': {{{_fmt(v)}}}")
        elif isinstance(v, (int, float)):
            parts.append(f"'{k}': {v}")
        else:
            parts.append(f"'{k}': '{v}'")
    return ', '.join(parts)


DEFAULT_COLOR = '#4472c4'  # steel blue, clearly visible on white


def build_init(colors=None, width=None, height=None):
    """Build a %%{init}%% block. White background applied to all charts."""
    theme_vars = {
        'background': '#ffffff',
        'xyChart': {'plotColorPalette': ','.join(colors) if colors else DEFAULT_COLOR},
    }

    config = {'theme': 'base', 'themeVariables': theme_vars}
    if width or height:
        xy = {}
        if width:
            xy['width'] = width
        if height:
            xy['height'] = height
        config['xyChart'] = xy

    return f"%%{{init: {{{_fmt(config)}}}}}%%\n"


# ---------------------------------------------------------------------------
# Axis helpers
# ---------------------------------------------------------------------------

def nice_ceil(val):
    if val <= 0:
        return 10
    step = 10 ** max(0, math.floor(math.log10(val)) - 1)
    return math.ceil(val / step) * step


def nice_floor(val):
    if val >= 0:
        return 0
    step = 10 ** max(0, math.floor(math.log10(abs(val))) - 1)
    return math.floor(val / step) * step


def axis_bounds(values):
    return nice_floor(min(values, default=0)), nice_ceil(max(values, default=1))


# ---------------------------------------------------------------------------
# Chart generators
# ---------------------------------------------------------------------------

def generate_mermaid_combined(added, removed, project_name):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    if not all_years:
        return None

    add_values = [added.get(y, 0)   for y in all_years]
    rem_values = [removed.get(y, 0) for y in all_years]
    x_labels   = [str(y)            for y in all_years]
    y_max = nice_ceil(max(max(add_values), max(rem_values), 1))

    return build_init(colors=['#22aa44', '#dd4444'], width=1200, height=500) + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — TODO/FIXME Additions (green) and Removals (red) by Year"',
        f'    x-axis [{", ".join(x_labels)}]',
        f'    y-axis "Lines" 0 --> {y_max}',
        f'    bar [{", ".join(str(v) for v in add_values)}]',
        f'    bar [{", ".join(str(v) for v in rem_values)}]',
    ])


def generate_mermaid_single(data, label, project_name, color=None):
    all_years = sorted(data.keys())
    if not all_years:
        return None

    values = [data[y] for y in all_years]
    x_labels = [str(y) for y in all_years]
    return build_init(colors=[color] if color else None) + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — {label}"',
        f'    x-axis [{", ".join(x_labels)}]',
        f'    y-axis "Lines" 0 --> {nice_ceil(max(values, default=1))}',
        f'    bar [{", ".join(str(v) for v in values)}]',
    ])


def generate_mermaid_cumulative_net(added, removed, project_name):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    if not all_years:
        return None

    cumulative, values = 0, []
    for y in all_years:
        cumulative += added.get(y, 0) - removed.get(y, 0)
        values.append(cumulative)

    y_min, y_max = axis_bounds(values)
    return build_init() + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — Cumulative TODO/FIXME Debt over Time"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "Accumulated Lines" {y_min} --> {y_max}',
        f'    line [{", ".join(str(v) for v in values)}]',
    ])


def generate_mermaid_net_per_year(added, removed, project_name):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    if not all_years:
        return None

    values = [added.get(y, 0) - removed.get(y, 0) for y in all_years]
    y_min, y_max = axis_bounds(values)
    return build_init() + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — TODO/FIXME Net Change per Year"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "Net Lines" {y_min} --> {y_max}',
        f'    line [{", ".join(str(v) for v in values)}]',
    ])


def generate_mermaid_cleanup_ratio(added, removed, project_name):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    if not all_years:
        return None

    values = [
        round(removed.get(y, 0) / added.get(y, 0) * 100) if added.get(y, 0) > 0 else 0
        for y in all_years
    ]
    return build_init() + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — TODO/FIXME Cleanup Ratio per Year (Removals / Additions %)"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "%" 0 --> {nice_ceil(max(values, default=100))}',
        f'    line [{", ".join(str(v) for v in values)}]',
    ])


def generate_mermaid_cumulative_both(added, removed, project_name):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    if not all_years:
        return None

    cum_add = cum_rem = 0
    add_values, rem_values = [], []
    for y in all_years:
        cum_add += added.get(y, 0)
        cum_rem += removed.get(y, 0)
        add_values.append(cum_add)
        rem_values.append(cum_rem)

    y_max = nice_ceil(max(max(add_values), max(rem_values)))
    return build_init(colors=['#22aa44', '#dd4444']) + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — Cumulative TODO/FIXME Additions vs Removals"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "Lines" 0 --> {y_max}',
        f'    line [{", ".join(str(v) for v in add_values)}]',
        f'    line [{", ".join(str(v) for v in rem_values)}]',
    ])


def generate_mermaid_introduction_rate(added, loc_added, project_name):
    """TODOs added / kLOC added per year — developer discipline at time of writing."""
    all_years = sorted(set(list(added.keys()) + list(loc_added.keys())))
    if not all_years:
        return None

    values = []
    for y in all_years:
        kloc = loc_added.get(y, 0) / 1000
        rate = round(added.get(y, 0) / kloc, 2) if kloc > 0 else 0
        values.append(rate)

    y_min, y_max = axis_bounds(values)
    return build_init() + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — TODO/FIXME Introduction Rate (per kLOC added)"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "TODOs / kLOC" {y_min} --> {y_max}',
        f'    line [{", ".join(str(v) for v in values)}]',
    ])


def generate_mermaid_debt_density(added, removed, loc_added, loc_removed, project_name):
    """Cumulative net TODOs / cumulative net kLOC — running debt burden in the codebase."""
    all_years = sorted(set(list(added.keys()) + list(loc_added.keys())))
    if not all_years:
        return None

    cum_todos = cum_loc = 0
    values = []
    for y in all_years:
        cum_todos += added.get(y, 0) - removed.get(y, 0)
        cum_loc   += loc_added.get(y, 0) - loc_removed.get(y, 0)
        density = round(cum_todos / (cum_loc / 1000), 2) if cum_loc > 0 else 0
        values.append(density)

    y_min, y_max = axis_bounds(values)
    return build_init() + '\n'.join([
        'xychart-beta',
        f'    title "{project_name} — Running TODO/FIXME Debt Density (per kLOC)"',
        f'    x-axis [{", ".join(str(y) for y in all_years)}]',
        f'    y-axis "TODOs / kLOC" {y_min} --> {y_max}',
        f'    line [{", ".join(str(v) for v in values)}]',
    ])


# ---------------------------------------------------------------------------
# Stats table
# ---------------------------------------------------------------------------

def print_stats(added, removed):
    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    total_a = total_r = 0

    print(f"\n{'Year':>6} | {'Added':>8} | {'Removed':>8} | {'Net':>9}", file=sys.stderr)
    print("─" * 43, file=sys.stderr)

    for year in all_years:
        a = added.get(year, 0)
        r = removed.get(year, 0)
        total_a += a
        total_r += r
        net = a - r
        sign = '+' if net >= 0 else ''
        print(f"{year:>6} | {a:>8,} | {r:>8,} | {sign}{net:>8,}", file=sys.stderr)

    print("─" * 43, file=sys.stderr)
    net = total_a - total_r
    sign = '+' if net >= 0 else ''
    print(f"{'TOTAL':>6} | {total_a:>8,} | {total_r:>8,} | {sign}{net:>8,}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    all_branches = '--all' in sys.argv
    project_name = get_project_name()

    added, removed = analyze_git_history(all_branches)

    if not added and not removed:
        print("No TODO/FIXME mentions found in git history.", file=sys.stderr)
        sys.exit(1)

    print_stats(added, removed)

    loc_added, loc_removed = count_loc_per_year(all_branches)

    sections = [
        (
            "## Combined: Additions and Removals by Year",
            "Each year shows two adjacent bars: additions (green) and removals (red).",
            generate_mermaid_combined(added, removed, project_name),
        ),
        (
            "## Additions by Year",
            "Number of lines containing `TODO` or `FIXME` added per year. "
            "Peaks indicate periods of rapid feature development or technical debt introduction.",
            generate_mermaid_single(added, "TODO/FIXME Additions by Year", project_name, color="#22aa44"),
        ),
        (
            "## Removals by Year",
            "Number of lines containing `TODO` or `FIXME` removed per year. "
            "Peaks reflect cleanup efforts or resolution of previously noted issues.",
            generate_mermaid_single(removed, "TODO/FIXME Removals by Year", project_name, color="#dd4444"),
        ),
        (
            "## Cumulative Debt over Time",
            "Running total of unresolved `TODO`/`FIXME` items (additions − removals, accumulated year by year). "
            "A rising line means debt is growing; a falling line means active cleanup is outpacing new additions.",
            generate_mermaid_cumulative_net(added, removed, project_name),
        ),
        (
            "## Net Change per Year",
            "Difference between additions and removals for each year (additions − removals). "
            "Positive values mean more TODOs were introduced than resolved; "
            "negative values indicate a net cleanup year.",
            generate_mermaid_net_per_year(added, removed, project_name),
        ),
        (
            "## Cleanup Ratio per Year",
            "Percentage of new `TODO`/`FIXME` lines that were also removed in the same year "
            "(removals / additions × 100). "
            "100% = breakeven; above 100% = paying down old debt; well below 100% = accumulating backlog.",
            generate_mermaid_cleanup_ratio(added, removed, project_name),
        ),
        (
            "## Cumulative Additions vs Removals",
            "Total ever-added (green) and total ever-removed (red) `TODO`/`FIXME` lines over the project lifetime. "
            "The widening gap between the two lines represents the current unresolved backlog.",
            generate_mermaid_cumulative_both(added, removed, project_name),
        ),
        (
            "## TODO/FIXME Introduction Rate (per kLOC added)",
            "For every 1,000 lines of new code written in a given year, how many `TODO`/`FIXME` comments were introduced. "
            "Measures developer discipline at the time of writing, independent of total codebase size. "
            "A spike means the team was moving fast and leaving more markers behind; a falling trend means code quality is improving.",
            generate_mermaid_introduction_rate(added, loc_added, project_name),
        ),
        (
            "## Running TODO/FIXME Debt Density (per kLOC)",
            "The current density of unresolved `TODO`/`FIXME` items per 1,000 lines of code in the codebase, "
            "tracked at the end of each year. "
            "Unlike raw counts, this normalises for codebase growth — a flat or falling line means the codebase is getting "
            "cleaner even if absolute TODO counts rise. An upward trend is a red flag: debt is growing faster than the code.",
            generate_mermaid_debt_density(added, removed, loc_added, loc_removed, project_name),
        ),
    ]

    all_years = sorted(set(list(added.keys()) + list(removed.keys())))
    year_range = f"{all_years[0]}–{all_years[-1]}" if all_years else "N/A"
    total_added = sum(added.values())
    total_removed = sum(removed.values())

    print(f"\n# {project_name} — TODO/FIXME Analysis\n")
    print(f"""> This report scans the full git commit history of **{project_name}** ({year_range}) \
and tracks every line containing a `TODO` or `FIXME` comment (case-insensitive) \
that was **added** or **removed** in each commit. Merge commits are excluded to avoid double-counting.
>
> For each commit, the diff is parsed line by line: lines prefixed with `+` are additions, \
lines prefixed with `-` are removals. Results are aggregated by the commit year.
>
> **{total_added:,}** TODO/FIXME lines added · **{total_removed:,}** removed · \
**{total_added - total_removed:,}** unresolved backlog across **{len(all_years)}** years.
""")
    for heading, description, chart in sections:
        if chart:
            print(f"{heading}\n\n{description}\n\n```mermaid\n{chart}\n```\n")


if __name__ == '__main__':
    main()

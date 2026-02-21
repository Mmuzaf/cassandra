
# Mmuzaf:cassandra — TODO/FIXME Analysis

> This report scans the full git commit history of **Mmuzaf:cassandra** (2009–2026) and tracks every line containing a `TODO` or `FIXME` comment (case-insensitive) that was **added** or **removed** in each commit. Merge commits are excluded to avoid double-counting.
>
> For each commit, the diff is parsed line by line: lines prefixed with `+` are additions, lines prefixed with `-` are removals. Results are aggregated by the commit year.
>
> **2,827** TODO/FIXME lines added · **1,792** removed · **1,035** unresolved backlog across **18** years.

## Combined: Additions and Removals by Year

Each year shows two adjacent bars: additions (green) and removals (red).

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44,#dd4444'}}, 'xyChart': {'width': 1200, 'height': 500}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Additions (green) and Removals (red) by Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 460
    bar [266, 231, 143, 57, 56, 168, 108, 99, 16, 77, 76, 86, 100, 133, 436, 451, 295, 29]
    bar [215, 186, 170, 64, 40, 83, 109, 81, 11, 32, 13, 55, 39, 15, 126, 317, 222, 14]
```

## Additions by Year

Number of lines containing `TODO` or `FIXME` added per year. Peaks indicate periods of rapid feature development or technical debt introduction.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Additions by Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 460
    bar [266, 231, 143, 57, 56, 168, 108, 99, 16, 77, 76, 86, 100, 133, 436, 451, 295, 29]
```

## Removals by Year

Number of lines containing `TODO` or `FIXME` removed per year. Peaks reflect cleanup efforts or resolution of previously noted issues.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#dd4444'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Removals by Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 320
    bar [215, 186, 170, 64, 40, 83, 109, 81, 11, 32, 13, 55, 39, 15, 126, 317, 222, 14]
```

## Cumulative Debt over Time

Running total of unresolved `TODO`/`FIXME` items (additions − removals, accumulated year by year). A rising line means debt is growing; a falling line means active cleanup is outpacing new additions.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — Cumulative TODO/FIXME Debt over Time"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Accumulated Lines" 0 --> 1100
    line [51, 96, 69, 62, 78, 163, 162, 180, 185, 230, 293, 324, 385, 503, 813, 947, 1020, 1035]
```

## Net Change per Year

Difference between additions and removals for each year (additions − removals). Positive values mean more TODOs were introduced than resolved; negative values indicate a net cleanup year.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Net Change per Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Net Lines" -27 --> 310
    line [51, 45, -27, -7, 16, 85, -1, 18, 5, 45, 63, 31, 61, 118, 310, 134, 73, 15]
```

## Cleanup Ratio per Year

Percentage of new `TODO`/`FIXME` lines that were also removed in the same year (removals / additions × 100). 100% = breakeven; above 100% = paying down old debt; well below 100% = accumulating backlog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Cleanup Ratio per Year (Removals / Additions %)"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "%" 0 --> 120
    line [81, 81, 119, 112, 71, 49, 101, 82, 69, 42, 17, 64, 39, 11, 29, 70, 75, 48]
```

## Cumulative Additions vs Removals

Total ever-added (green) and total ever-removed (red) `TODO`/`FIXME` lines over the project lifetime. The widening gap between the two lines represents the current unresolved backlog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#22aa44,#dd4444'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — Cumulative TODO/FIXME Additions vs Removals"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 2900
    line [266, 497, 640, 697, 753, 921, 1029, 1128, 1144, 1221, 1297, 1383, 1483, 1616, 2052, 2503, 2798, 2827]
    line [215, 401, 571, 635, 675, 758, 867, 948, 959, 991, 1004, 1059, 1098, 1113, 1239, 1556, 1778, 1792]
```

## TODO/FIXME Introduction Rate (per kLOC added)

For every 1,000 lines of new code written in a given year, how many `TODO`/`FIXME` comments were introduced. Measures developer discipline at the time of writing, independent of total codebase size. A spike means the team was moving fast and leaving more markers behind; a falling trend means code quality is improving.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — TODO/FIXME Introduction Rate (per kLOC added)"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "TODOs / kLOC" 0 --> 5
    line [1.19, 1.19, 0.59, 0.39, 0.35, 0.79, 0.35, 0.48, 0.21, 0.7, 0.96, 0.24, 0.49, 0.47, 1.03, 2.34, 1.39, 4.94]
```

## Running TODO/FIXME Debt Density (per kLOC)

The current density of unresolved `TODO`/`FIXME` items per 1,000 lines of code in the codebase, tracked at the end of each year. Unlike raw counts, this normalises for codebase growth — a flat or falling line means the codebase is getting cleaner even if absolute TODO counts rise. An upward trend is a red flag: debt is growing faster than the code.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'background': '#ffffff', 'xyChart': {'plotColorPalette': '#4472c4'}}}}%%
xychart-beta
    title "Mmuzaf:cassandra — Running TODO/FIXME Debt Density (per kLOC)"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "TODOs / kLOC" 0 --> 1
    line [0.74, 0.63, 0.37, 0.24, 0.24, 0.41, 0.29, 0.31, 0.3, 0.33, 0.39, 0.3, 0.32, 0.35, 0.49, 0.54, 0.53, 0.54]
```


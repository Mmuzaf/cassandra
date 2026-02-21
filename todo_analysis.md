
# cassandra_claude — TODO/FIXME Analysis

## Combined: Additions and Removals by Year

Each year shows two adjacent bars: additions (`+`) and removals (`-`). A zero-height spacer between year groups aids readability.

```mermaid
%%{init: {'xyChart': {'width': 1800, 'height': 500}}}%%
xychart-beta
    title "cassandra_claude — TODO/FIXME Additions (+) and Removals (-) by Year"
    x-axis ["09+", "09-", "", "10+", "10-", "", "11+", "11-", "", "12+", "12-", "", "13+", "13-", "", "14+", "14-", "", "15+", "15-", "", "16+", "16-", "", "17+", "17-", "", "18+", "18-", "", "19+", "19-", "", "20+", "20-", "", "21+", "21-", "", "22+", "22-", "", "23+", "23-", "", "24+", "24-", "", "25+", "25-", "", "26+", "26-"]
    y-axis "Lines" 0 --> 460
    bar [266, 215, 0, 231, 186, 0, 143, 170, 0, 57, 64, 0, 56, 40, 0, 168, 83, 0, 108, 109, 0, 99, 81, 0, 16, 11, 0, 77, 32, 0, 76, 13, 0, 86, 55, 0, 100, 39, 0, 133, 15, 0, 436, 126, 0, 451, 317, 0, 295, 222, 0, 5, 1]
```

## Additions by Year

Number of lines containing `TODO` or `FIXME` added per year. Peaks indicate periods of rapid feature development or technical debt introduction.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#22aa44'}}}}%%
xychart-beta
    title "cassandra_claude — TODO/FIXME Additions by Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 460
    bar [266, 231, 143, 57, 56, 168, 108, 99, 16, 77, 76, 86, 100, 133, 436, 451, 295, 5]
```

## Removals by Year

Number of lines containing `TODO` or `FIXME` removed per year. Peaks reflect cleanup efforts or resolution of previously noted issues.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#dd4444'}}}}%%
xychart-beta
    title "cassandra_claude — TODO/FIXME Removals by Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 320
    bar [215, 186, 170, 64, 40, 83, 109, 81, 11, 32, 13, 55, 39, 15, 126, 317, 222, 1]
```

## Cumulative Debt over Time

Running total of unresolved `TODO`/`FIXME` items (additions − removals, accumulated year by year). A rising line means debt is growing; a falling line means active cleanup is outpacing new additions.

```mermaid
xychart-beta
    title "cassandra_claude — Cumulative TODO/FIXME Debt over Time"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Accumulated Lines" 0 --> 1100
    line [51, 96, 69, 62, 78, 163, 162, 180, 185, 230, 293, 324, 385, 503, 813, 947, 1020, 1024]
```

## Net Change per Year

Difference between additions and removals for each year (additions − removals). Positive values mean more TODOs were introduced than resolved; negative values indicate a net cleanup year.

```mermaid
xychart-beta
    title "cassandra_claude — TODO/FIXME Net Change per Year"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Net Lines" -27 --> 310
    line [51, 45, -27, -7, 16, 85, -1, 18, 5, 45, 63, 31, 61, 118, 310, 134, 73, 4]
```

## Cleanup Ratio per Year

Percentage of new `TODO`/`FIXME` lines that were also removed in the same year (removals / additions × 100). 100% = breakeven; above 100% = paying down old debt; well below 100% = accumulating backlog.

```mermaid
xychart-beta
    title "cassandra_claude — TODO/FIXME Cleanup Ratio per Year (Removals / Additions %)"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "%" 0 --> 120
    line [81, 81, 119, 112, 71, 49, 101, 82, 69, 42, 17, 64, 39, 11, 29, 70, 75, 20]
```

## Cumulative Additions vs Removals

Total ever-added (green) and total ever-removed (red) `TODO`/`FIXME` lines over the project lifetime. The widening gap between the two lines represents the current unresolved backlog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#22aa44,#dd4444'}}}}%%
xychart-beta
    title "cassandra_claude — Cumulative TODO/FIXME Additions vs Removals"
    x-axis [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Lines" 0 --> 2900
    line [266, 497, 640, 697, 753, 921, 1029, 1128, 1144, 1221, 1297, 1383, 1483, 1616, 2052, 2503, 2798, 2803]
    line [215, 401, 571, 635, 675, 758, 867, 948, 959, 991, 1004, 1059, 1098, 1113, 1239, 1556, 1778, 1779]
```


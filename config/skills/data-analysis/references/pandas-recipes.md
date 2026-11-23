# pandas recipes for everyday business data

## Loading

```python
import pandas as pd

# CSV from German Excel / POS systems: semicolon, decimal comma, often cp1252
df = pd.read_csv("in.csv", sep=";", decimal=",", thousands=".", encoding="utf-8-sig")
# UnicodeDecodeError → encoding="cp1252". Unsure of the separator →
# sep=None, engine="python" sniffs it.

# Excel: every sheet, or one sheet with the header on row 3
sheets = pd.read_excel("in.xlsx", sheet_name=None)      # dict of DataFrames
df = pd.read_excel("in.xlsx", sheet_name="Data", header=2)

# SQLite / a database export
import sqlite3
df = pd.read_sql("SELECT * FROM sales", sqlite3.connect("shop.db"))
```

## First look

```python
print(df.shape); print(df.dtypes); print(df.head(10).to_string())
print(df.isna().sum()); print(df.describe(include="all").T.to_string())
print(df.duplicated().sum(), "duplicate rows")
```

## Dates

```python
df["date"] = pd.to_datetime(df["date"], dayfirst=True)        # 11.11.2026
df["date"] = pd.to_datetime(df["serial"], unit="D", origin="1899-12-30")  # Excel serials
df["month"] = df["date"].dt.to_period("M")
df["weekday"] = df["date"].dt.dayofweek                        # Monday = 0
weekdays = df[df["weekday"] < 5]
lunch = df[df["time"].between("11:30", "15:00")]               # "HH:MM" strings
```

## Aggregating

```python
monthly = df.groupby("month").agg(covers=("covers", "sum"), revenue=("revenue", "sum"))
monthly["per_cover"] = monthly["revenue"] / monthly["covers"]

per_day = df.groupby(["month", "date"])["covers"].sum().groupby("month").mean()

table = df.pivot_table(index="month", columns="service", values="covers",
                       aggfunc="sum", margins=True, margins_name="Total")

monthly["yoy"] = monthly["covers"].pct_change(12)               # same month last year
monthly["rolling3"] = monthly["covers"].rolling(3).mean()
```

## Comparing two periods

```python
before = per_day.loc["2025-09":"2026-05"]
after = per_day.loc["2026-06":"2026-08"]
change = after.mean() / before.mean() - 1
from scipy import stats
stats.mannwhitneyu(after, before)
```

## Flagging what stands out

```python
q1, q3 = s.quantile([0.25, 0.75]); iqr = q3 - q1
outliers = s[(s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)]
big_moves = monthly[monthly["yoy"].abs() > 0.15]
```

## Output

```python
monthly.round(2).to_csv("output/monthly.csv")
print(monthly.to_markdown())                                    # tabulate is installed
# German formatting for a text answer
f"{1234.5:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")   # 1.234,5
```

## pandas 2.3 notes

- Assign with `.loc[row_mask, "col"] = value`, never chained
  `df[mask]["col"] = value` (it silently does nothing).
- `groupby(..., observed=True)` on categorical columns avoids empty groups.
- `df.query("revenue > 0 and service == 'lunch'")` keeps filters readable.

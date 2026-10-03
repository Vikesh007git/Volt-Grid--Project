# Databricks notebook source
# MAGIC %md
# MAGIC # EV rollout — synthetic source files
# MAGIC
# MAGIC Generates the four source files for the learning pipeline, plus two files that are
# MAGIC deliberately malformed for the Day 6 ingestion drills.
# MAGIC
# MAGIC Run top to bottom once. Re-running is safe and reproducible (fixed seed).

# COMMAND ----------

# DBTITLE 1,Target location
CATALOG = "workspace"   # in Free Edition this is usually the default catalog
SCHEMA  = "ev_cl"
VOLUME  = "landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("Writing to:", LANDING)

# COMMAND ----------

# DBTITLE 1,Setup
import random
from datetime import datetime, timedelta

import pandas as pd

random.seed(42)

# Reference date is the Monday of the current week — the same rule the real
# pipeline uses in Ingestion.__init__ to derive default_reference_date.
_today = datetime.now()
REF_DATE = (_today - timedelta(days=_today.weekday())).strftime("%Y%m%d")
print("Reference date:", REF_DATE)

REGIONS = ["Amsterdam", "Rotterdam", "Utrecht", "Eindhoven", "Den Haag", "Groningen"]
CONTRACTORS = ["Van Dijk Infra", "Noord Energie BV", "Stroomnet Partners", "Kabel & Co"]
STATUSES = ["Planned", "Civil Complete", "Energised", "Commissioned", "On Hold"]

N_SITES = 120

# Site ids that exist in the master file
site_ids = [f"EVC-{i:04d}" for i in range(1, N_SITES + 1)]

# Sites that appear ONLY in schedule/status and never in the master. These are why
# the transform must union all keys before joining — an inner join silently drops them.
ORPHAN_SCHEDULE = [f"EVC-9{i:03d}" for i in range(1, 6)]
ORPHAN_STATUS = [f"EVC-8{i:03d}" for i in range(1, 5)]


def messy_date(d, allow_null=True):
    """Render a date in one of four formats, or as a missing-value marker.

    Mirrors what arrives from clients: one column, several formats, because the
    rows were typed by different people in different tools.
    """
    if allow_null and random.random() < 0.18:
        return random.choice(["", "-", "N/A"])
    fmt = random.choices(
        ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y %H:%M", "%Y/%m/%d"],
        weights=[55, 25, 12, 8],
    )[0]
    return d.strftime(fmt)


def messy_str(s):
    """Add the whitespace and casing noise that survives an Excel round-trip."""
    r = random.random()
    if r < 0.10:
        return f"  {s}"
    if r < 0.18:
        return f"{s}  "
    if r < 0.22:
        return s.lower()
    return s


def messy_int(n):
    """Numbers arrive as strings, with '-' standing in for 'nothing to report'."""
    if random.random() < 0.15:
        return "-"
    return str(n)


BASE = datetime(2026, 1, 5)

# COMMAND ----------

# DBTITLE 1,1. Site master
rows = []
for sid in site_ids:
    region = random.choice(REGIONS)
    rows.append({
        "Site ID": messy_str(sid),
        "Site Name": f"{region} Charging Hub {sid[-3:]}",
        "Region": messy_str(region),
        "Hub ID": f"HUB-{region[:3].upper()}-{random.randint(1, 4):02d}",
        "Contractor": messy_str(random.choice(CONTRACTORS)),
        "Chargers Planned": messy_int(random.choice([2, 4, 6, 8, 12])),
    })

# Three exact duplicates. Day 5 quarantines these instead of dropping them.
for sid in random.sample(site_ids, 3):
    dup = next(r for r in rows if r["Site ID"].strip().upper() == sid)
    rows.append(dict(dup))

site_master = pd.DataFrame(rows)
site_master.to_csv(f"{LANDING}/EVC-SITE-MASTER_{REF_DATE}.csv", sep=";", index=False)
print(f"EVC-SITE-MASTER_{REF_DATE}.csv  ->  {len(site_master)} rows")

# COMMAND ----------

# DBTITLE 1,2. Schedule — several rows per site, nulls scattered
rows = []
for sid in site_ids + ORPHAN_SCHEDULE:
    start = BASE + timedelta(days=random.randint(0, 200))
    # 1-4 rows per site, each carrying only some of the dates. No single row has
    # the full picture, which is what forces first_value(... ) IGNORE NULLS.
    for _ in range(random.randint(1, 4)):
        rows.append({
            "Site ID": messy_str(sid),
            "Plandatum": messy_date(start),
            "Civiel startdatum": messy_date(start + timedelta(days=random.randint(5, 30))),
            "Civiel einddatum": messy_date(start + timedelta(days=random.randint(31, 90))),
            "Bestellnummer": messy_int(random.randint(450000000, 450999999)),
            "Energiedatum": messy_date(start + timedelta(days=random.randint(60, 150))),
        })

schedule = pd.DataFrame(rows)
schedule.to_csv(f"{LANDING}/EVC-SCHEDULE_{REF_DATE}.csv", sep=";", index=False)
print(f"EVC-SCHEDULE_{REF_DATE}.csv  ->  {len(schedule)} rows")

# COMMAND ----------

# DBTITLE 1,3. Status
rows = []
for sid in site_ids + ORPHAN_STATUS:
    status = random.choice(STATUSES)
    # Four different ways of saying "missing", including a non-breaking space that
    # looks identical to an empty cell but is not one.
    if random.random() < 0.12:
        status = random.choice(["N/A", "NA", "", chr(160)])
    rows.append({
        "Site ID": messy_str(sid),
        "Status": status,
        "Laatste update": messy_date(BASE + timedelta(days=random.randint(180, 250)), allow_null=False),
    })

status_df = pd.DataFrame(rows)
status_df.to_csv(f"{LANDING}/EVC-STATUS_{REF_DATE}.csv", sep=";", index=False)
print(f"EVC-STATUS_{REF_DATE}.csv  ->  {len(status_df)} rows")

# COMMAND ----------

# DBTITLE 1,4. Weekly progress — wide format, for Day 7
rows = []
for sid in site_ids:
    planned = random.choice([2, 4, 6, 8, 12])
    installed = 0
    row = {
        "Site ID": sid,
        "Region": random.choice(REGIONS),
        "Contractor": random.choice(CONTRACTORS),
    }
    for w in range(1, 57):
        if installed < planned and random.random() < 0.08:
            installed += 1
        row[f"W{w}"] = "" if random.random() < 0.05 else installed
    rows.append(row)

progress = pd.DataFrame(rows)
progress.to_excel(f"{LANDING}/EVC-PROGRESS_{REF_DATE}.xlsx", index=False, sheet_name="Progress")
print(f"EVC-PROGRESS_{REF_DATE}.xlsx  ->  {len(progress)} rows x {len(progress.columns)} cols")

# COMMAND ----------

# DBTITLE 1,5. Two broken files for the Day 6 drills
# Dashes in the date: passes the filemask check, fails strptime("%Y%m%d").
status_df.head(20).to_csv(
    f"{LANDING}/EVC-STATUS_2026-09-14.csv", sep=";", index=False
)

# Three underscore-separated components instead of two, and a duplicated column
# name — the two failures is_filename_format_valid and has_file_duplicate_columns
# are each meant to catch.
bad = site_master.head(20).copy()
bad["Region "] = bad["Region"]
bad.to_csv(f"{LANDING}/EVC-SITE-MASTER_{REF_DATE}_v2.csv", sep=";", index=False)

print("Wrote 2 deliberately malformed files.")

# COMMAND ----------

# DBTITLE 1,What you just created
print(f"""
Files in {LANDING}:

  EVC-SITE-MASTER_{REF_DATE}.csv     {len(site_master):>5} rows
  EVC-SCHEDULE_{REF_DATE}.csv        {len(schedule):>5} rows
  EVC-STATUS_{REF_DATE}.csv          {len(status_df):>5} rows
  EVC-PROGRESS_{REF_DATE}.xlsx       {len(progress):>5} rows, 56 week columns
  EVC-STATUS_2026-09-14.csv             bad date format
  EVC-SITE-MASTER_{REF_DATE}_v2.csv     3 name components + duplicate column

Landmines to find. Your cleaning is done when every one of these is handled:

  1. Site ID has leading/trailing spaces and mixed case      -> trim + upper before any join
  2. Four date formats inside one column                     -> try each, coalesce
  3. '-' means "no number"                                   -> null, then default
  4. Missing shows up as '', '-', 'N/A', 'NA' and chr(160)   -> all five, not just null
  5. 3 exact duplicate sites in the master                   -> quarantine (Day 5)
  6. 5 sites in schedule, 4 in status, absent from master    -> union of keys (Day 3)
  7. 1-4 schedule rows per site, no row complete             -> first_value IGNORE NULLS (Day 2)
  8. Blank cells scattered through W1..W56                   -> Day 7

Sanity check — these should NOT match:
  site master sites : {N_SITES}
  distinct in schedule/status : {N_SITES + len(ORPHAN_SCHEDULE)} / {N_SITES + len(ORPHAN_STATUS)}
""")

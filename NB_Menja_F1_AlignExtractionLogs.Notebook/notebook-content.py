# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3417eac3-01d7-49dd-905f-ed6179565f84",
# META       "default_lakehouse_name": "LH_Menja_BI_v1_Mews_DEV",
# META       "default_lakehouse_workspace_id": "edeabf05-3395-4b50-9140-7f034cd65e9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "3417eac3-01d7-49dd-905f-ed6179565f84"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # F-1 — Align ExtractionRunLog and ExtractionFileLog
# 
# **Prepared for review. Nothing here has been run.**
# 
# ## What this does in plain words
# 
# Two small bookkeeping tables already exist in the lakehouse:
# 
# - `ExtractionRunLog` — one row per extraction run
# - `ExtractionFileLog` — one row per raw file written
# 
# FINAL decision **D-186** says these tables must carry more columns than they
# currently do. This notebook **adds the missing columns to the existing tables**.
# 
# It does **not**:
# 
# - rebuild the tables
# - rewrite or delete history
# - invent values for old rows
# 
# Old rows simply keep `NULL` in the new columns. That is intentional: inventing a
# value for a run that already happened would be making up provenance, which
# D-186 forbids.
# 
# ## Columns required by FINAL D-186
# 
# `ExtractionRunLog` must carry:
# 
# `RunID, PropertyKey, PMS, SourceType, SourcePropertyCode, Endpoint,
# MewsScopeType, MewsScopeIds, WindowStartUtc, WindowEndUtc, RunStartUtc,
# RunEndUtc, Status, PagesWritten, RecordCount, ErrorMessage`
# 
# `ExtractionFileLog` must carry:
# 
# `FileID, RunID, PropertyKey, PMS, SourceType, SourcePropertyCode, Endpoint,
# MewsScopeType, MewsScopeIds, PageOrChunkIndex, FileName, FilePath, RecordCount,
# WrittenUtc`
# 
# Both lists were read directly from the governance workbook on 2026-07-30, so
# this is confirmed, not inferred.
# 
# **The same five columns are missing from both tables:** `PropertyKey`,
# `SourceType`, `SourcePropertyCode`, `MewsScopeType`, `MewsScopeIds`.
# 
# ## Two governed rules this notebook also checks
# 
# 1. **Parent–child provenance must match.** D-186: every `ExtractionFileLog` row's
#    `PropertyKey`, `MewsScopeType` and `MewsScopeIds` must exactly match its
#    parent `ExtractionRunLog` row via `RunID`. A mismatch is a provenance failure
#    and must not be silently corrected. Section 4 checks this.
# 
# 2. **DEMO and LIVE share these tables.** D-186 states both routes may write here,
#    and `SourceType` is what distinguishes them. `SYNTHETIC` must not write to
#    these tables at all.
# 
# ## Before running
# 
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` **first** (attaching restarts the
#    session).
# 2. Run the cells in order. Section 1 only inspects and reports. Nothing is
#    changed until you run Section 3.


# MARKDOWN ********************

# ## Section 1 — Settings and inspection (read-only)
# 
# This section changes nothing. It prints what the tables look like today and
# what is missing.

# CELL ********************

# F-1 Section 1 - settings and read-only inspection

from pyspark.sql import functions as F

RUN_LOG_TABLE = "ExtractionRunLog"
FILE_LOG_TABLE = "ExtractionFileLog"

# True  = add all five columns required by FINAL D-186 (recommended)
# False = add only SourceType and SourcePropertyCode (your earlier F-1 scope note)
ADD_FULL_D186_SET = True

FULL_ADDITIONS = [
    ("PropertyKey", "STRING"),
    ("SourceType", "STRING"),
    ("SourcePropertyCode", "STRING"),
    ("MewsScopeType", "STRING"),
    ("MewsScopeIds", "STRING"),
]

MINIMAL_ADDITIONS = [
    ("SourceType", "STRING"),
    ("SourcePropertyCode", "STRING"),
]

ADDITIONS = FULL_ADDITIONS if ADD_FULL_D186_SET else MINIMAL_ADDITIONS

# Column order required by FINAL D-186 for the run log (for reporting only).
D186_RUN_LOG_ORDER = [
    "RunID", "PropertyKey", "PMS", "SourceType", "SourcePropertyCode",
    "Endpoint", "MewsScopeType", "MewsScopeIds", "WindowStartUtc",
    "WindowEndUtc", "RunStartUtc", "RunEndUtc", "Status", "PagesWritten",
    "RecordCount", "ErrorMessage",
]


def existing_columns(table_name):
    if not spark.catalog.tableExists(table_name):
        return None
    return [f.name for f in spark.table(table_name).schema.fields]


def report(table_name):
    cols = existing_columns(table_name)
    print("=" * 70)
    print("TABLE:", table_name)
    if cols is None:
        print("  DOES NOT EXIST. Stop and confirm before continuing - F-1 alters")
        print("  existing tables and does not create them.")
        return None
    print("  current columns :", cols)
    print("  row count       :", spark.table(table_name).count())
    missing = [c for c, _ in ADDITIONS if c not in cols]
    print("  to be added     :", missing if missing else "none (already aligned)")
    return cols


run_cols = report(RUN_LOG_TABLE)
file_cols = report(FILE_LOG_TABLE)

print("=" * 70)
if run_cols is not None:
    gap = [c for c in D186_RUN_LOG_ORDER if c not in run_cols]
    print("ExtractionRunLog columns required by FINAL D-186 but absent:", gap)
print("Mode:", "FULL D-186 set" if ADD_FULL_D186_SET else "MINIMAL two-column set")
print("Nothing has been changed by this cell.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 — Check the PMS value already being written
# 
# **This is a finding you need to decide on, not a fix.**
# 
# FINAL decision **D-224** says PMS values use governed **uppercase** codes, and
# the initial allowed value is `MEWS`.
# 
# `NB00` currently writes `PMS_NAME = "Mews"` (mixed case). `B_PropertySourceIdentity`
# will hold `MEWS` (column governance C-417).
# 
# Why this matters: property resolution matches on `PMS + SourceType +
# SourcePropertyCode`. If the logs say `Mews` and the identity table says `MEWS`,
# the match finds **zero rows** and every downstream step stops.
# 
# This cell only reports what is in the table. It does not change any data,
# because changing existing log rows would be rewriting history, which your F-1
# scope excludes.

# CELL ********************

# F-1 Section 2 - report distinct PMS values already stored (read-only)

if spark.catalog.tableExists(RUN_LOG_TABLE):
    print("Distinct PMS values in", RUN_LOG_TABLE, ":")
    spark.table(RUN_LOG_TABLE).groupBy("PMS").count().orderBy("PMS").show(50, False)

if spark.catalog.tableExists(FILE_LOG_TABLE):
    print("Distinct PMS values in", FILE_LOG_TABLE, ":")
    spark.table(FILE_LOG_TABLE).groupBy("PMS").count().orderBy("PMS").show(50, False)

print("D-224 requires uppercase PMS codes; initial governed value is MEWS.")
print("If any value above is not MEWS, that is a governance mismatch to decide on.")
print("This cell changed nothing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3 — Add the missing columns (this changes the tables)
# 
# Plain words: `ALTER TABLE ... ADD COLUMNS` adds empty columns to a Delta table.
# It is cheap, it does not touch existing row data, and it does not rewrite files.
# 
# Safety notes:
# 
# - New columns are **nullable**. Delta cannot add a `NOT NULL` column to a table
#   that already has rows, and inventing a value to satisfy `NOT NULL` is exactly
#   what D-186 forbids.
# - The cell is **idempotent**: a column that already exists is skipped, so
#   re-running is safe.
# - No backfill is performed.

# CELL ********************

# F-1 Section 3 - add missing columns in place (idempotent, no backfill)

def add_missing_columns(table_name, additions):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            table_name + " does not exist. F-1 alters existing tables only. "
            "Do not create it here - confirm the correct build path first."
        )

    current = [f.name for f in spark.table(table_name).schema.fields]
    to_add = [(c, t) for c, t in additions if c not in current]

    if not to_add:
        print(table_name, "- already has all target columns. Nothing to do.")
        return []

    clause = ", ".join(c + " " + t for c, t in to_add)
    sql = "ALTER TABLE " + table_name + " ADD COLUMNS (" + clause + ")"
    print(table_name, "- running:", sql)
    spark.sql(sql)
    print(table_name, "- added:", [c for c, _ in to_add])
    return [c for c, _ in to_add]


added_run = add_missing_columns(RUN_LOG_TABLE, ADDITIONS)
added_file = add_missing_columns(FILE_LOG_TABLE, ADDITIONS)

print("-" * 70)
print("Added to", RUN_LOG_TABLE, ":", added_run)
print("Added to", FILE_LOG_TABLE, ":", added_file)
print("Existing rows keep NULL in the new columns. No backfill was performed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 — Validation
# 
# Run this after Section 3 and keep the output as your evidence.
# 
# Expected:
# 
# - both tables list every target column
# - row counts are **unchanged** from Section 1
# - the new columns are `NULL` for all pre-existing rows

# CELL ********************

# F-1 Section 4 - validation evidence

new_cols = [c for c, _ in ADDITIONS]

for t in (RUN_LOG_TABLE, FILE_LOG_TABLE):
    print("=" * 70)
    print("TABLE:", t)
    cols = [f.name for f in spark.table(t).schema.fields]
    print("  columns   :", cols)
    print("  row count :", spark.table(t).count())

    missing = [c for c in new_cols if c not in cols]
    if missing:
        raise RuntimeError("Expected columns still missing in " + t + ": " + str(missing))

    present = [c for c in new_cols if c in cols]
    if present and spark.table(t).count() > 0:
        agg = [F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(c) for c in present]
        print("  non-null counts in new columns (expect 0 for historical rows):")
        spark.table(t).agg(*agg).show(1, False)

print("=" * 70)

# D-186: file-log provenance must exactly match its parent run-log row via RunID.
# This is vacuous while the columns are still NULL, but it is the governed
# invariant NB06 must satisfy, so it is checked from the start.
prov_cols = [c for c in ["PropertyKey", "MewsScopeType", "MewsScopeIds"] if c in new_cols]

if prov_cols and spark.table(FILE_LOG_TABLE).count() > 0:
    r = spark.table(RUN_LOG_TABLE).select(
        ["RunID"] + [F.col(c).alias("run_" + c) for c in prov_cols])
    f = spark.table(FILE_LOG_TABLE).select(
        ["FileID", "RunID"] + [F.col(c).alias("file_" + c) for c in prov_cols])
    joined = f.join(r, on="RunID", how="inner")

    mismatch = joined
    cond = F.lit(False)
    for c in prov_cols:
        cond = cond | (~F.col("run_" + c).eqNullSafe(F.col("file_" + c)))
    n_mismatch = mismatch.filter(cond).count()

    print("File-log rows whose provenance differs from parent run (expect 0):", n_mismatch)
    if n_mismatch:
        mismatch.filter(cond).show(50, False)
        raise RuntimeError(
            "D-186 provenance mismatch between ExtractionFileLog and its parent "
            "ExtractionRunLog row. This must not be silently corrected."
        )
else:
    print("Parent-child provenance check skipped (no file-log rows yet).")

print("=" * 70)
print("F-1 validation finished. Report the output above before committing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## What is still open after F-1
# 
# - The `PMS = "Mews"` vs `MEWS` mismatch (Section 2) is a **known, accepted**
#   state: existing DEMO-path log rows keep `"Mews"` and are not rewritten. NB00
#   and NB06 must write `MEWS` going forward. A governance note records this.
# - Whichever notebooks write these logs (`NB00`, later `NB06`) must be updated to
#   actually populate the new columns. F-1 only makes room for them.
# 
# **Reminder:** pause Fabric capacity `fabaurorabiv1devf2` in Azure when you are
# done working.

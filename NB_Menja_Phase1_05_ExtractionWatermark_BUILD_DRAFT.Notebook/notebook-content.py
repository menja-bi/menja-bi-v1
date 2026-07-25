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

# # NB_Menja_Phase1_05_ExtractionWatermark_BUILD_DRAFT
# 
# **Purpose:** create and manage the per-property extraction watermark store.
# This is the first implementation object for recurring Mews extraction under **D-213 (FINAL)**.
# 
# In plain words: this notebook makes one small Delta table, `ExtractionWatermark`,
# that remembers, for each property, "the source has been captured through this
# time" so the next run can pull only what changed since then.
# 
# **What this object does:**
# - Creates `ExtractionWatermark` with an explicit schema if it is missing
#   (same create-if-missing pattern as the D-186 log tables in NB00).
# - Provides read helpers the extractor will call to decide its pull start.
# - Provides an advance helper the extractor will call *only after a fully
#   successful, fully written run*, which moves the watermark forward.
# 
# **What this object does NOT do (kept out on purpose):**
# - It does NOT pull from Mews, loop over properties, or rebuild NB00.
#   Wiring these helpers into the extractor is a separate build step.
# - It invents no mapping, join, fallback, or business rule.
# 
# **Governing FINAL decisions:**
# 
# | ID | What it governs here |
# |---|---|
# | D-213 | Recurring extraction cadence: per-property watermark on `UpdatedUtc`, advance only on success, pull from watermark minus a 15-minute overlap margin, idempotent writes, first run = full pull |
# | D-189 | `SnapshotDateTime` / captured-through time = Mews `UpdatedUtc` (source-change time) |
# | D-195 / D-205 | Property identity resolved via reservation -> service -> enterprise -> `D_Property.PMS_PropertyCode` -> `PropertyKey` / `PropertyID` |
# | D-203 | `D_Property` identity set carried here: `PropertyKey`, `PropertyID`, `PMS_PropertyCode` |
# | D-186 | Sibling audit trail (`ExtractionRunLog`); `LastSuccessfulRunID` is a soft link to the run that advanced the watermark |
# 
# **Confirmed key design:** this store keys on `PropertyKey` (Menja's canonical
# per-property watermark identity). `PropertyID` and `PMS_PropertyCode` are non-key
# traceability / source-resolution columns. `PMS_PropertyCode` is the Mews pull-side
# identifier used to resolve the source enterprise to the governed `D_Property` row;
# it must never replace `PropertyKey` as the watermark identity.
# 
# **Before running:**
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` to this notebook FIRST
#    (attaching restarts the session, so attach before running anything).
# 2. Then Run all, or run cells top to bottom. This notebook writes only the
#    empty table structure; it does not write any property rows by itself.


# MARKDOWN ********************

# ## Section 1 - Imports, constants, and the watermark schema
# 
# Plain words:
# - `ExtractionWatermark` holds one row per property.
# - Columns use the same PascalCase / UTC-suffix style as the D-186 log tables.
# - `CapturedThroughUtc` is the actual watermark: the highest Mews `UpdatedUtc`
#   that a fully successful run has captured through (D-189 basis, D-213 rule).
# - The 15-minute overlap margin is a code constant (D-213), not a stored column,
#   so no per-property override is implied where none is governed.

# CELL ********************

# Section 1 - Imports, constants, watermark schema

from datetime import datetime, timezone, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from delta.tables import DeltaTable

# Fixed identity of this store
PMS = "Mews"                              # multi-PMS discipline; matches D-186 log tables
WATERMARK_TABLE = "ExtractionWatermark"   # per-property watermark store (D-213, new)

# D-213: each run pulls from the watermark minus a 15-minute overlap margin.
OVERLAP_MARGIN = timedelta(minutes=15)


def utc_now():
    return datetime.now(timezone.utc)


# One row per property.
# CapturedThroughUtc is nullable: a property with no successful capture yet
# has no watermark, which D-213 treats as "first run = full pull".
watermark_schema = StructType([
    StructField("PMS", StringType(), False),                       # "Mews"
    StructField("PropertyKey", StringType(), False),              # governed surrogate (D-203)
    StructField("PropertyID", StringType(), False),               # governed natural id (D-195)
    StructField("PMS_PropertyCode", StringType(), False),         # Mews EnterpriseId (D-205)
    StructField("CapturedThroughUtc", TimestampType(), True),     # THE watermark (UpdatedUtc, D-189/D-213)
    StructField("LastSuccessfulRunID", StringType(), True),       # soft link to ExtractionRunLog (D-186)
    StructField("LastSuccessfulRunEndUtc", TimestampType(), True),# operational finish time (not the AsOf)
    StructField("WatermarkUpdatedUtc", TimestampType(), False),   # when this store row was last written
])

print("Watermark constants and schema ready.")
print("Table:", WATERMARK_TABLE, "| PMS:", PMS, "| overlap margin:", OVERLAP_MARGIN)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 - Ensure the store exists
# 
# Plain words:
# - If `ExtractionWatermark` is missing, create it empty with the explicit schema.
# - Explicit schema avoids Spark failing to infer types from an empty/NULL table.
# - This is the same safe create-if-missing approach NB00 uses for its log tables.
# - Running this cell writes no property rows; it only guarantees the structure.

# CELL ********************

# Section 2 - Ensure the watermark table exists

def ensure_watermark_table_exists():
    # Creates the per-property watermark store with an explicit schema if missing.
    if spark.catalog.tableExists(WATERMARK_TABLE):
        print("Watermark table exists:", WATERMARK_TABLE)
    else:
        empty_df = spark.createDataFrame([], schema=watermark_schema)
        empty_df.write.format("delta").saveAsTable(WATERMARK_TABLE)
        print("Watermark table created:", WATERMARK_TABLE)


ensure_watermark_table_exists()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3 - Read helpers (used by the extractor to decide its pull start)
# 
# Plain words:
# - `get_watermark(property_key)` returns the captured-through time for a property,
#   or `None` if the property has never been successfully captured.
# - `pull_start_for(property_key)` returns the value the extractor should use as
#   its incremental `UpdatedUtc` floor: the watermark minus the 15-minute overlap.
#   It returns `None` when there is no watermark, which D-213 means as "full pull".
# - These helpers only read. They never advance the watermark.

# CELL ********************

# Section 3 - Read helpers

def get_watermark(property_key):
    # Current captured-through UTC for a property, or None if no successful
    # capture exists yet (D-213: first run => full pull).
    ensure_watermark_table_exists()
    row = (
        spark.table(WATERMARK_TABLE)
        .where(F.col("PropertyKey") == property_key)
        .select("CapturedThroughUtc")
        .head()
    )
    if row is None:
        return None
    return row["CapturedThroughUtc"]  # may itself be None


def pull_start_for(property_key):
    # The incremental UpdatedUtc floor for the next run.
    # None => no watermark yet => extractor must do a FULL pull (D-213).
    wm = get_watermark(property_key)
    if wm is None:
        return None
    return wm - OVERLAP_MARGIN


print("Read helpers ready: get_watermark(), pull_start_for().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 - Advance helper (called ONLY after a fully successful run)
# 
# Plain words:
# - `advance_watermark(...)` moves a property's watermark forward after a run that
#   fully succeeded and was fully written (D-213). The extractor decides success
#   using the D-186 `ExtractionRunLog` `Status` = `Success`; this helper does not.
# - It uses a Delta MERGE (an "upsert"): update the property's row if it exists,
#   insert it if it does not.
# - Forward-only guard: the watermark is only moved when the new captured-through
#   time is later than the stored one (or the stored one is empty). Re-running the
#   same successful window therefore changes nothing (idempotent, D-213).

# CELL ********************

# Section 4 - Advance helper (success-only, forward-only, idempotent)

def advance_watermark(property_key,
                      property_id,
                      pms_property_code,
                      new_captured_through_utc,
                      run_id=None,
                      run_end_utc=None):
    # Call this ONLY after a fully successful, fully written run (D-213).
    ensure_watermark_table_exists()

    stage = spark.createDataFrame(
        [(
            PMS,
            property_key,
            property_id,
            pms_property_code,
            new_captured_through_utc,
            run_id,
            run_end_utc,
            utc_now(),
        )],
        schema=watermark_schema,
    )

    target = DeltaTable.forName(spark, WATERMARK_TABLE)
    (
        target.alias("t")
        .merge(stage.alias("s"), "t.PropertyKey = s.PropertyKey")
        .whenMatchedUpdate(
            # Forward-only: never move the watermark backwards.
            condition="t.CapturedThroughUtc IS NULL OR s.CapturedThroughUtc > t.CapturedThroughUtc",
            set={
                "PropertyID": "s.PropertyID",
                "PMS_PropertyCode": "s.PMS_PropertyCode",
                "CapturedThroughUtc": "s.CapturedThroughUtc",
                "LastSuccessfulRunID": "s.LastSuccessfulRunID",
                "LastSuccessfulRunEndUtc": "s.LastSuccessfulRunEndUtc",
                "WatermarkUpdatedUtc": "s.WatermarkUpdatedUtc",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("advance_watermark applied for PropertyKey:", property_key)


print("Advance helper ready: advance_watermark().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5 - Read-only smoke check (writes nothing)
# 
# Plain words:
# - Confirms the table exists and shows how many property rows it holds.
# - Shows the first-run behaviour for an unknown property: both helpers return
#   `None`, which the extractor reads as "do a full pull" (D-213).
# - This cell does not write any property rows.

# CELL ********************

# Section 5 - Read-only smoke check

ensure_watermark_table_exists()

row_count = spark.table(WATERMARK_TABLE).count()
print("ExtractionWatermark rows:", row_count)

sample_key = "__no_such_property__"
print("First-run check for an unknown property key:")
print("  get_watermark(sample)  ->", get_watermark(sample_key))
print("  pull_start_for(sample) ->", pull_start_for(sample_key),
      "  (None = full pull, per D-213)")

print("")
print("Current ExtractionWatermark contents:")
spark.table(WATERMARK_TABLE).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6 - Where the extractor will use this (not implemented here)
# 
# This is guidance only. Building the per-property pull loop is a separate step
# and is out of scope for this notebook under D-213.
# 
# Sketch of how the extractor will call these helpers, per property:
# 
# ```text
# start = pull_start_for(property_key)
# if start is None:
#     # first run: FULL pull; set watermark to the highest UpdatedUtc seen
# else:
#     # incremental pull: UpdatedUtc >= start   (start already includes the 15-min overlap)
# 
# ... perform the pull, de-duplicate by version key, write idempotently ...
# 
# if run fully succeeded and was fully written:   # D-186 ExtractionRunLog Status = "Success"
#     advance_watermark(
#         property_key, property_id, pms_property_code,
#         new_captured_through_utc = highest UpdatedUtc actually captured,
#         run_id = <that run's RunID>,
#         run_end_utc = <that run's RunEndUtc>,
#     )
# # on Failed / Partial: do NOT advance (D-213); the gap is logged, never filled
# ```
# 
# **Source cardinality rule (do not invent):** BI v1 uses ONLY the currently
# governed and validated property-resolution mapping (one governed source per
# PropertyKey). Do not assume this holds indefinitely. If implementation later
# requires multiple independent source watermarks for a single PropertyKey:
# STOP and raise a governance gap. Do not change the key or split the grain silently.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working,
# to avoid unnecessary cost.**


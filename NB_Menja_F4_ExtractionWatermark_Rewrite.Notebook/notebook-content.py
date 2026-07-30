# Fabric notebook source


# MARKDOWN ********************

# # F-4 — `ExtractionWatermark` (full rewrite)
# 
# **Prepared for review. Nothing here has been run.**
# 
# This replaces `NB_Menja_Phase1_05_ExtractionWatermark_BUILD_DRAFT`, which is
# stale. It is a rewrite, not a patch.
# 
# ## What this does in plain words
# 
# The watermark is one small note per live feed that says:
# 
# > "We have already captured everything that changed up to this moment."
# 
# The next run reads that note, steps back 15 minutes for safety, and asks Mews
# only for what changed since then.
# 
# ## Why the old draft had to go
# 
# | Old draft | FINAL governance |
# |---|---|
# | keyed on `PropertyKey` only | keyed on `PropertyKey + PMS + SourceType + SourcePropertyCode` (D-213, D-228) |
# | carried `PropertyID`, `PMS_PropertyCode` | not part of the watermark |
# | carried `LastSuccessfulRunID`, `LastSuccessfulRunEndUtc`, `WatermarkUpdatedUtc` | explicitly rejected in Package E-1 |
# | `PMS = "Mews"` | D-224 requires uppercase `MEWS` |
# 
# ## The governed shape (FINAL D-213)
# 
# `PropertyKey`, `PMS`, `SourceType`, `SourcePropertyCode`, `CapturedThroughUtc`.
# 
# Five columns. Nothing else.
# 
# ## The governed rules this notebook enforces
# 
# - **LIVE only.** `SourceType` must be `LIVE`. DEMO and SYNTHETIC never get a
#   watermark row (D-221, D-228).
# - **One row per exact eligible identity**, and that identity must have exactly one
#   enabled `PropertyExtractionConfig` row and exactly one
#   `B_PropertySourceIdentity` row.
# - **No row until cold start succeeds.** The first watermark is only created after
#   a complete, fully written, fully logged cold-start interval (D-214).
# - **Advance only on complete success.** Failed or Partial runs change nothing
#   (D-213, D-216).
# - **A successful zero-row interval still advances** — to the fixed `WindowEndUtc`
#   (D-216).
# - **Forward-only and idempotent.** A watermark never moves backwards, and
#   re-applying the same value changes nothing (D-213).
# - **15-minute overlap** on read (D-213).
# 
# ## What this notebook does NOT do
# 
# It does not call Mews, loop over properties, or run an extraction. It creates the
# table and provides the helper functions that NB06 will call. Wiring is a separate
# step.
# 
# ## Before running
# 
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` **first**.
# 2. Run **F-1, F-2 and F-3 first**.
# 3. Run cells in order.

# MARKDOWN ********************

# ## Section 1 — Settings and governed schema

# CELL ********************

# F-4 Section 1 - settings, constants, governed schema

from datetime import datetime, timezone, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

WATERMARK_TABLE = "ExtractionWatermark"
CONFIG_TABLE = "PropertyExtractionConfig"       # F-3
IDENTITY_TABLE = "B_PropertySourceIdentity"     # F-2

# D-213: each recurring run pulls from the watermark minus a 15-minute overlap.
OVERLAP_MARGIN = timedelta(minutes=15)

# D-213 / D-228: the watermark exists only for LIVE identities.
REQUIRED_SOURCE_TYPE = "LIVE"

IDENTITY_COLS = ["PropertyKey", "PMS", "SourceType", "SourcePropertyCode"]
GOVERNED_COLS = IDENTITY_COLS + ["CapturedThroughUtc"]

# Set to True ONLY if an existing ExtractionWatermark table has the stale schema
# and you have confirmed no live extraction state would be lost.
REPLACE_STALE_TABLE = False

watermark_schema = StructType([
    StructField("PropertyKey", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("SourceType", StringType(), False),
    StructField("SourcePropertyCode", StringType(), False),
    StructField("CapturedThroughUtc", TimestampType(), False),
])


def utc_now():
    return datetime.now(timezone.utc)


print("Watermark table :", WATERMARK_TABLE)
print("Governed columns:", GOVERNED_COLS)
print("Overlap margin  :", OVERLAP_MARGIN, "(D-213)")

# MARKDOWN ********************

# ## Section 2 — Inspect any existing table before touching it
# 
# If a wrong-schema `ExtractionWatermark` already exists (the stale draft may have
# been run), this stops and tells you. It does **not** silently drop it.
# 
# Dropping is only safe if no real live extraction state would be lost. Live
# extraction has never been enabled, so in practice the old table holds no real
# watermark — but you confirm that, not this notebook.

# CELL ********************

# F-4 Section 2 - inspect existing table (read-only unless you opt in)

def describe_existing():
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        print(WATERMARK_TABLE, "does not exist. It will be created in Section 3.")
        return None
    cols = [f.name for f in spark.table(WATERMARK_TABLE).schema.fields]
    n = spark.table(WATERMARK_TABLE).count()
    print("Existing", WATERMARK_TABLE, "found.")
    print("  columns   :", cols)
    print("  row count :", n)
    if n:
        spark.table(WATERMARK_TABLE).show(50, False)
    return cols


existing_cols = describe_existing()

if existing_cols is not None and existing_cols != GOVERNED_COLS:
    print("-" * 70)
    print("SCHEMA MISMATCH against FINAL D-213.")
    print("  expected:", GOVERNED_COLS)
    print("  found   :", existing_cols)
    stale_extras = [c for c in existing_cols if c not in GOVERNED_COLS]
    print("  columns not governed:", stale_extras)
    if not REPLACE_STALE_TABLE:
        raise RuntimeError(
            "Existing ExtractionWatermark does not match FINAL D-213 and "
            "REPLACE_STALE_TABLE is False, so nothing was changed. Review the rows "
            "printed above. If no real live watermark state would be lost, set "
            "REPLACE_STALE_TABLE = True and re-run."
        )
    print("REPLACE_STALE_TABLE is True - Section 3 will drop and recreate the table.")

# MARKDOWN ********************

# ## Section 3 — Create the governed table

# CELL ********************

# F-4 Section 3 - create (or deliberately recreate) the governed table

if spark.catalog.tableExists(WATERMARK_TABLE):
    cols = [f.name for f in spark.table(WATERMARK_TABLE).schema.fields]
    if cols == GOVERNED_COLS:
        print("Table already matches FINAL D-213. Nothing to do.")
    elif REPLACE_STALE_TABLE:
        print("Dropping stale table", WATERMARK_TABLE)
        spark.sql("DROP TABLE " + WATERMARK_TABLE)
        (spark.createDataFrame([], schema=watermark_schema)
         .write.format("delta").saveAsTable(WATERMARK_TABLE))
        print("Recreated", WATERMARK_TABLE, "with the governed five-column schema.")
    else:
        raise RuntimeError("Stale table present and REPLACE_STALE_TABLE is False.")
else:
    (spark.createDataFrame([], schema=watermark_schema)
     .write.format("delta").saveAsTable(WATERMARK_TABLE))
    print("Created", WATERMARK_TABLE, "with the governed five-column schema.")

print("Final columns:", [f.name for f in spark.table(WATERMARK_TABLE).schema.fields])

# MARKDOWN ********************

# ## Section 4 — Eligibility check
# 
# Before any watermark is read or written, the identity must be proven eligible:
# 
# 1. `SourceType` is `LIVE`
# 2. exactly one **enabled** `PropertyExtractionConfig` row matches
# 3. exactly one `B_PropertySourceIdentity` row matches
# 
# Anything else stops. No fallback, no "closest match", no defaulting.

# CELL ********************

# F-4 Section 4 - eligibility resolution

def require_live_identity(identity):
    """Validate one identity dict. Returns the enabled config row or raises."""
    missing = [c for c in IDENTITY_COLS if not identity.get(c)]
    if missing:
        raise RuntimeError("Identity is incomplete; missing: " + str(missing))

    if identity["SourceType"] != REQUIRED_SOURCE_TYPE:
        raise RuntimeError(
            "ExtractionWatermark is LIVE-only (D-213, D-228). Refusing SourceType='"
            + str(identity["SourceType"]) + "'."
        )

    for table in (IDENTITY_TABLE, CONFIG_TABLE):
        if not spark.catalog.tableExists(table):
            raise RuntimeError(table + " does not exist. Run F-2 and F-3 first.")

    def _filter(df):
        for c in IDENTITY_COLS:
            df = df.filter(F.col(c) == F.lit(identity[c]))
        return df

    n_ident = _filter(spark.table(IDENTITY_TABLE)).count()
    if n_ident != 1:
        raise RuntimeError(
            "Expected exactly one " + IDENTITY_TABLE + " row for this identity, found "
            + str(n_ident) + ". Missing or ambiguous identity must not be guessed."
        )

    cfg = _filter(spark.table(CONFIG_TABLE))
    n_cfg = cfg.count()
    if n_cfg != 1:
        raise RuntimeError(
            "Expected exactly one " + CONFIG_TABLE + " row for this identity, found "
            + str(n_cfg) + "."
        )

    row = cfg.collect()[0]
    if not row["IsLiveExtractionEnabled"]:
        raise RuntimeError(
            "Live extraction is disabled for this identity. "
            "A disabled configuration must not create or advance a watermark (D-219)."
        )
    return row


def _identity_predicate(identity):
    cond = F.lit(True)
    for c in IDENTITY_COLS:
        cond = cond & (F.col(c) == F.lit(identity[c]))
    return cond


print("Eligibility helpers ready.")

# MARKDOWN ********************

# ## Section 5 — Read helpers
# 
# - `get_watermark(identity)` → the stored captured-through time, or `None` if the
#   feed has never had a successful cold start.
# - `pull_start_for(identity)` → what the next run should use as its lower boundary:
#   watermark minus 15 minutes. `None` means "no watermark yet, so use
#   `ColdStartUpdatedUtcFrom` from config" (D-214).
# 
# These only read. They never write.

# CELL ********************

# F-4 Section 5 - read helpers

def get_watermark(identity):
    require_live_identity(identity)
    rows = (spark.table(WATERMARK_TABLE)
            .filter(_identity_predicate(identity))
            .select("CapturedThroughUtc")
            .collect())
    if len(rows) > 1:
        raise RuntimeError(
            "More than one watermark row for one identity. The governed rule is one "
            "row per eligible LIVE PropertySourceIdentity."
        )
    return rows[0]["CapturedThroughUtc"] if rows else None


def pull_start_for(identity):
    """Lower boundary for the next recurring run.

    None means no watermark exists yet, so the caller must use the governed
    ColdStartUpdatedUtcFrom from PropertyExtractionConfig (D-214).
    """
    wm = get_watermark(identity)
    if wm is None:
        return None
    if wm.tzinfo is None:
        wm = wm.replace(tzinfo=timezone.utc)
    return wm - OVERLAP_MARGIN


def cold_start_from(identity):
    """The governed cold-start lower boundary from config (D-214)."""
    row = require_live_identity(identity)
    return row["ColdStartUpdatedUtcFrom"]


print("Read helpers ready.")

# MARKDOWN ********************

# ## Section 6 — Write helpers
# 
# Two calls, both meant to be used by NB06 **only after** a run is fully complete.
# 
# - `initialize_watermark_after_cold_start(...)` — creates the very first row. Only
#   valid once the whole cold-start interval succeeded and was fully written and
#   logged (D-214).
# - `advance_watermark(...)` — moves an existing watermark forward. Refuses to move
#   backwards. Re-applying the same value is a no-op.
# 
# Both require `run_status == "Success"`. `Failed` and `Partial` are refused
# outright (D-213, D-216).

# CELL ********************

# F-4 Section 6 - write helpers

def _require_success(run_status):
    if run_status != "Success":
        raise RuntimeError(
            "Watermark changes require a fully successful run. Got run_status='"
            + str(run_status) + "'. Failed and Partial runs must not create or "
            "advance a watermark (D-213, D-216)."
        )


def _as_utc(ts):
    if ts is None:
        raise RuntimeError("A watermark value is required; None was supplied.")
    if getattr(ts, "tzinfo", None) is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def initialize_watermark_after_cold_start(identity, captured_through_utc, run_status):
    """Create the first watermark row after a complete, successful cold start."""
    require_live_identity(identity)
    _require_success(run_status)
    value = _as_utc(captured_through_utc)

    if get_watermark(identity) is not None:
        raise RuntimeError(
            "A watermark already exists for this identity. Use advance_watermark "
            "instead of re-initialising."
        )

    row = {c: identity[c] for c in IDENTITY_COLS}
    row["CapturedThroughUtc"] = value.replace(tzinfo=None)

    (spark.createDataFrame([row], schema=watermark_schema)
     .write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE))
    print("Initial watermark created:", identity, "->", value)
    return value


def advance_watermark(identity, captured_through_utc, run_status):
    """Move an existing watermark forward. Forward-only and idempotent."""
    require_live_identity(identity)
    _require_success(run_status)
    new_value = _as_utc(captured_through_utc)

    current = get_watermark(identity)
    if current is None:
        raise RuntimeError(
            "No watermark exists yet for this identity. The first row may only be "
            "created by initialize_watermark_after_cold_start (D-214)."
        )

    current_utc = _as_utc(current)
    if new_value < current_utc:
        raise RuntimeError(
            "Refusing to move the watermark backwards. Current " + str(current_utc)
            + ", requested " + str(new_value) + ". Advancement is forward-only (D-213)."
        )
    if new_value == current_utc:
        print("Watermark already at", current_utc, "- no change (idempotent).")
        return current_utc

    dt = DeltaTable.forName(spark, WATERMARK_TABLE)
    cond = " AND ".join("t." + c + " = '" + str(identity[c]).replace("'", "''") + "'"
                        for c in IDENTITY_COLS)
    dt.alias("t").update(
        condition=cond,
        set={"CapturedThroughUtc": F.lit(new_value.replace(tzinfo=None)).cast("timestamp")},
    )
    print("Watermark advanced:", identity, str(current_utc), "->", str(new_value))
    return new_value


print("Write helpers ready. Nothing has been written by this cell.")

# MARKDOWN ********************

# ## Section 7 — Validation
# 
# This section only checks structure and invariants. It writes nothing.
# 
# Expected:
# 
# - exactly the five governed columns
# - no `SourceType` other than `LIVE`
# - at most one row per identity
# - every watermark row has a matching enabled config row and identity row
# 
# On a fresh build the table is empty, which is correct — no watermark exists until
# a cold start succeeds.

# CELL ********************

# F-4 Section 7 - validation evidence

t = spark.table(WATERMARK_TABLE)
cols = [f.name for f in t.schema.fields]
n = t.count()

print("Columns   :", cols)
print("Row count :", n, "(0 is correct before any successful cold start)")

if cols != GOVERNED_COLS:
    raise RuntimeError("Expected " + str(GOVERNED_COLS) + " but found " + str(cols))

bad_st = t.filter(F.col("SourceType") != F.lit(REQUIRED_SOURCE_TYPE)).count()
print("Rows with SourceType != LIVE (expect 0):", bad_st)
if bad_st:
    raise RuntimeError("ExtractionWatermark is LIVE-only (D-213, D-228).")

n_dup = t.groupBy(*IDENTITY_COLS).count().filter(F.col("count") > 1).count()
print("Identities with more than one row (expect 0):", n_dup)
if n_dup:
    raise RuntimeError("One row per eligible LIVE identity is required.")

if n and spark.catalog.tableExists(CONFIG_TABLE):
    orphans = (t.join(spark.table(CONFIG_TABLE).select(*IDENTITY_COLS),
                      on=IDENTITY_COLS, how="left_anti").count())
    print("Watermark rows with no matching config row (expect 0):", orphans)
    if orphans:
        raise RuntimeError("Every watermark row must match one config row (D-215).")

if n:
    t.show(200, False)

print("F-4 validation finished.")

# MARKDOWN ********************

# ## What is still open after F-4
# 
# - These helpers are not wired to anything yet. NB06 is the caller, and NB06 is
#   **not** prepared — see the blockers in the session summary.
# - The 15-minute overlap is taken from FINAL D-213. If you ever change it, change
#   it in governance first.
# 
# **Reminder:** pause Fabric capacity `fabaurorabiv1devf2` in Azure when you are
# done working.

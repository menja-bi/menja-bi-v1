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

# # NB_Menja_ExtractionControl_Setup
# 
# Sets up four small control tables. Rerunnable. Attach `LH_Menja_BI_v1_Mews_DEV`
# first, then run top to bottom.
# 
# At ~50 hotels these tables hold well under 200 rows in total. **Cell 5 prints all
# of them — reading that output is the check.** There is no separate validation
# framework, because at this size you can just look.
# 
# Three things are guarded in code, because each one fails *silently*:
# 
# 1. **Identity load is append-only.** Editing a used row would silently re-point
#    reservations that already resolved through it.
# 2. **No watermark value is ever written here.** A made-up `CapturedThroughUtc`
#    makes the first real extraction skip everything before it, with no error.
# 3. **An enabled config row must resolve to exactly one identity.** Otherwise
#    extraction runs against nothing and reports success.
# 
# Everything else fails loudly or is visible in the printout.

# CELL ********************

# Shared setup

import pandas as pd

SEED = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"
ID   = ["PropertyKey", "PMS", "SourceType", "SourcePropertyCode"]


def sheet(name, cols):
    """Read one seed sheet by exact name. Stop if a governed column is absent -
    nothing here is renamed, inferred or substituted."""
    df = pd.read_excel(SEED, sheet_name=name, dtype=str).dropna(how="all")
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(
            name + ": missing governed columns " + str(missing) +
            ". Found: " + str(list(df.columns)) + ". Fix the seed sheet. "
            "SourceType is never derived from IsLiveExtractionEnabled, and "
            "SourcePropertyCode is never taken from MewsScopeIds."
        )
    df = df[cols].copy()
    for c in ID:
        if c in df.columns:
            df[c] = df[c].str.strip()
    return df


print("Seed:", SEED)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 — Extraction logs: add the five columns D-186 needs

# CELL ********************

# ALTER TABLE ADD COLUMNS is cheap, leaves existing rows untouched, and skips
# columns already present. Old rows keep NULL - no backfill, no history rewrite.

NEW = ["PropertyKey", "SourceType", "SourcePropertyCode", "MewsScopeType", "MewsScopeIds"]

for t in ["ExtractionRunLog", "ExtractionFileLog"]:
    have = [f.name for f in spark.table(t).schema.fields]
    add = [c for c in NEW if c not in have]
    if add:
        spark.sql("ALTER TABLE " + t + " ADD COLUMNS (" +
                  ", ".join(c + " STRING" for c in add) + ")")
    print(t, "| added:", add if add else "nothing, already aligned")

# D-224 wants MEWS. NB00 wrote "Mews". Property resolution joins on exact text,
# so a mismatch here silently returns zero rows later.
print("\nPMS values already stored (history is not rewritten):")
spark.table("ExtractionRunLog").groupBy("PMS").count().show(20, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 — `B_PropertySourceIdentity`: append only

# CELL ********************

s = sheet("B_PropertySourceIdentity", ID + ["ValidFromUtc"])
s["ValidFromUtc"] = pd.to_datetime(s["ValidFromUtc"], utc=True).dt.tz_convert(None)
print("Seed rows:")
print(s.to_string(index=False))

if spark.catalog.tableExists("B_PropertySourceIdentity"):
    old = spark.table("B_PropertySourceIdentity").toPandas()
    m = s.merge(old, on=ID, how="left", suffixes=("_new", "_old"))
    changed = m[m["ValidFromUtc_old"].notna() &
                (m["ValidFromUtc_new"] != m["ValidFromUtc_old"])]
    if len(changed):
        raise RuntimeError(
            "These identities are already stored with a different ValidFromUtc. "
            "A used identity is never edited - add the replacement as a NEW row:\n"
            + changed.to_string(index=False)
        )
    s = (m[m["ValidFromUtc_old"].isna()][ID + ["ValidFromUtc_new"]]
         .rename(columns={"ValidFromUtc_new": "ValidFromUtc"}))

if len(s):
    (spark.createDataFrame(s).write.format("delta")
     .mode("append").saveAsTable("B_PropertySourceIdentity"))
print("\nAppended:", len(s), "row(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 — `PropertyExtractionConfig`: current state, replaced from seed
# 
# The seed sheet only has `PropertyKey` + `PMS`. This table only ever turns LIVE
# extraction on or off, so a per-row `SourceType` would always read `LIVE` -
# typing it every time is duplicate data entry, not a governed choice. Same for
# `SourcePropertyCode`: it already lives in `B_PropertySourceIdentity`, keyed on
# the same `PropertyKey + PMS + LIVE`, so it is looked up from there instead of
# retyped.
# 
# This is a modeling call, not something read off the workbook - flag it if the
# governance intends something different.

# CELL ********************

CFG = ["PropertyKey", "PMS", "IsLiveExtractionEnabled", "ColdStartUpdatedUtcFrom",
       "MewsScopeType", "MewsScopeIds"]

c = sheet("PropertyExtractionConfig", CFG)
c["IsLiveExtractionEnabled"] = c["IsLiveExtractionEnabled"].str.strip().str.upper().eq("TRUE")
c["ColdStartUpdatedUtcFrom"] = pd.to_datetime(
    c["ColdStartUpdatedUtcFrom"], errors="coerce", utc=True).dt.tz_convert(None)

# This table only ever configures LIVE extraction - not inferred per row, it is
# what the table is for. SourcePropertyCode is looked up from the identity table
# (the one place it is authoritative), never retyped or invented.
c["SourceType"] = "LIVE"
live_ids = (spark.table("B_PropertySourceIdentity")
            .filter("SourceType = 'LIVE'")
            .select("PropertyKey", "PMS", "SourcePropertyCode").toPandas())
c = c.merge(live_ids, on=["PropertyKey", "PMS"], how="left")

# An ENABLED row with no matching LIVE identity would run extraction against
# nothing and still report success. A disabled row is inert - SourcePropertyCode
# stays blank rather than forcing a LIVE identity to be created early.
bad = c[c["IsLiveExtractionEnabled"] & c["SourcePropertyCode"].isna()]
if len(bad):
    raise RuntimeError(
        "Enabled config rows with no matching LIVE identity in "
        "B_PropertySourceIdentity (add that identity, or disable the row):\n"
        + bad[["PropertyKey", "PMS"]].to_string(index=False)
    )

out = c[ID + ["IsLiveExtractionEnabled", "ColdStartUpdatedUtcFrom",
             "MewsScopeType", "MewsScopeIds"]].copy()

# An all-null column (e.g. SourcePropertyCode when no LIVE identity exists yet)
# has no type Arrow can infer, and gets silently dropped from the saved table.
# An explicit schema plus real None values (not NaN/NaT/"") avoids that.
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType)
CFG_SCHEMA = StructType([
    StructField("PropertyKey", StringType()),
    StructField("PMS", StringType()),
    StructField("SourceType", StringType()),
    StructField("SourcePropertyCode", StringType()),
    StructField("IsLiveExtractionEnabled", BooleanType()),
    StructField("ColdStartUpdatedUtcFrom", TimestampType()),
    StructField("MewsScopeType", StringType()),
    StructField("MewsScopeIds", StringType()),
])
out["ColdStartUpdatedUtcFrom"] = out["ColdStartUpdatedUtcFrom"].astype(object).where(
    out["ColdStartUpdatedUtcFrom"].notna(), None)
for col in ["SourcePropertyCode", "MewsScopeType", "MewsScopeIds"]:
    out[col] = out[col].astype(object).where(out[col].notna() & (out[col] != ""), None)

(spark.createDataFrame(out, schema=CFG_SCHEMA)
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("PropertyExtractionConfig"))
print(c.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 — `ExtractionWatermark`: create the empty table only

# CELL ********************

WANT = ID + ["CapturedThroughUtc"]

def create():
    spark.sql("""CREATE TABLE ExtractionWatermark (
        PropertyKey STRING, PMS STRING, SourceType STRING,
        SourcePropertyCode STRING, CapturedThroughUtc TIMESTAMP) USING DELTA""")

if not spark.catalog.tableExists("ExtractionWatermark"):
    create()

cols = [f.name for f in spark.table("ExtractionWatermark").schema.fields]
n = spark.table("ExtractionWatermark").count()

if cols != WANT:
    if n > 0:
        raise RuntimeError(
            "ExtractionWatermark shape is " + str(cols) + ", expected " + str(WANT) +
            ". It holds " + str(n) + " row(s), which may be real extraction state, "
            "so nothing was dropped. Decide deliberately."
        )
    # 0 rows = a stale table from the old pre-rework structure, nothing to lose.
    print("Stale ExtractionWatermark found:", cols)
    print("0 rows - safe to drop and recreate with the governed shape.")
    spark.sql("DROP TABLE ExtractionWatermark")
    create()
    cols = [f.name for f in spark.table("ExtractionWatermark").schema.fields]
    n = spark.table("ExtractionWatermark").count()

print("ExtractionWatermark:", cols)
print("rows:", n, "- 0 is correct. A value is only written by a successful extraction run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 — Read these. This is the check.

# CELL ********************

for t in ["ExtractionRunLog", "ExtractionFileLog", "B_PropertySourceIdentity",
          "PropertyExtractionConfig", "ExtractionWatermark", "D_Property"]:
    print("=" * 60)
    print(t, "|", spark.table(t).count(), "rows")
    spark.table(t).show(60, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Still open
# 
# - Nothing populates the five new log columns yet — `NB00` and `NB06` must be
#   updated. This only makes room for them.
# - The DEMO `SourcePropertyCode` must equal the `EnterpriseId` that `NB10` reads
#   from the Mews services payload, or F-5 resolution finds nothing.
# - The Mews scope identifier is unverified against the real source.
# - Cell 3 assumes `PropertyExtractionConfig` is LIVE-only and pulls
#   `SourcePropertyCode` from the identity table rather than the seed sheet - a
#   modeling call made this session, not something read off the workbook. Flag it
#   if governance intends config to carry its own identity columns.
# - F-5 (NB10 rework) and NB06 are not covered here.
# 
# Pause capacity `fabaurorabiv1devf2` in Azure when you are done.

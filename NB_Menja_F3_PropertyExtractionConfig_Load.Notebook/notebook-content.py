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

# # F-3 — Build `PropertyExtractionConfig` from the manual seed sheet
# 
# **Prepared for review. Nothing here has been run.**
# 
# ## What this does in plain words
# 
# `PropertyExtractionConfig` is the switchboard for live extraction. One row says:
# 
# > "For this exact property + source, live extraction is on or off, start from
# > this date, and ask Mews using this scope."
# 
# This notebook copies that sheet from the seed workbook into a governed Delta
# table, with heavy checking.
# 
# ## The governed shape (FINAL D-219)
# 
# | Column | Role |
# |---|---|
# | `PropertyKey` | Menja property key |
# | `PMS` | `MEWS` (uppercase, D-224) |
# | `SourceType` | **must be `LIVE`** — any other value is invalid for this table |
# | `SourcePropertyCode` | source-native property code |
# | `IsLiveExtractionEnabled` | on/off switch for the live extraction path only |
# | `ColdStartUpdatedUtcFrom` | first-run lower boundary (D-214) |
# | `MewsScopeType` | request scope type (D-215) |
# | `MewsScopeIds` | request scope identifiers (D-215) |
# 
# The first four columns together identify one eligible LIVE
# PropertySourceIdentity (D-228).
# 
# ## Values you confirmed for OSL
# 
# - `ColdStartUpdatedUtcFrom = 2025-01-01T00:00:00Z`
# - `MewsScopeType = PROPERTY`
# - `MewsScopeIds = 851df8c8-90f2-4c4a-8e01-a4fc46b25178`
# 
# ## Important safety gate
# 
# D-215 requires `MewsScopeIds` to be **approved, non-demo** identifiers. Nothing
# in governance or in this notebook can prove that the GUID above is the correct
# live OSL scope. A wrong GUID either errors at Mews or silently returns nothing.
# 
# So: this notebook **refuses to load an enabled row** until you set
# `MEWS_SCOPE_IDS_VERIFIED = True` after checking the GUID against Mews yourself.
# A disabled row loads without that flag.
# 
# ## Before running
# 
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` **first**.
# 2. Run **F-2 first**. This notebook checks every config row against
#    `B_PropertySourceIdentity`.
# 3. Run cells in order. Nothing is written until Section 5.


# MARKDOWN ********************

# ## Section 1 — Settings
# 
# One decision is baked in here and you should confirm it: the config table is
# treated as **current state** and is fully replaced from the seed on each run.
# 
# That differs from `B_PropertySourceIdentity`, which is append-only because D-229
# protects historical identities. D-219 does not state a retention rule for config,
# so replace-from-seed is the simplest honest reading — the seed is the single
# governed source of config content. Say so if you want different behaviour.

# CELL ********************

# F-3 Section 1 - settings

import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)

SEED_XLSX_PATH = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"
SEED_SHEET = "PropertyExtractionConfig"      # D-198 allow-listed sheet
TARGET_TABLE = "PropertyExtractionConfig"
IDENTITY_TABLE = "B_PropertySourceIdentity"  # built by F-2

# SAFETY GATE - see the header. Leave False until you have verified the
# MewsScopeIds values against the real Mews source (D-215 requires approved,
# non-demo identifiers).
MEWS_SCOPE_IDS_VERIFIED = False

IDENTITY_COLS = ["PropertyKey", "PMS", "SourceType", "SourcePropertyCode"]
GOVERNED_COLS = IDENTITY_COLS + [
    "IsLiveExtractionEnabled",
    "ColdStartUpdatedUtcFrom",
    "MewsScopeType",
    "MewsScopeIds",
]

ALLOWED_PMS = {"MEWS"}                 # D-224
REQUIRED_SOURCE_TYPE = "LIVE"          # D-219: every row must be LIVE

target_schema = StructType([
    StructField("PropertyKey", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("SourceType", StringType(), False),
    StructField("SourcePropertyCode", StringType(), False),
    StructField("IsLiveExtractionEnabled", BooleanType(), False),
    # D-219: these three are mandatory only when IsLiveExtractionEnabled = TRUE.
    # For FALSE rows they may remain blank, so the columns are nullable.
    StructField("ColdStartUpdatedUtcFrom", TimestampType(), True),
    StructField("MewsScopeType", StringType(), True),
    StructField("MewsScopeIds", StringType(), True),
])

print("Seed sheet:", SEED_SHEET)
print("Target    :", TARGET_TABLE)
print("MewsScopeIds verified by user:", MEWS_SCOPE_IDS_VERIFIED)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 — Read the seed sheet

# CELL ********************

# F-3 Section 2 - read the seed sheet

if not os.path.isfile(SEED_XLSX_PATH):
    raise RuntimeError("Seed workbook not found at " + SEED_XLSX_PATH + ".")

xls = pd.ExcelFile(SEED_XLSX_PATH)
print("Sheets present:", xls.sheet_names)

if SEED_SHEET not in xls.sheet_names:
    raise RuntimeError("Required seed sheet '" + SEED_SHEET + "' not found.")

seed_pdf = pd.read_excel(xls, sheet_name=SEED_SHEET, dtype=str).dropna(how="all")
print("Rows read :", len(seed_pdf))
print("Columns   :", list(seed_pdf.columns))

missing_cols = [c for c in GOVERNED_COLS if c not in seed_pdf.columns]
if missing_cols:
    raise RuntimeError(
        "Governed D-219 columns missing from seed sheet: " + str(missing_cols) +
        ". Actual columns: " + str(list(seed_pdf.columns)) +
        ". Fix the seed sheet; do not infer columns here."
    )

extra = [c for c in seed_pdf.columns if c not in GOVERNED_COLS]
if extra:
    print("NOTE - ungoverned seed columns will be dropped:", extra)

seed_pdf = seed_pdf[GOVERNED_COLS].copy()
print("\n--- seed content as read ---")
print(seed_pdf.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3 — Validate the config content
# 
# Each check is a stop. Mapping to governance:
# 
# - blanks in identity, scope or cold-start fields → D-219 / D-214 / D-215
# - `SourceType` not `LIVE` → D-219 ("any other SourceType is invalid for this table")
# - `PMS` not `MEWS` → D-224
# - duplicate identity → D-219 / D-228 (one row per eligible LIVE identity)
# - `ColdStartUpdatedUtcFrom` unparseable or not in the past → D-214 (must be earlier
#   than the fixed extraction upper boundary)
# - empty `MewsScopeType` / `MewsScopeIds` → D-215
# - enabled row while `MEWS_SCOPE_IDS_VERIFIED = False` → the D-215 "approved,
#   non-demo" requirement is unproven

# CELL ********************

# F-3 Section 3 - validation. Every failure stops the notebook.

from datetime import datetime, timezone

errors = []

# Identity fields and the enable flag are always required (D-219).
for c in IDENTITY_COLS + ["IsLiveExtractionEnabled"]:
    blank = seed_pdf[c].isna() | (seed_pdf[c].astype(str).str.strip() == "")
    if blank.any():
        errors.append("Column '" + c + "' is blank in seed rows: " +
                      str([int(i) + 2 for i in seed_pdf.index[blank]]))

for c in IDENTITY_COLS:
    seed_pdf[c] = seed_pdf[c].astype(str).str.strip()

for c in ["MewsScopeType", "MewsScopeIds"]:
    seed_pdf[c] = seed_pdf[c].astype(str).str.strip().replace({"nan": "", "None": ""})

# PMS vocabulary (D-224)
bad_pms = sorted(set(seed_pdf["PMS"]) - ALLOWED_PMS)
if bad_pms:
    errors.append("PMS values not allowed under D-224: " + str(bad_pms))

# SourceType must be LIVE for every row (D-219)
bad_st = sorted(set(seed_pdf["SourceType"]) - {REQUIRED_SOURCE_TYPE})
if bad_st:
    errors.append(
        "D-219 requires SourceType = LIVE for every PropertyExtractionConfig row. "
        "Found: " + str(bad_st)
    )

# Identity uniqueness (D-219 / D-228)
dup = seed_pdf.duplicated(subset=IDENTITY_COLS, keep=False)
if dup.any():
    errors.append("Duplicate config identities:\n" +
                  seed_pdf.loc[dup, IDENTITY_COLS].to_string(index=False))

# Boolean parsing for IsLiveExtractionEnabled - strict, no guessing
TRUE_SET = {"TRUE", "T", "YES", "Y", "1"}
FALSE_SET = {"FALSE", "F", "NO", "N", "0"}
raw_flag = seed_pdf["IsLiveExtractionEnabled"].astype(str).str.strip().str.upper()
unknown_flag = sorted(set(raw_flag) - TRUE_SET - FALSE_SET)
if unknown_flag:
    errors.append(
        "IsLiveExtractionEnabled values could not be read as true/false: " +
        str(unknown_flag) + ". D-219 states a blank, null, missing or invalid "
        "value must not be interpreted as TRUE, so this stops rather than "
        "guessing. Use TRUE or FALSE in the seed sheet."
    )
else:
    seed_pdf["IsLiveExtractionEnabled"] = raw_flag.isin(TRUE_SET)

# D-219: the three detail fields are mandatory ONLY when enabled.
# For disabled rows they may remain blank.
cold = pd.to_datetime(seed_pdf["ColdStartUpdatedUtcFrom"], errors="coerce", utc=True)
seed_pdf["ColdStartUpdatedUtcFrom"] = cold.dt.tz_convert(None)

if "IsLiveExtractionEnabled" in seed_pdf.columns and seed_pdf["IsLiveExtractionEnabled"].dtype == bool:
    enabled_mask = seed_pdf["IsLiveExtractionEnabled"]
    now_utc = pd.Timestamp(datetime.now(timezone.utc))

    for c in ["MewsScopeType", "MewsScopeIds"]:
        bad = enabled_mask & (seed_pdf[c].astype(str).str.strip() == "")
        if bad.any():
            errors.append(
                "D-219: '" + c + "' is mandatory when IsLiveExtractionEnabled = TRUE. "
                "Blank in rows: " + str([int(i) + 2 for i in seed_pdf.index[bad]])
            )

    bad_cold = enabled_mask & cold.isna()
    if bad_cold.any():
        errors.append(
            "D-219/D-214: ColdStartUpdatedUtcFrom is mandatory and must be a valid "
            "UTC timestamp when IsLiveExtractionEnabled = TRUE. Missing or "
            "unparseable in rows: " + str([int(i) + 2 for i in seed_pdf.index[bad_cold]])
        )

    not_past = enabled_mask & cold.notna() & (cold >= now_utc)
    if not_past.any():
        errors.append(
            "D-214: ColdStartUpdatedUtcFrom must be earlier than the extraction "
            "upper boundary. Not in the past in rows: " +
            str([int(i) + 2 for i in seed_pdf.index[not_past]])
        )

    # D-219 + D-228: at most one enabled configuration per PropertyKey at a time.
    multi = (seed_pdf[enabled_mask].groupby("PropertyKey").size())
    multi = multi[multi > 1]
    if len(multi) > 0:
        errors.append(
            "D-219: at most one live extraction configuration may be enabled per "
            "PropertyKey at a time. These PropertyKeys have more than one enabled "
            "row: " + str(list(multi.index))
        )

# Safety gate on enabled rows (D-215 approved, non-demo scope identifiers)
if not errors and seed_pdf["IsLiveExtractionEnabled"].any() and not MEWS_SCOPE_IDS_VERIFIED:
    enabled = seed_pdf[seed_pdf["IsLiveExtractionEnabled"]]
    errors.append(
        "These rows are enabled for live extraction but MewsScopeIds have not been "
        "confirmed as approved non-demo identifiers (D-215):\n" +
        enabled[IDENTITY_COLS + ["MewsScopeType", "MewsScopeIds"]].to_string(index=False) +
        "\nVerify the scope identifiers against Mews, then set "
        "MEWS_SCOPE_IDS_VERIFIED = True in Section 1."
    )

if errors:
    raise RuntimeError(
        "Config validation failed. Nothing was written.\n\n- " + "\n- ".join(errors)
    )

print("Config validation passed.")
print("Enabled rows :", int(seed_pdf["IsLiveExtractionEnabled"].sum()))
print("Disabled rows:", int((~seed_pdf["IsLiveExtractionEnabled"]).sum()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 — Check each config row against `B_PropertySourceIdentity`
# 
# D-215 and D-228 require each config row to identify **exactly one** eligible LIVE
# PropertySourceIdentity. This section proves that. Zero matches or more than one
# match stops the notebook.
# 
# Nothing is written here.

# CELL ********************

# F-3 Section 4 - cross-check against B_PropertySourceIdentity (read-only)

if not spark.catalog.tableExists(IDENTITY_TABLE):
    raise RuntimeError(
        IDENTITY_TABLE + " does not exist. Run F-2 first. "
        "D-215 requires each config row to resolve to exactly one governed "
        "PropertySourceIdentity."
    )

ident_pdf = (spark.table(IDENTITY_TABLE)
             .select(*IDENTITY_COLS)
             .toPandas())

print("Identity rows available:", len(ident_pdf))

counts = (seed_pdf[IDENTITY_COLS]
          .merge(ident_pdf.assign(_hit=1), on=IDENTITY_COLS, how="left")
          .groupby(IDENTITY_COLS, dropna=False)["_hit"]
          .sum()
          .reset_index()
          .rename(columns={"_hit": "MatchCount"}))

print("\n--- match counts per config identity (each must be exactly 1) ---")
print(counts.to_string(index=False))

bad = counts[counts["MatchCount"] != 1]
if len(bad) > 0:
    raise RuntimeError(
        "Every PropertyExtractionConfig row must resolve to exactly one LIVE "
        "PropertySourceIdentity (D-215, D-228). These do not:\n" +
        bad.to_string(index=False) +
        "\nFix B_PropertySourceIdentity or the config seed. Do not guess a match."
    )

print("\nAll config identities resolve to exactly one governed LIVE identity.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5 — Write (replace current state)
# 
# This is the only cell that changes the lakehouse. It replaces the whole config
# table from the validated seed.
# 
# The previous contents are printed first so you have a record of what changed.

# CELL ********************

# F-3 Section 5 - replace config table from validated seed

if spark.catalog.tableExists(TARGET_TABLE):
    print("--- current table contents BEFORE replace ---")
    spark.table(TARGET_TABLE).show(200, False)
else:
    print(TARGET_TABLE, "does not exist yet; it will be created.")

df_cfg = spark.createDataFrame(seed_pdf[GOVERNED_COLS], schema=target_schema)

(df_cfg.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(TARGET_TABLE))

print("Wrote", df_cfg.count(), "rows to", TARGET_TABLE, ".")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6 — Validation

# CELL ********************

# F-3 Section 6 - validation evidence

t = spark.table(TARGET_TABLE)
cols = [f.name for f in t.schema.fields]

print("Columns   :", cols)
print("Row count :", t.count())

if cols != GOVERNED_COLS:
    raise RuntimeError("Unexpected column set. Expected " + str(GOVERNED_COLS) +
                       " but found " + str(cols))

bad_st = t.filter(F.col("SourceType") != F.lit(REQUIRED_SOURCE_TYPE)).count()
print("Rows with SourceType != LIVE (expect 0):", bad_st)
if bad_st:
    raise RuntimeError("D-219 requires SourceType = LIVE on every row.")

n_dup = t.groupBy(*IDENTITY_COLS).count().filter(F.col("count") > 1).count()
print("Duplicate config identities (expect 0):", n_dup)
if n_dup:
    raise RuntimeError("Duplicate config identities found.")

print("\nEnabled/disabled split:")
t.groupBy("IsLiveExtractionEnabled").count().show(10, False)

print("Full table contents:")
t.show(200, False)

print("F-3 validation finished.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## What is still open after F-3
# 
# - The OSL `MewsScopeIds` GUID is still **unverified against Mews**. Until you
#   verify it and set `MEWS_SCOPE_IDS_VERIFIED = True`, an enabled row will not
#   load.
# - Live extraction stays off until you deliberately turn it on in the seed sheet.
# 
# **Reminder:** pause Fabric capacity `fabaurorabiv1devf2` in Azure when you are
# done working.

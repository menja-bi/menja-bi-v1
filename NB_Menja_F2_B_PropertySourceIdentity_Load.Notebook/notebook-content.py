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

# # F-2 — Build `B_PropertySourceIdentity` from the manual seed sheet
# 
# **Prepared for review. Nothing here has been run.**
# 
# ## What this does in plain words
# 
# `B_PropertySourceIdentity` is a small hand-maintained lookup table. Its job is
# to answer one question:
# 
# > "Given a PMS, a source type, and the property code that the source system
# > uses, which Menja property is this?"
# 
# This notebook copies that hand-maintained sheet out of the seed workbook into a
# governed Delta table, checking it carefully on the way.
# 
# ## The governed shape (column governance C-416 to C-420)
# 
# | Column | Role |
# |---|---|
# | `PropertyKey` | The Menja property key. This is the key everything downstream joins on. |
# | `PMS` | Which PMS contract. Uppercase. `MEWS` today. |
# | `SourceType` | `DEMO`, `LIVE` or `SYNTHETIC`. |
# | `SourcePropertyCode` | The property code the source system uses. Never a Menja key. |
# | `ValidFromUtc` | When this identity became valid. |
# 
# The logical grain (T-040) is one row per
# `PropertyKey + PMS + SourceType + SourcePropertyCode`.
# 
# **Note:** the consolidated package brief listed only the four identity fields.
# FINAL column governance C-420 also requires `ValidFromUtc`, so this notebook
# loads five columns. Confirm that is what you expect.
# 
# ## Rules this notebook enforces
# 
# - **Append only.** D-229 says a row already used by governed source data must not
#   be overwritten, deleted, reassigned, or replaced. So this notebook adds new
#   rows and **stops** if the seed tries to change an existing row.
# - **No derivation.** Rows are never built from PMS data (T-040).
# - **No extras.** No surrogate key, no `IsActive`, no `IsCurrent`, no automatic
#   source switching, no fallback to `D_Property.PMS_PropertyCode`.
# - **Stop, do not guess.** Any missing, blank, duplicate, or off-vocabulary value
#   raises an error.
# 
# ## Before running
# 
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` **first**.
# 2. Make sure the current seed workbook is uploaded to the lakehouse `Files/Seeds`
#    area (same path `NB25` uses).
# 3. Run cells in order. Nothing is written until Section 5.


# MARKDOWN ********************

# ## Section 1 — Settings
# 
# `SEED_SHEET` is read explicitly by name. D-198 requires Fabric code to select
# allow-listed sheets deliberately rather than importing everything.
# `B_PropertySourceIdentity` is on the D-198 allow-list.

# CELL ********************

# F-2 Section 1 - settings

import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

SEED_XLSX_PATH = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"
SEED_SHEET = "B_PropertySourceIdentity"          # D-198 allow-listed sheet
TARGET_TABLE = "B_PropertySourceIdentity"        # governed Delta table (T-040)

# Governed columns, in governed order (C-416 to C-420)
IDENTITY_COLS = ["PropertyKey", "PMS", "SourceType", "SourcePropertyCode"]
GOVERNED_COLS = IDENTITY_COLS + ["ValidFromUtc"]

# Governed vocabulary (D-224)
ALLOWED_PMS = {"MEWS"}
ALLOWED_SOURCE_TYPE = {"DEMO", "LIVE", "SYNTHETIC"}

# Optional cross-check: confirm each PropertyKey exists in D_Property (C-416).
# Set to False only if D_Property has not been built yet in this environment.
CHECK_PROPERTYKEY_AGAINST_D_PROPERTY = True

target_schema = StructType([
    StructField("PropertyKey", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("SourceType", StringType(), False),
    StructField("SourcePropertyCode", StringType(), False),
    StructField("ValidFromUtc", TimestampType(), False),
])

print("Seed path :", SEED_XLSX_PATH)
print("Seed sheet:", SEED_SHEET)
print("Target    :", TARGET_TABLE)
print("Governed columns:", GOVERNED_COLS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 — Read the seed sheet
# 
# If the sheet or a governed column is missing, this stops. It does not fall back
# to another sheet and does not guess a column name.

# CELL ********************

# F-2 Section 2 - read the seed sheet, stop on anything unexpected

if not os.path.isfile(SEED_XLSX_PATH):
    raise RuntimeError(
        "Seed workbook not found at " + SEED_XLSX_PATH +
        ". Upload the current runtime copy to the lakehouse Files/Seeds area first."
    )

xls = pd.ExcelFile(SEED_XLSX_PATH)
print("Sheets present in seed workbook:", xls.sheet_names)

if SEED_SHEET.startswith("_"):
    raise RuntimeError("Sheets starting with '_' must not be imported as seed data (D-198).")

if SEED_SHEET not in xls.sheet_names:
    raise RuntimeError(
        "Required seed sheet '" + SEED_SHEET + "' not found. "
        "Do not substitute another sheet."
    )

seed_pdf = pd.read_excel(xls, sheet_name=SEED_SHEET, dtype=str).dropna(how="all")
print("Rows read from seed sheet:", len(seed_pdf))
print("Columns found            :", list(seed_pdf.columns))

missing_cols = [c for c in GOVERNED_COLS if c not in seed_pdf.columns]
if missing_cols:
    raise RuntimeError(
        "Governed columns missing from seed sheet: " + str(missing_cols) +
        ". Actual columns: " + str(list(seed_pdf.columns)) +
        ". Fix the seed sheet; do not rename or infer columns here."
    )

extra_cols = [c for c in seed_pdf.columns if c not in GOVERNED_COLS]
if extra_cols:
    print("NOTE - these seed columns are not governed columns and will be dropped:", extra_cols)

seed_pdf = seed_pdf[GOVERNED_COLS].copy()
print("\n--- seed content as read ---")
print(seed_pdf.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3 — Validate the seed content
# 
# Every check below is a stop, not a warning. Each one maps to a FINAL decision:
# 
# - blanks → C-416 to C-420 require all five values
# - `PMS` not uppercase / not `MEWS` → D-224
# - `SourceType` outside `DEMO/LIVE/SYNTHETIC`, or `HISTORICAL` → D-224
# - duplicate identity → T-040 grain
# - `ValidFromUtc` not a real timestamp → C-420
# - `PropertyKey` not in `D_Property` → C-416

# CELL ********************

# F-2 Section 3 - validation. Every failure stops the notebook.

errors = []

# 1. No blanks anywhere in the governed columns.
for c in GOVERNED_COLS:
    blank = seed_pdf[c].isna() | (seed_pdf[c].astype(str).str.strip() == "")
    if blank.any():
        errors.append("Column '" + c + "' has blank values in rows: " +
                      str([int(i) + 2 for i in seed_pdf.index[blank]]))

# 2. Trim whitespace (safe normalisation only - no value substitution).
for c in IDENTITY_COLS:
    seed_pdf[c] = seed_pdf[c].astype(str).str.strip()

# 3. PMS vocabulary and case (D-224).
bad_pms = sorted(set(seed_pdf["PMS"]) - ALLOWED_PMS)
if bad_pms:
    errors.append(
        "PMS values not allowed under D-224 (uppercase, initial value MEWS): " + str(bad_pms)
    )

# 4. SourceType vocabulary (D-224). HISTORICAL is explicitly not a SourceType.
bad_st = sorted(set(seed_pdf["SourceType"]) - ALLOWED_SOURCE_TYPE)
if bad_st:
    errors.append("SourceType values not allowed under D-224: " + str(bad_st))

# 5. Identity uniqueness (T-040 grain).
dup_mask = seed_pdf.duplicated(subset=IDENTITY_COLS, keep=False)
if dup_mask.any():
    errors.append(
        "Duplicate PropertySourceIdentity rows in seed (grain is "
        "PropertyKey + PMS + SourceType + SourcePropertyCode):\n" +
        seed_pdf.loc[dup_mask, IDENTITY_COLS].to_string(index=False)
    )

# 6. ValidFromUtc must parse as a real timestamp (C-420).
parsed = pd.to_datetime(seed_pdf["ValidFromUtc"], errors="coerce", utc=True)
if parsed.isna().any():
    errors.append(
        "ValidFromUtc could not be parsed as a UTC timestamp in rows: " +
        str([int(i) + 2 for i in seed_pdf.index[parsed.isna()]])
    )
else:
    seed_pdf["ValidFromUtc"] = parsed.dt.tz_convert(None)

# 7. PropertyKey must resolve to a governed Menja property (C-416).
if CHECK_PROPERTYKEY_AGAINST_D_PROPERTY:
    if not spark.catalog.tableExists("D_Property"):
        errors.append(
            "D_Property does not exist, so PropertyKey cannot be validated (C-416). "
            "Build D_Property first, or set CHECK_PROPERTYKEY_AGAINST_D_PROPERTY = False "
            "and record that the check was skipped."
        )
    else:
        known = {r["PropertyKey"] for r in
                 spark.table("D_Property").select("PropertyKey").distinct().collect()}
        unknown = sorted(set(seed_pdf["PropertyKey"]) - known)
        if unknown:
            errors.append("PropertyKey values not present in D_Property: " + str(unknown))

if errors:
    raise RuntimeError(
        "Seed validation failed. Fix the seed sheet and re-run. "
        "Nothing was written.\n\n- " + "\n- ".join(errors)
    )

print("Seed validation passed.")
print("Distinct SourceType values:", sorted(set(seed_pdf["SourceType"])))
print("Rows ready to consider for load:", len(seed_pdf))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 — Compare against what is already stored
# 
# D-229 protects history. So before writing, this section works out three groups:
# 
# - **NEW** — identities not in the table yet. These get appended.
# - **UNCHANGED** — already stored with the same `ValidFromUtc`. Skipped.
# - **CONFLICT** — already stored but the seed now shows a different
#   `ValidFromUtc`. This **stops** the notebook. A changed identity must be added
#   as a *new* row, not an edit of an old one.
# 
# Nothing is written in this section.

# CELL ********************

# F-2 Section 4 - classify seed rows against the existing table (read-only)

table_exists = spark.catalog.tableExists(TARGET_TABLE)

if not table_exists:
    print(TARGET_TABLE, "does not exist yet. All seed rows are NEW.")
    new_pdf = seed_pdf.copy()
    unchanged_n = 0
else:
    existing_pdf = spark.table(TARGET_TABLE).select(*GOVERNED_COLS).toPandas()
    print("Existing rows in", TARGET_TABLE, ":", len(existing_pdf))

    merged = seed_pdf.merge(
        existing_pdf, on=IDENTITY_COLS, how="left",
        suffixes=("_seed", "_stored"),
    )

    is_new = merged["ValidFromUtc_stored"].isna()
    matched = merged[~is_new].copy()

    conflict = matched[
        pd.to_datetime(matched["ValidFromUtc_seed"]).ne(
            pd.to_datetime(matched["ValidFromUtc_stored"]))
    ]
    if len(conflict) > 0:
        raise RuntimeError(
            "D-229 violation blocked. These identities already exist but the seed "
            "shows a different ValidFromUtc. A used identity must not be edited; "
            "add a replacement as a new row instead. Nothing was written.\n\n" +
            conflict[IDENTITY_COLS + ["ValidFromUtc_seed", "ValidFromUtc_stored"]]
            .to_string(index=False)
        )

    unchanged_n = len(matched)
    new_pdf = merged[is_new][IDENTITY_COLS + ["ValidFromUtc_seed"]].rename(
        columns={"ValidFromUtc_seed": "ValidFromUtc"})

print("-" * 70)
print("NEW rows to append :", len(new_pdf))
print("UNCHANGED, skipped :", unchanged_n)
if len(new_pdf) > 0:
    print("\n--- rows that would be appended ---")
    print(new_pdf.to_string(index=False))
print("\nNothing has been written yet.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5 — Write (append only)
# 
# This is the only cell that changes the lakehouse.
# 
# - If the table does not exist, it is created with the governed five-column schema.
# - If it exists, only the NEW rows are appended.
# - Nothing is ever updated or deleted.

# CELL ********************

# F-2 Section 5 - create if missing, then append NEW rows only

if len(new_pdf) == 0 and table_exists:
    print("No new identities to load. Table left unchanged.")
else:
    df_new = spark.createDataFrame(new_pdf[GOVERNED_COLS], schema=target_schema)

    if not table_exists:
        df_new.write.format("delta").saveAsTable(TARGET_TABLE)
        print("Created", TARGET_TABLE, "with", df_new.count(), "rows.")
    else:
        df_new.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print("Appended", df_new.count(), "new rows to", TARGET_TABLE, ".")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6 — Validation
# 
# Keep this output as your evidence. Expected:
# 
# - the table has exactly the five governed columns
# - the identity grain is unique (no duplicates)
# - `PMS` is `MEWS`, `SourceType` is only `DEMO` / `LIVE` / `SYNTHETIC`
# - the rows you expect for OSL are present

# CELL ********************

# F-2 Section 6 - validation evidence

t = spark.table(TARGET_TABLE)
cols = [f.name for f in t.schema.fields]

print("Columns   :", cols)
print("Row count :", t.count())

if cols != GOVERNED_COLS:
    raise RuntimeError("Unexpected column set. Expected " + str(GOVERNED_COLS) +
                       " but found " + str(cols))

dupes = (t.groupBy(*IDENTITY_COLS).count().filter(F.col("count") > 1))
n_dupes = dupes.count()
print("Duplicate identity rows (expect 0):", n_dupes)
if n_dupes:
    dupes.show(50, False)
    raise RuntimeError("Duplicate PropertySourceIdentity rows found. Grain is broken.")

print("\nDistinct PMS values (expect only MEWS):")
t.groupBy("PMS").count().orderBy("PMS").show(20, False)

print("Distinct SourceType values:")
t.groupBy("SourceType").count().orderBy("SourceType").show(20, False)

print("Full table contents:")
t.orderBy("PropertyKey", "PMS", "SourceType", "SourcePropertyCode").show(200, False)

print("F-2 validation finished.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## What to check by eye before moving on
# 
# F-3, F-4 and F-5 all depend on the right rows existing here. Specifically:
# 
# - The **DEMO** row for OSL must exist, and its `SourcePropertyCode` must be the
#   Mews demo `EnterpriseId` that `NB10` will actually read from the services
#   payload. If it is not, F-5 property resolution will find zero matches.
# - The **LIVE** row for OSL must exist before F-3 can load an enabled
#   `PropertyExtractionConfig` row.
# 
# Neither of those can be confirmed from governance alone — they depend on the seed
# content you maintain.
# 
# **Reminder:** pause Fabric capacity `fabaurorabiv1devf2` in Azure when you are
# done working.

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

# # Dimensions — BUILD_DRAFT — D_Property + D_ReservationStatus (Phase-1)
# 
# **Plain words:** this notebook reads the governed seed workbook and writes two small
# lookup tables into the lakehouse as Delta tables: `D_Property` and `D_ReservationStatus`.
# It exists so that the next notebook, `F_RoomNights`, has the two dimension tables it needs
# to read. It builds **nothing else**.
# 
# **Why this notebook exists:** the `F_RoomNights` build stopped because `D_Property` did not
# exist as a lakehouse table. `F_RoomNights` reads `D_ReservationStatus` to decide which
# room-nights count (the inventory-deduct filter) and checks that `D_Property` exists. Both
# must be present first. That is "Path B".
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until you run it
# in Fabric and confirm the results.
# 
# **What it does NOT do:** it does not invent any values. Every value comes from the governed
# seed workbook. It does not build `D_Date`, `D_RatePlan`, or any other dimension. It does not
# touch `F_RoomNights`. It does not map status behaviour from raw text — `InventoryDeduct`
# comes only from the seed.
# 
# **Governance basis:**
# 
# | Rule | Decision |
# |---|---|
# | Seed workbook is runtime input; read only allow-listed sheets; never read `_` sheets | D-198 (FINAL) |
# | `D_ReservationStatus` seed content + `InventoryDeduct` classification (Confirmed/Started/Processed deduct; Optional/Canceled/UNKNOWN do not) | D-190 (FINAL) |
# | `D_Property` governed column set | D-103 (FINAL) |
# | `D_Property` is PMS-primary with manual-seed fallback; in Phase-1 no PMS property-attribute source is live, so it behaves as manual-only (seed-sourced) | D-196 (FINAL) |
# | `PMS_PropertyCode` naming for the PMS-native property lookup value | D-204 / D-205 (FINAL) |
# | `F_RoomNights` slice reads these two dimensions; deduct filter comes only from `D_ReservationStatus.InventoryDeduct` | D-208 (FINAL) |
# 
# **Materialization:** you (governance authority) confirmed the dimensions are Delta tables in
# the lakehouse. Supporting decisions: D-208 (the fact build needs them) + D-198 (Fabric may
# read these seed sheets) + D-190 / D-103 (their governed content). This notebook only copies
# governed seed content into governed columns; it adds no logic. Recommended for the audit
# trail: a short workbook note recording that these governed dimensions are materialized as
# DEV Delta tables from the D-198 seed (this is documentation, not new logic).
# 
# **Seed headers confirmed 2026-07-11.** Two seed columns are intentionally **dropped**, because
# `03_Columns` (the column authority) does not list them for these dimensions:
# 
# - `D_ReservationStatus.PMSStatusCode` — the I_Reservations match key. D-190 states "no new
#   column is added"; it is not a governed D_ReservationStatus dimension column.
# - `D_Property.TenantKey`, `D_Property.TenantID` — tenant is governed on `D_Tenant`, and the
#   fact gets tenant from `T_MI_Property` (D-177). They are not governed D_Property columns.
# 
# Dropping them is the governed behaviour and does not affect F_RoomNights. If you instead want
# any of these retained on the dimension, that needs a governance update to `03_Columns` first
# — tell me and I will stop rather than add an ungoverned column.


# MARKDOWN ********************

# ## 1. Configuration
# 
# **Plain words:** one place for every name and path. `# <-- VERIFY` marks values you must
# check before running, because they depend on the exact seed file and its column headers,
# which this notebook cannot see from outside Fabric.
# 
# The **governed column allow-lists** below are copied from `03_Columns`. The notebook keeps
# only these columns and drops everything else in the seed sheet. This is deliberate: a
# governed dimension carries only its governed columns.
# 
# With the confirmed 2026-07-11 headers, the intentional drops are:
# `D_ReservationStatus.PMSStatusCode` (I_Reservations match key; D-190 "no new column added"),
# and `D_Property.TenantKey` / `D_Property.TenantID` (tenant is on `D_Tenant`; fact tenant comes
# from `T_MI_Property` under D-177). The build helper prints exactly what it drops so this stays
# visible at run time.

# CELL ********************

# ---------------------------------------------------------------
# 1. Configuration.
# ---------------------------------------------------------------

# --- seed workbook (D-198): runtime copy in the lakehouse Files area ---
# Same path the confirmed I_RoomNights build used to read D_Property.TimeZone.
SEED_XLSX_PATH = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"  # <-- VERIFY

# --- output tables (Delta) ---
TBL_D_PROPERTY          = "D_Property"
TBL_D_RESERVATIONSTATUS = "D_ReservationStatus"

# --- full rebuild for this DRAFT ---
WRITE_MODE = "overwrite"

# -----------------------------------------------------------------------------
# D_ReservationStatus — governed columns (03_Columns C-099..C-103, D-190)
# -----------------------------------------------------------------------------
DRS_SHEET            = "D_ReservationStatus"                 # D-198 allow-listed sheet
DRS_GOVERNED_COLS    = [
    "ReservationStatusKey",   # C-099 SurrogateKey / Primary  (KEY)
    "ReservationStatusID",    # C-100 BusinessKey
    "ReservationStatusName",  # C-101 Label
    "InventoryDeduct",        # C-102 Boolean  (DEDUCT FLAG)
    "SortOrder",              # C-103 Whole Number (nullable)
]
DRS_KEY_COL          = "ReservationStatusKey"
DRS_REQUIRED_COLS    = ["ReservationStatusKey", "InventoryDeduct"]   # hard-required by F_RoomNights
DRS_BOOL_COLS        = ["InventoryDeduct"]                           # coerce to true boolean
DRS_INT_COLS         = ["SortOrder"]                                 # coerce to int if present

# -----------------------------------------------------------------------------
# D_Property — governed columns (03_Columns C-070..C-086, C-413; D-103, D-205)
# -----------------------------------------------------------------------------
DPR_SHEET            = "D_Property"                          # D-198 allow-listed sheet
DPR_GOVERNED_COLS    = [
    "PropertyKey",          # C-070 SurrogateKey / Primary (KEY)
    "PropertyID",           # C-071 BusinessKey
    "PropertyName",         # C-072 Label
    "CountryKey",           # C-073 ForeignKey (nullable)
    "PropertyCode",         # C-076 BusinessKey
    "Brand",                # C-077
    "Chain",                # C-078
    "Country",              # C-079
    "City",                 # C-080
    "TimeZone",             # C-081
    "TotalPhysicalRooms",   # C-082 Whole Number
    "OpenDate",             # C-083 Date
    "CurrencyCode",         # C-084
    "IsActive",             # C-085 Boolean
    "LoadTimestamp",        # C-086 Technical
    "PMS_PropertyCode",     # C-413 PMS-native property lookup value (D-205)
]
DPR_KEY_COL          = "PropertyKey"
DPR_REQUIRED_COLS    = ["PropertyKey"]                      # hard-required (F_RoomNights checks this)
DPR_BOOL_COLS        = ["IsActive"]                         # coerce if present (nullable)
DPR_INT_COLS         = ["CountryKey", "TotalPhysicalRooms"] # coerce if present (nullable)

# -----------------------------------------------------------------------------
# Seed-header rename map, per sheet.
# Headers confirmed 2026-07-11: every governed column already uses its governed
# name, so no rename is needed. Empty = "seed headers already match governed names".
# -----------------------------------------------------------------------------
SEED_RENAME_MAP = {
    "D_ReservationStatus": {},   # confirmed: ReservationStatusName header matches governed name
    "D_Property": {},            # confirmed: headers match governed names
}

# -----------------------------------------------------------------------------
# Expected intentional drops (confirmed 2026-07-11 headers vs 03_Columns).
# These seed columns are NOT governed columns of these dimensions and are dropped.
# The notebook checks the actual drops match this list, so an unexpected extra
# seed column is surfaced instead of silently discarded.
# -----------------------------------------------------------------------------
EXPECTED_DROPS = {
    "D_ReservationStatus": ["PMSStatusCode"],          # I_Reservations match key (D-190)
    "D_Property": ["TenantKey", "TenantID"],           # tenant lives on D_Tenant / T_MI_Property (D-177)
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1b. Environment guard
# 
# **Plain words:** one fail-loudly check. The default lakehouse must be attached, otherwise
# nothing can be read or written. Attach `LH_Menja_BI_v1_Mews_DEV` before running.

# CELL ********************

# ---------------------------------------------------------------
# 1b. Lakehouse attachment guard — fail loudly.
# ---------------------------------------------------------------
import os

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"
if not os.path.isdir(LAKEHOUSE_FILES_ROOT):
    raise RuntimeError(
        "Default lakehouse is not attached. Attach LH_Menja_BI_v1_Mews_DEV "
        "to this notebook before running.")
print("Lakehouse attached. Files root visible.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Load the seed workbook (D-198)
# 
# **Plain words:** open the seed workbook and read only the two sheets we are allowed to read
# for this build: `D_Property` and `D_ReservationStatus`. Sheets whose names start with `_`
# are documentation and are never read (D-198). Everything is read as text first, exactly like
# the confirmed I_RoomNights build did.

# CELL ********************

# ---------------------------------------------------------------
# 2. Seed loading with explicit allow-list (D-198).
# ---------------------------------------------------------------
import pandas as pd

SEED_SHEETS_NEEDED = [DPR_SHEET, DRS_SHEET]   # explicit allow-list (D-198)

if not os.path.isfile(SEED_XLSX_PATH):
    raise RuntimeError(
        f"Seed workbook not found at {SEED_XLSX_PATH}. "
        "Upload the runtime copy to the lakehouse Files/Seeds area first.")

xls = pd.ExcelFile(SEED_XLSX_PATH)
print("Sheets present in seed workbook:", xls.sheet_names)

seed = {}
for sheet in SEED_SHEETS_NEEDED:
    if sheet.startswith("_"):
        raise RuntimeError(f"Sheet '{sheet}' starts with '_' and must not be imported (D-198).")
    if sheet not in xls.sheet_names:
        raise RuntimeError(f"Required seed sheet '{sheet}' not found in workbook (D-198 allow-list).")
    pdf = pd.read_excel(xls, sheet_name=sheet, dtype=str)
    pdf = pdf.dropna(how="all")
    seed[sheet] = pdf
    print(f"Loaded seed sheet '{sheet}': {len(pdf)} rows, columns: {list(pdf.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Shared build helper — governed-columns-only, no invention
# 
# **Plain words:** one small helper both dimensions use, so the rules are identical and
# visible. It does five things and nothing more:
# 
# 1. Apply the optional header rename (only what you confirmed in `SEED_RENAME_MAP`).
# 2. Keep **only** governed columns; drop everything else (no ungoverned columns land).
# 3. Require the key column and any hard-required columns; **fail loudly** if missing.
# 4. Check the key is present on every row and unique (a dimension key must be unique).
# 5. Convert flagged boolean and integer columns to real types; **fail loudly** on any
#    value it does not recognise (no guessing, no silent NULLs).
# 
# The boolean map is strict on purpose: only clear true/false tokens are accepted, because
# `InventoryDeduct` decides which room-nights count and must never be guessed.

# CELL ********************

# ---------------------------------------------------------------
# 3. Shared build helper.
# ---------------------------------------------------------------
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType

_TRUE_TOKENS  = {"true", "1", "yes", "y"}
_FALSE_TOKENS = {"false", "0", "no", "n"}


def _coerce_bool_series(pdf, col):
    """Map a text column to True/False. Blank stays None. Unknown tokens -> error list."""
    bad = []
    out = []
    for raw in pdf[col].tolist():
        if raw is None or (isinstance(raw, float) and np.isnan(raw)) or str(raw).strip() == "":
            out.append(None)
            continue
        tok = str(raw).strip().lower()
        if tok in _TRUE_TOKENS:
            out.append(True)
        elif tok in _FALSE_TOKENS:
            out.append(False)
        else:
            out.append(None)
            bad.append(raw)
    return out, bad


def _coerce_int_series(pdf, col):
    """Map a text column to int. Blank stays None. Unparseable -> error list."""
    bad = []
    out = []
    for raw in pdf[col].tolist():
        if raw is None or (isinstance(raw, float) and np.isnan(raw)) or str(raw).strip() == "":
            out.append(None)
            continue
        try:
            out.append(int(float(str(raw).strip())))
        except (ValueError, TypeError):
            out.append(None)
            bad.append(raw)
    return out, bad


def build_dimension(pdf_raw, sheet_name, governed_cols, key_col,
                    required_cols, bool_cols, int_cols):
    print(f"\n--- Building {sheet_name} ---")

    # 1. optional header rename (only confirmed entries)
    rename = SEED_RENAME_MAP.get(sheet_name, {})
    pdf = pdf_raw.rename(columns=rename) if rename else pdf_raw.copy()
    if rename:
        print(f"  Applied header rename: {rename}")

    # 2. keep only governed columns that are actually present
    present_governed = [c for c in governed_cols if c in pdf.columns]
    dropped = [c for c in pdf.columns if c not in governed_cols]
    expected = EXPECTED_DROPS.get(sheet_name, [])
    if dropped:
        print(f"  Dropping non-governed columns: {dropped} (expected: {expected})")
        unexpected = [c for c in dropped if c not in expected]
        if unexpected:
            print(f"  WARNING: unexpected non-governed seed column(s) dropped: {unexpected}. "
                  f"These are excluded per 03_Columns. If any should be governed, update "
                  f"03_Columns first — do not rely on this notebook to carry it.")
    pdf = pdf[present_governed].copy()
    print(f"  Governed columns kept: {present_governed}")

    # 3. hard-required columns must be present
    missing_required = [c for c in required_cols if c not in pdf.columns]
    if missing_required:
        raise RuntimeError(
            f"{sheet_name} seed is missing required column(s) {missing_required}. "
            f"F_RoomNights cannot run without them. Fix the seed headers (or add a "
            f"SEED_RENAME_MAP entry) — no fallback, no guessing.")

    # 4. key present on every row + unique
    if pdf[key_col].isna().any() or (pdf[key_col].astype(str).str.strip() == "").any():
        blanks = pdf[pdf[key_col].isna() | (pdf[key_col].astype(str).str.strip() == "")]
        raise RuntimeError(f"{sheet_name}: blank {key_col} on row(s):\n{blanks.to_string()}")
    dup = pdf[key_col].duplicated()
    if dup.any():
        raise RuntimeError(f"{sheet_name}: duplicate {key_col} value(s):\n{pdf[dup].to_string()}")

    # 5a. boolean coercion (fail loudly on unknown tokens)
    for c in bool_cols:
        if c in pdf.columns:
            vals, bad = _coerce_bool_series(pdf, c)
            if bad:
                raise RuntimeError(
                    f"{sheet_name}.{c}: value(s) not recognised as true/false: {sorted(set(bad))}. "
                    f"Governance does not permit guessing this flag.")
            pdf[c] = vals

    # 5b. integer coercion (fail loudly on unparseable)
    for c in int_cols:
        if c in pdf.columns:
            vals, bad = _coerce_int_series(pdf, c)
            if bad:
                raise RuntimeError(
                    f"{sheet_name}.{c}: value(s) not parseable as whole number: {sorted(set(bad))}.")
            pdf[c] = vals

    # required non-null flags (only the hard-required set)
    for c in required_cols:
        if c in bool_cols:
            if any(v is None for v in pdf[c].tolist()):
                raise RuntimeError(f"{sheet_name}.{c} is required non-null but has blank row(s).")

    print(f"  Rows: {len(pdf)}")
    return pdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Build `D_ReservationStatus`
# 
# **Plain words:** turn the seed sheet into the governed status dimension. The important
# column is `InventoryDeduct` — it decides which room-nights `F_RoomNights` keeps. Its values
# come only from the seed (D-190). We do not set them here.

# CELL ********************

# ---------------------------------------------------------------
# 4. Build D_ReservationStatus and write as Delta.
# ---------------------------------------------------------------
pdf_drs = build_dimension(
    seed[DRS_SHEET], DRS_SHEET,
    DRS_GOVERNED_COLS, DRS_KEY_COL, DRS_REQUIRED_COLS, DRS_BOOL_COLS, DRS_INT_COLS)

df_drs = spark.createDataFrame(pdf_drs)
# enforce true types on the columns F_RoomNights depends on
for c in DRS_BOOL_COLS:
    if c in df_drs.columns:
        df_drs = df_drs.withColumn(c, F.col(c).cast(BooleanType()))
for c in DRS_INT_COLS:
    if c in df_drs.columns:
        df_drs = df_drs.withColumn(c, F.col(c).cast(IntegerType()))

(df_drs.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_D_RESERVATIONSTATUS))
print(f"Wrote {TBL_D_RESERVATIONSTATUS} ({WRITE_MODE}).")
df_drs.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Build `D_Property`
# 
# **Plain words:** turn the seed sheet into the governed property dimension. `F_RoomNights`
# only checks that `PropertyKey` exists here, but we build the full governed column set that
# the seed provides so the table is complete for later work.

# CELL ********************

# ---------------------------------------------------------------
# 5. Build D_Property and write as Delta.
# ---------------------------------------------------------------
pdf_dpr = build_dimension(
    seed[DPR_SHEET], DPR_SHEET,
    DPR_GOVERNED_COLS, DPR_KEY_COL, DPR_REQUIRED_COLS, DPR_BOOL_COLS, DPR_INT_COLS)

df_dpr = spark.createDataFrame(pdf_dpr)
for c in DPR_BOOL_COLS:
    if c in df_dpr.columns:
        df_dpr = df_dpr.withColumn(c, F.col(c).cast(BooleanType()))
for c in DPR_INT_COLS:
    if c in df_dpr.columns:
        df_dpr = df_dpr.withColumn(c, F.col(c).cast(IntegerType()))

(df_dpr.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_D_PROPERTY))
print(f"Wrote {TBL_D_PROPERTY} ({WRITE_MODE}).")
df_dpr.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Validation — read both tables back and prove them
# 
# **Plain words:** every check below reads the **written** Delta table, so what you check is
# what actually landed. If any line prints FAIL, do not run `F_RoomNights` on top of these.
# 
# Checks:
# 1. Table exists and is readable.
# 2. Key column present, non-null, unique.
# 3. `D_ReservationStatus.InventoryDeduct` is a real boolean and non-null on every row.
# 4. Every governed status key resolves to a deduct value (this is exactly what `F_RoomNights`
#    will require of the keys it sees).

# CELL ********************

# ---------------------------------------------------------------
# 6. Read-back validation.
# ---------------------------------------------------------------
def validate_dim(table_name, key_col, bool_required=None):
    print(f"\n--- Validating {table_name} ---")
    t = spark.read.table(table_name)
    n = t.count()
    n_key_null = t.filter(F.col(key_col).isNull()).count()
    n_key_distinct = t.select(key_col).distinct().count()
    print(f"  Rows: {n}")
    print(f"  {key_col} NULLs: {n_key_null} -> {'OK' if n_key_null == 0 else 'FAIL'}")
    print(f"  {key_col} unique: {n_key_distinct}/{n} -> "
          f"{'OK' if n_key_distinct == n else 'FAIL'}")
    if bool_required:
        for c in bool_required:
            is_bool = dict(t.dtypes).get(c) == "boolean"
            n_null = t.filter(F.col(c).isNull()).count()
            print(f"  {c} is boolean: {is_bool} -> {'OK' if is_bool else 'FAIL'}")
            print(f"  {c} NULLs: {n_null} -> {'OK' if n_null == 0 else 'FAIL'}")
    return t


t_drs = validate_dim(TBL_D_RESERVATIONSTATUS, DRS_KEY_COL, bool_required=["InventoryDeduct"])
t_dpr = validate_dim(TBL_D_PROPERTY, DPR_KEY_COL)

print("\n--- D_ReservationStatus deduct summary (from seed, not set here) ---")
(t_drs.groupBy("InventoryDeduct").count().orderBy("InventoryDeduct").show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Run summary
# 
# **Plain words:** a short, honest summary of what landed. This is not a completion claim —
# it is only what this run produced. You confirm success by inspecting the output.

# CELL ********************

# ---------------------------------------------------------------
# 7. Run summary.
# ---------------------------------------------------------------
print("=" * 60)
print("Dimension build run summary (DRAFT — user must confirm)")
print("=" * 60)
print(f"Seed workbook: {SEED_XLSX_PATH}")
print(f"{TBL_D_RESERVATIONSTATUS}: {t_drs.count()} rows, columns: {t_drs.columns}")
print(f"{TBL_D_PROPERTY}: {t_dpr.count()} rows, columns: {t_dpr.columns}")
print("")
print("Next: F_RoomNights can be attempted once both tables above validated OK.")
print("Reminder: pause Fabric capacity fabaurorabiv1devf2 in Azure when done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

# ============================================================================
# CELL 1 — HEADER / GOVERNANCE REFERENCES
# ============================================================================
# Notebook : NB_Menja_Phase1_30_F_RoomNights_BUILD_DRAFT
# Project  : Menja BI v1 — Phase 1 Fabric build
# Status   : DRAFT — do not run before verifying the VERIFY-marked parameters
#            in CELL 2 against the Excel governance workbook.
#
# Scope (per D-208, FINAL):
#   BUILDS      : D_Date, F_RoomNights
#   READS ONLY  : I_RoomNights, D_Property, D_ReservationStatus
#   OUT OF SCOPE: D_RoomType, D_RatePlan, D_Channel, D_Segment,
#                 D_RevenueStream, D_Block, D_Event, all revenue/currency
#                 logic (I-196 OPEN), no-show treatment (I-186 OPEN),
#                 international calendar localization (I-197 OPEN).
#
# Governance references used:
#   D-208 : Phase-1 build slice scope + build mechanics for F_RoomNights.
#           F_RoomNights consumes built I_RoomNights as its ONLY input,
#           keeps every snapshot version (no latest-only filter),
#           includes a row only when its reservation status deducts
#           inventory, carries RoomNightID and IsDayUse unchanged,
#           and writes all ungoverned columns as NULL for this slice
#           (explicit override of NullableFlag = No for those columns).
#   D-190 : Status classification. Confirmed / Started / Processed deduct
#           inventory. Optional / Canceled / UNKNOWN do not.
#           Applied here ONLY via D_ReservationStatus.InventoryDeduct.
#           This notebook never infers behavior from raw status strings.
#   D-047 : D_Date build rules. ACTIVE columns C-029..C-035.
#   C-203 : "Current state" is resolved later in DAX measures using
#           MAX(SnapshotDateTime). NOT applied in this notebook.
#   R-005 : Model relationship F_RoomNights -> D_Date.
#   I-197 : OPEN — international week-start + local-language day/month
#           names. Future additive work. NOT built here.
#
# Role split: user owns governance, Fabric runs, and GitHub commits.
# This notebook fails hard on any missing input, missing column, or
# failed validation. It never invents fallbacks.
# ============================================================================


# ============================================================================
# CELL 2 — PARAMETERS
# ============================================================================
from datetime import date, timedelta

# --- Table names (Lakehouse Delta tables) -----------------------------------
TBL_I_ROOMNIGHTS        = "I_RoomNights"          # input fact (read-only)
TBL_D_PROPERTY          = "D_Property"            # existing dim (read-only)
TBL_D_RESERVATIONSTATUS = "D_ReservationStatus"   # existing dim (read-only)
TBL_D_DATE              = "D_Date"                # built by this notebook
TBL_F_ROOMNIGHTS        = "F_RoomNights"          # built by this notebook

# --- D_Date range configuration (D-047 / D-208) ------------------------------
# Start: earliest of (explicit start, min F_RoomNights.StayDate).
# End  : latest of (explicit end,   max F_RoomNights.StayDate + horizon).
D_DATE_EXPLICIT_START_DATE = None          # e.g. date(2024, 1, 1) or None
D_DATE_EXPLICIT_END_DATE   = None          # e.g. date(2028,12,31) or None
D_DATE_FORWARD_HORIZON_DAYS = 730          # VERIFY: governed forward horizon?

# --- Week convention (v1, single governed convention) ------------------------
# VERIFY against governance workbook. Assumed here:
#   ISO week start = Monday, WeekdayNumber 1=Monday .. 7=Sunday,
#   WeekdayName / MonthName in English (local languages deferred to I-197).
WEEK_START_ASSUMPTION = "ISO_MONDAY"

# --- ACTIVE D_Date columns C-029..C-035 (D-047) ------------------------------
# VERIFY: exact governed column names for C-029..C-035 were not included in
# the handoff packet. The 7 names below are a PLACEHOLDER standard set and
# MUST be replaced with the exact names from the governance workbook before
# this notebook is run. DateKey/Date/WeekdayNumber/WeekdayName are confirmed
# by the packet; the remaining three are assumed.
D_DATE_ACTIVE_COLUMNS = [
    "DateKey",        # yyyymmdd integer, 1:1 with Date (confirmed in packet)
    "Date",           # calendar date (confirmed in packet)
    "Year",           # ASSUMED — verify C-ID and name
    "MonthNumber",    # ASSUMED — verify C-ID and name
    "MonthName",      # ASSUMED — verify C-ID and name
    "WeekdayNumber",  # confirmed in packet (v1 convention)
    "WeekdayName",    # confirmed in packet (v1 convention)
]

# --- I_RoomNights columns this notebook depends on ---------------------------
# VERIFY names against the built I_RoomNights schema / governance workbook.
COL_ROOMNIGHT_ID   = "RoomNightID"
COL_STATUS_KEY     = "ReservationStatusKey"
COL_STAY_DATE      = "StayDate"
COL_IS_DAY_USE     = "IsDayUse"
COL_SNAPSHOT_DT    = "SnapshotDateTime"
COL_PROPERTY_KEY   = "PropertyKey"        # VERIFY exact name in I_RoomNights

REQUIRED_I_COLUMNS = [
    COL_ROOMNIGHT_ID, COL_STATUS_KEY, COL_STAY_DATE,
    COL_IS_DAY_USE, COL_SNAPSHOT_DT, COL_PROPERTY_KEY,
]

# Governed carried NOT-NULL fields (checked in Section 10).
GOVERNED_NOT_NULL_COLUMNS = REQUIRED_I_COLUMNS

# --- Ungoverned F_RoomNights columns written NULL under D-208 -----------------
# VERIFY: replace every placeholder name and Spark type below with the exact
# governed column names and data types from the governance workbook.
# D-208 explicitly overrides NullableFlag = No for these columns in this slice.
# Categories per D-208: revenue, currency, room-type / rate-plan / channel /
# segment keys, market country, block, event, revenue stream,
# revenue-state / revenue-derivation fields.
UNGOVERNED_NULL_COLUMNS = {
    # "governed column name" : "spark type string"
    "RoomRevenue":        "decimal(18,4)",   # PLACEHOLDER — VERIFY
    "CurrencyCode":       "string",          # PLACEHOLDER — VERIFY
    "RoomTypeKey":        "string",          # PLACEHOLDER — VERIFY
    "RatePlanKey":        "string",          # PLACEHOLDER — VERIFY
    "ChannelKey":         "string",          # PLACEHOLDER — VERIFY
    "SegmentKey":         "string",          # PLACEHOLDER — VERIFY
    "MarketCountry":      "string",          # PLACEHOLDER — VERIFY
    "BlockKey":           "string",          # PLACEHOLDER — VERIFY
    "EventKey":           "string",          # PLACEHOLDER — VERIFY
    "RevenueStreamKey":   "string",          # PLACEHOLDER — VERIFY
    "RevenueState":       "string",          # PLACEHOLDER — VERIFY
    "RevenueDerivation":  "string",          # PLACEHOLDER — VERIFY
}

# --- D_ReservationStatus dependency ------------------------------------------
COL_STATUS_DIM_KEY       = "ReservationStatusKey"  # VERIFY name in dimension
COL_INVENTORY_DEDUCT     = "InventoryDeduct"       # governed flag (D-190)

# --- D_Property dependency ----------------------------------------------------
COL_PROPERTY_DIM_KEY = "PropertyKey"               # VERIFY name in dimension


# ============================================================================
# CELL 3 — IMPORTS AND FAIL-FAST HELPERS
# ============================================================================
from pyspark.sql import functions as F
from pyspark.sql import types as T

RUN_LOG = []

def log(msg: str):
    line = f"[NB_30_F_RoomNights] {msg}"
    RUN_LOG.append(line)
    print(line)

def fail(msg: str):
    log(f"FAIL: {msg}")
    raise RuntimeError(f"BUILD FAILED — {msg}")

def require(condition: bool, msg: str):
    if not condition:
        fail(msg)


# ============================================================================
# CELL 4 — LOAD SOURCE DELTA TABLES (READ-ONLY)
# ============================================================================
def load_table(name: str):
    if not spark.catalog.tableExists(name):
        fail(f"Required input table '{name}' does not exist in the Lakehouse. "
             f"This notebook does not create or repair inputs.")
    df = spark.table(name)
    log(f"Loaded '{name}' — {df.count()} rows.")
    return df

df_i_roomnights = load_table(TBL_I_ROOMNIGHTS)
df_d_property   = load_table(TBL_D_PROPERTY)
df_d_status     = load_table(TBL_D_RESERVATIONSTATUS)


# ============================================================================
# CELL 5 — VALIDATE REQUIRED INPUTS (FAIL FAST, NO FALLBACKS)
# ============================================================================
# 5.1 Required columns in I_RoomNights
missing_i_cols = [c for c in REQUIRED_I_COLUMNS if c not in df_i_roomnights.columns]
require(not missing_i_cols,
        f"I_RoomNights is missing required governed columns: {missing_i_cols}. "
        f"No fallback is permitted — fix upstream or correct parameter names.")

# 5.2 Required columns in D_ReservationStatus
for c in [COL_STATUS_DIM_KEY, COL_INVENTORY_DEDUCT]:
    require(c in df_d_status.columns,
            f"D_ReservationStatus is missing column '{c}'. Per D-190/D-208 the "
            f"inventory filter may only come from this dimension. "
            f"Status behavior must NOT be inferred from raw strings.")

# 5.3 Required column in D_Property
require(COL_PROPERTY_DIM_KEY in df_d_property.columns,
        f"D_Property is missing column '{COL_PROPERTY_DIM_KEY}'.")

# 5.4 D_ReservationStatus completeness for the keys actually present in the fact.
#     Every status key seen in I_RoomNights must exist in the dimension with a
#     non-NULL InventoryDeduct flag. An unmapped or NULL-flagged key means the
#     dimension is incomplete -> fail rather than silently include or exclude.
unmapped = (
    df_i_roomnights.select(COL_STATUS_KEY).distinct()
    .join(
        df_d_status.select(
            F.col(COL_STATUS_DIM_KEY).alias(COL_STATUS_KEY),
            F.col(COL_INVENTORY_DEDUCT),
        ),
        on=COL_STATUS_KEY, how="left",
    )
    .where(F.col(COL_INVENTORY_DEDUCT).isNull())
)
unmapped_count = unmapped.count()
if unmapped_count > 0:
    unmapped.show(50, truncate=False)
    fail(f"{unmapped_count} distinct ReservationStatusKey value(s) in "
         f"I_RoomNights have no D_ReservationStatus row or a NULL "
         f"InventoryDeduct flag. Governance (D-190) does not permit guessing.")

# 5.5 RoomNightID must be unique in the INPUT before we rely on it as the grain.
i_total = df_i_roomnights.count()
i_distinct_ids = df_i_roomnights.select(COL_ROOMNIGHT_ID).distinct().count()
require(i_total == i_distinct_ids,
        f"RoomNightID is not unique in I_RoomNights "
        f"({i_total} rows vs {i_distinct_ids} distinct IDs). "
        f"F_RoomNights is defined as one row per qualifying I_RoomNights row "
        f"with RoomNightID carried unchanged, so input uniqueness is required. "
        f"If the governed grain is snapshot-aware beyond RoomNightID, that must "
        f"be resolved in governance first — do not proceed.")

log("Input validation passed: tables, columns, status mapping, input grain.")


# ============================================================================
# CELL 6 — BUILD D_DATE (D-047, ACTIVE COLUMNS C-029..C-035, R-005 TARGET)
# ============================================================================
# Range: must cover all F_RoomNights.StayDate values plus a forward horizon.
# F_RoomNights StayDates are a subset of I_RoomNights StayDates, so sizing the
# calendar from I_RoomNights guarantees coverage.
stay_bounds = df_i_roomnights.agg(
    F.min(COL_STAY_DATE).alias("min_stay"),
    F.max(COL_STAY_DATE).alias("max_stay"),
).collect()[0]

require(stay_bounds["min_stay"] is not None,
        "I_RoomNights contains no StayDate values; cannot size D_Date.")

min_stay = stay_bounds["min_stay"]
max_stay = stay_bounds["max_stay"]
if hasattr(min_stay, "date"):  # normalize timestamp -> date if needed
    min_stay = min_stay.date()
    max_stay = max_stay.date()

d_date_start = min(filter(None, [D_DATE_EXPLICIT_START_DATE, min_stay]))
d_date_end   = max(filter(None, [
    D_DATE_EXPLICIT_END_DATE,
    max_stay + timedelta(days=D_DATE_FORWARD_HORIZON_DAYS),
]))
log(f"D_Date range: {d_date_start} .. {d_date_end} "
    f"(stay dates {min_stay} .. {max_stay}, horizon "
    f"{D_DATE_FORWARD_HORIZON_DAYS} days).")

df_d_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{d_date_start}'), "
        f"to_date('{d_date_end}'), interval 1 day)) AS Date"
    )
    .withColumn("DateKey",
                (F.year("Date") * 10000
                 + F.month("Date") * 100
                 + F.dayofmonth("Date")).cast("int"))
    .withColumn("Year", F.year("Date").cast("int"))
    .withColumn("MonthNumber", F.month("Date").cast("int"))
    .withColumn("MonthName", F.date_format("Date", "MMMM"))       # English, v1
    # ISO weekday: Monday=1 .. Sunday=7 (Spark dayofweek: Sunday=1 .. Saturday=7)
    .withColumn("WeekdayNumber",
                (((F.dayofweek("Date") + 5) % 7) + 1).cast("int"))
    .withColumn("WeekdayName", F.date_format("Date", "EEEE"))     # English, v1
    .select(*D_DATE_ACTIVE_COLUMNS)
)
# NOTE: planned measure-only flags C-036/C-037 are intentionally NOT built.
# NOTE: international week-start and local-language names are I-197 (OPEN).


# ============================================================================
# CELL 7 — BUILD F_ROOMNIGHTS (D-208)
# ============================================================================
# Rules applied:
#   - Only input: built I_RoomNights.
#   - Include a row only when ReservationStatusKey maps to
#     D_ReservationStatus.InventoryDeduct = TRUE (per snapshot-version row).
#   - Keep every snapshot version. NO latest-only filter.
#   - RoomNightID and IsDayUse carried unchanged.
#   - All governed carried columns from I_RoomNights are carried 1:1.
#   - All ungoverned columns written as typed NULLs (D-208 override).
deducting_keys = (
    df_d_status
    .where(F.col(COL_INVENTORY_DEDUCT) == F.lit(True))
    .select(F.col(COL_STATUS_DIM_KEY).alias(COL_STATUS_KEY))
)

df_f_roomnights = df_i_roomnights.join(deducting_keys, on=COL_STATUS_KEY,
                                       how="inner")

# Expected row count, computed independently for the Section 10 check.
expected_f_rows = df_f_roomnights.count()

# Append ungoverned columns as typed NULLs.
for col_name, col_type in UNGOVERNED_NULL_COLUMNS.items():
    require(col_name not in df_f_roomnights.columns,
            f"Ungoverned column '{col_name}' already exists on the carried "
            f"I_RoomNights schema. D-208 requires it to be NULL in this slice; "
            f"resolve the conflict in governance before overwriting data.")
    df_f_roomnights = df_f_roomnights.withColumn(
        col_name, F.lit(None).cast(col_type))

log(f"F_RoomNights built in memory: {expected_f_rows} qualifying rows "
    f"(inventory-deducting statuses only), "
    f"{len(UNGOVERNED_NULL_COLUMNS)} ungoverned columns written NULL.")


# ============================================================================
# CELL 8 — WRITE OUTPUT TABLES
# ============================================================================
# Full deterministic rebuild each run: I_RoomNights is the only input, so
# overwrite is safe and repeatable for this slice.
(df_d_date.write.mode("overwrite")
 .option("overwriteSchema", "true")
 .format("delta").saveAsTable(TBL_D_DATE))
log(f"Wrote '{TBL_D_DATE}'.")

(df_f_roomnights.write.mode("overwrite")
 .option("overwriteSchema", "true")
 .format("delta").saveAsTable(TBL_F_ROOMNIGHTS))
log(f"Wrote '{TBL_F_ROOMNIGHTS}'.")

# Re-read what was actually written; all Section 10 checks run on disk state.
df_d_date_out = spark.table(TBL_D_DATE)
df_f_out      = spark.table(TBL_F_ROOMNIGHTS)


# ============================================================================
# CELL 9 — SECTION 10 VALIDATIONS
# ============================================================================
VALIDATION_RESULTS = []

def check(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    VALIDATION_RESULTS.append((name, status, detail))
    log(f"VALIDATION {status} — {name}" + (f" — {detail}" if detail else ""))
    if not passed:
        fail(f"Validation '{name}' failed. {detail}")

# ---- D_Date checks -----------------------------------------------------------
d_rows = df_d_date_out.count()

check("D_Date.DateKey uniqueness",
      df_d_date_out.select("DateKey").distinct().count() == d_rows,
      f"{d_rows} rows")

check("D_Date.Date uniqueness",
      df_d_date_out.select("Date").distinct().count() == d_rows)

bad_datekey = df_d_date_out.where(
    F.col("DateKey") != (F.year("Date") * 10000
                         + F.month("Date") * 100
                         + F.dayofmonth("Date"))
).count()
check("D_Date.DateKey equals yyyymmdd of Date", bad_datekey == 0)

null_expr = None
for c in D_DATE_ACTIVE_COLUMNS:
    e = F.col(c).isNull()
    null_expr = e if null_expr is None else (null_expr | e)
check("D_Date no NULLs in ACTIVE columns C-029..C-035",
      df_d_date_out.where(null_expr).count() == 0)

f_stay_bounds = df_f_out.agg(
    F.min(COL_STAY_DATE).alias("mn"), F.max(COL_STAY_DATE).alias("mx")
).collect()[0]
d_bounds = df_d_date_out.agg(
    F.min("Date").alias("mn"), F.max("Date").alias("mx")
).collect()[0]
check("D_Date range covers F_RoomNights StayDate min/max",
      d_bounds["mn"] <= f_stay_bounds["mn"]
      and d_bounds["mx"] >= f_stay_bounds["mx"],
      f"fact {f_stay_bounds['mn']}..{f_stay_bounds['mx']} within calendar "
      f"{d_bounds['mn']}..{d_bounds['mx']}")

orphan_staydates = (
    df_f_out.select(F.col(COL_STAY_DATE).cast("date").alias("Date")).distinct()
    .join(df_d_date_out.select("Date"), on="Date", how="left_anti").count()
)
check("Every F_RoomNights.StayDate has a matching D_Date.Date (R-005)",
      orphan_staydates == 0)

# ---- F_RoomNights checks ------------------------------------------------------
f_rows = df_f_out.count()

check("F_RoomNights row count equals qualifying I_RoomNights rows "
      "(InventoryDeduct = TRUE, per snapshot-version row; no latest-only filter)",
      f_rows == expected_f_rows,
      f"written {f_rows}, expected {expected_f_rows}")

# Grain: RoomNightID uniqueness. This is sufficient because (a) input
# uniqueness was proven in CELL 5, and (b) D-208 defines F_RoomNights as
# exactly one row per qualifying I_RoomNights row with RoomNightID unchanged.
# Snapshot history is therefore already encoded inside RoomNightID's grain.
check("F_RoomNights.RoomNightID uniqueness (governed grain)",
      df_f_out.select(COL_ROOMNIGHT_ID).distinct().count() == f_rows)

# Snapshot history preserved: more than one snapshot version may exist per
# reservation; assert the fact retains every distinct SnapshotDateTime that
# the qualifying input rows had.
i_snap = (df_i_roomnights.join(deducting_keys, on=COL_STATUS_KEY, how="inner")
          .select(COL_SNAPSHOT_DT).distinct().count())
f_snap = df_f_out.select(COL_SNAPSHOT_DT).distinct().count()
check("Snapshot history preserved (distinct SnapshotDateTime retained)",
      i_snap == f_snap, f"{f_snap} distinct snapshot times")

# All ungoverned columns are NULL.
for col_name in UNGOVERNED_NULL_COLUMNS:
    check(f"Ungoverned column '{col_name}' is entirely NULL (D-208 override)",
          df_f_out.where(F.col(col_name).isNotNull()).count() == 0)

# Governed carried not-null columns contain no NULLs.
for col_name in GOVERNED_NOT_NULL_COLUMNS:
    check(f"Governed carried column '{col_name}' has no NULLs",
          df_f_out.where(F.col(col_name).isNull()).count() == 0)

# Parent coverage.
check("Parent coverage: F_RoomNights -> D_Property",
      df_f_out.select(COL_PROPERTY_KEY).distinct()
      .join(df_d_property.select(
          F.col(COL_PROPERTY_DIM_KEY).alias(COL_PROPERTY_KEY)),
          on=COL_PROPERTY_KEY, how="left_anti").count() == 0)

check("Parent coverage: F_RoomNights -> D_ReservationStatus",
      df_f_out.select(COL_STATUS_KEY).distinct()
      .join(df_d_status.select(
          F.col(COL_STATUS_DIM_KEY).alias(COL_STATUS_KEY)),
          on=COL_STATUS_KEY, how="left_anti").count() == 0)
# (Parent coverage to D_Date already proven by the R-005 StayDate check above.)

# Day-use visibility (informational + assertion that they were not dropped).
day_use_in  = (df_i_roomnights.join(deducting_keys, on=COL_STATUS_KEY,
                                    how="inner")
               .where(F.col(COL_IS_DAY_USE) == F.lit(True)).count())
day_use_out = df_f_out.where(F.col(COL_IS_DAY_USE) == F.lit(True)).count()
check("Day-use rows remain visible when inventory-deducting",
      day_use_in == day_use_out,
      f"{day_use_out} day-use rows carried")


# ============================================================================
# CELL 10 — HUMAN-READABLE RUN SUMMARY
# ============================================================================
overnight_out = f_rows - day_use_out
print("=" * 76)
print("RUN SUMMARY — NB_Menja_Phase1_30_F_RoomNights_BUILD_DRAFT")
print("=" * 76)
print(f"Input  I_RoomNights rows            : {i_total}")
print(f"Qualifying rows (InventoryDeduct)   : {expected_f_rows}")
print(f"Excluded rows (non-deducting)       : {i_total - expected_f_rows}")
print(f"F_RoomNights rows written           : {f_rows}")
print(f"  of which overnight                : {overnight_out}")
print(f"  of which day-use                  : {day_use_out}")
print(f"Distinct snapshot versions retained : {f_snap}")
print(f"Ungoverned columns written NULL     : {len(UNGOVERNED_NULL_COLUMNS)}")
print(f"D_Date rows                         : {d_rows}")
print(f"D_Date range                        : {d_bounds['mn']} .. {d_bounds['mx']}")
print(f"Week convention (v1)                : {WEEK_START_ASSUMPTION} "
      f"(local languages deferred, I-197)")
print("-" * 76)
print("Validations:")
for name, status, detail in VALIDATION_RESULTS:
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
print("-" * 76)
print("Governance boundaries respected: D-208 slice only. No revenue (I-196),")
print("no no-show logic (I-186), no localization (I-197), no extra dimensions.")
print("=" * 76)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

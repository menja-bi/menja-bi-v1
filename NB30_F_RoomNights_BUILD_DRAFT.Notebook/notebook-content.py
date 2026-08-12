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

# # F_RoomNights + D_Date — BUILD_DRAFT — Phase-1 Mews slice + governed revenue carry
# 
# **Plain words:** this notebook reads the already-built `I_RoomNights` table and creates the
# first **gold-layer** tables: `F_RoomNights` (the fact table Power BI will read) and `D_Date`
# (the calendar table). It keeps **every** reservation status and resolves current state at
# build: each reservation's latest snapshot version enters the fact (D-209). It carries the
# six governed revenue columns unchanged from those rows and computes `RoomRevenue` from them
# (D-245).
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until the user
# runs it in Fabric and confirms the results.
# 
# **Governance basis (all FINAL):**
# 
# | Rule | Decision / source |
# |---|---|
# | Build slice scope: input = built `I_RoomNights` only; NULL override for remaining ungoverned columns; parameterized `D_Date` build with coverage guarantee | D-208 |
# | Row selection: keep **all** reservation statuses; resolve current state at build — keep each reservation's latest version (`MAX(SnapshotDateTime)` per `ReservationID`) | D-209 (supersedes the D-208/D-125 deduct row filter) |
# | Revenue carry: `CurrencyCode`, both revenue amounts, `RevenueState`, both derivation methods carried unchanged from the D-209-selected row; no aggregation, no re-derivation | D-245 (C-218, C-219, C-220, C-222, C-223, C-224) |
# | `RoomRevenue` (C-221): realized when not blank, otherwise booked, blank when both are blank — computed at build | D-245, under the D-125 precedence |
# | `F_RoomNights.RoomNightID` stays identical to `I_RoomNights.RoomNightID`; no separate key | C-198 / D-125 |
# | `IsDayUse` carried from `I_RoomNights`; overnight KPIs exclude it by default (later, in measures) | C-415 / D-206 / D-048 |
# | `D_Date` structure: 7 ACTIVE columns C-029..C-035; `DateKey` yyyymmdd integer, one-to-one with `Date`; Monday = 1 | D-047 / D-208 |
# | Governed F_RoomNights column contract: 33 columns | 03_Columns C-198..C-226, C-382, C-402, C-403, C-415 |
# 
# **Explicitly NOT in this notebook:** room-type / rate-plan / channel / segment lookups,
# block or event linkage (incl. `IsBlockPickupRoomNight`), `RevenueStreamKey`,
# `F_GroupBlockSnapshot`, the influence bridge, **measures**, no-show treatment (I-186),
# multi-room beyond `BookedRoomIndex = 1`, incremental logic, and international calendar
# conventions (I-197). The nine columns that remain ungoverned, parked or deferred are
# written as honest NULLs (see Section 6) — D-245 keeps the write-NULL override in force for
# exactly those nine.


# MARKDOWN ********************

# ## 1. Configuration
# 
# **Plain words:** one place for every name and value. `# <-- CONFIRM` marks values you should
# double-check before running.
# 
# The only values that need thought are the **`D_Date` calendar range**. D-208 governs a
# *configurable* start/end range that must cover every `F_RoomNights.StayDate` **plus a forward
# horizon** (so future on-the-books dates always have a calendar row). The notebook **fails
# loudly** if the range does not cover the facts — it never silently trims or extends. Sizing
# the forward horizon is your call; the defaults below give roughly 18 months beyond the
# current demo data.

# CELL ********************

# ---------------------------------------------------------------
# 1. Configuration.
# ---------------------------------------------------------------

# --- input: the built I_RoomNights table (D-208: the ONLY fact input) ---
SOURCE_TABLE = "I_RoomNights"

# --- dimensions that must already exist (built by NB_..._25_Dimensions) ---
TBL_D_RESERVATIONSTATUS = "D_ReservationStatus"   # deduct filter source (D-125 / D-190)
TBL_D_PROPERTY          = "D_Property"            # existence check only (D-208 minimum dims)

# --- output tables ---
TARGET_FACT = "F_RoomNights"
TARGET_DATE = "D_Date"

# --- D_Date calendar range (D-208: configurable, must cover all StayDates + horizon) ---
D_DATE_START = "2025-01-01"   # <-- CONFIRM: must be <= earliest F_RoomNights StayDate
D_DATE_END   = "2027-12-31"   # <-- CONFIRM: must be >= latest F_RoomNights StayDate + horizon

# --- write mode for this BUILD_DRAFT ---
# Full rebuild. Incremental/change-aware append logic is explicitly out of scope (D-208).
WRITE_MODE = "overwrite"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Environment guards
# 
# **Plain words:** two fail-loudly checks before touching data.
# 
# 1. The default lakehouse must be attached, otherwise nothing can be read or written.
# 2. The Spark session time zone must be **UTC**, the same setting the confirmed
#    `I_Reservations` and `I_RoomNights` builds ran under. This notebook does no timestamp
#    math of its own, but carried `SnapshotDateTime` / `BookingDateTime` values should be
#    read and displayed under the same convention they were written under.

# CELL ********************

# ---------------------------------------------------------------
# 2. Lakehouse attachment + UTC session guard — fail loudly.
# ---------------------------------------------------------------
import os

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"

if not os.path.isdir(LAKEHOUSE_FILES_ROOT):
    raise RuntimeError(
        "No default lakehouse attached. Attach LH_Menja_BI_v1_Mews_DEV to this "
        "notebook, then re-run this cell.")
print("Default lakehouse Files area is reachable:", LAKEHOUSE_FILES_ROOT)

session_tz = spark.conf.get("spark.sql.session.timeZone")
if session_tz != "UTC":
    raise RuntimeError(
        f"Spark session time zone is '{session_tz}', expected 'UTC'. "
        "Carried timestamps were written under UTC. Do not override silently — "
        "investigate before running.")
print("Session time zone OK: UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Prerequisite tables
# 
# **Plain words:** D-208 names the minimum tables that must already exist before the fact can
# be built: the input `I_RoomNights`, plus the two dimensions `D_ReservationStatus` (the
# deduct filter comes only from its `InventoryDeduct` column) and `D_Property` (presence
# check — the fact carries `PropertyKey` and the dimension must exist for the model). If any
# is missing, stop with a clear message. No fallback, no inline seed reading.

# CELL ********************

# ---------------------------------------------------------------
# 3. Prerequisite Delta tables — fail loudly if missing.
# ---------------------------------------------------------------
from pyspark.sql import functions as F

REQUIRED_TABLES = [SOURCE_TABLE, TBL_D_RESERVATIONSTATUS, TBL_D_PROPERTY]
for tname in REQUIRED_TABLES:
    if not spark.catalog.tableExists(tname):
        raise RuntimeError(
            f"Required table '{tname}' does not exist in the lakehouse. "
            f"Build order: I_RoomNights (NB_..._20), dimensions (NB_..._25), then this "
            f"notebook. Do not continue.")
    print(f"Prerequisite table OK: {tname}")

# D_ReservationStatus must expose the two columns the deduct filter depends on.
drs_cols = set(spark.read.table(TBL_D_RESERVATIONSTATUS).columns)
for c in ["ReservationStatusKey", "InventoryDeduct"]:
    if c not in drs_cols:
        raise RuntimeError(
            f"{TBL_D_RESERVATIONSTATUS} is missing required column '{c}'. "
            f"The D-125 deduct filter cannot run — fix the dimension build first.")
print("D_ReservationStatus exposes ReservationStatusKey + InventoryDeduct.")

n_prop = spark.read.table(TBL_D_PROPERTY).count()
if n_prop == 0:
    raise RuntimeError(f"{TBL_D_PROPERTY} exists but is empty — dimension build incomplete.")
print(f"D_Property present with {n_prop} row(s).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Read `I_RoomNights` and run input gates
# 
# **Plain words:** read every stored room-night version — no filtering yet. Hard checks first:
# 
# - `RoomNightID` must never be NULL and must be unique (it is the identity key the fact
#   reuses one-to-one, C-198).
# - Every column this fact carries must actually exist in the input.
# - The carried not-null columns must contain no NULLs on the input side, so any NULL found
#   later in the written fact could only be a bug here, not inherited dirt.

# CELL ********************

# ---------------------------------------------------------------
# 4. Read input + gates.
# ---------------------------------------------------------------
df_in = spark.read.table(SOURCE_TABLE)

n_in_rows = df_in.count()
print(f"I_RoomNights rows (all snapshot versions): {n_in_rows}")
if n_in_rows == 0:
    raise RuntimeError("I_RoomNights is empty — nothing to build. Stop.")

# The 23 columns this build carries (populated), per 03_Columns + D-208 + D-245.
# The six revenue columns joined the carry on 2026-08-11 under D-245.
CARRIED_COLS = [
    "RoomNightID", "ReservationID", "PropertyKey", "PropertyID", "StayDate",
    "SnapshotDateTime", "ReservationStatusKey", "IsGroupReservation", "BookingDateTime",
    "ArrivalDate", "DepartureDate", "LOS_Nights", "BookingWindowDays", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
    # D-245 revenue carry (nullable by governed rule, D-241 / D-242):
    "CurrencyCode", "BookedRoomRevenue_RoomNight", "RealizedRoomRevenue_RoomNight",
    "RevenueState", "BookedRevenueDerivationMethod", "RealizedRevenueDerivationMethod",
]
missing = [c for c in CARRIED_COLS if c not in df_in.columns]
if missing:
    raise RuntimeError(f"I_RoomNights is missing carried column(s) {missing}. "
                       "The input does not match the governed contract — stop. "
                       "(Revenue columns require the amended NB20 to have run first.)")
print("All 23 carried columns present in input.")

# Gate: RoomNightID not NULL + unique.
n_null_id = df_in.filter(F.col("RoomNightID").isNull()).count()
if n_null_id:
    raise RuntimeError(f"{n_null_id} input rows have NULL RoomNightID — identity broken.")
n_dup_id = df_in.groupBy("RoomNightID").count().filter("count > 1").count()
if n_dup_id:
    raise RuntimeError(f"{n_dup_id} duplicate RoomNightID values in input — "
                       "fix I_RoomNights before building the fact.")
print("RoomNightID null/uniqueness gates OK.")

# D-236 (FINAL): PropertyID is nullable and remains an unpopulated, traceability-only
# attribute. The six revenue columns are nullable BY GOVERNED RULE: blank when no
# qualifying revenue exists on the night (D-241) or when the property currency could
# not be resolved (D-242). All seven are therefore excluded from the not-null gate.
# PropertyKey remains the sole relationship key and stays fully enforced.
NULLABLE_CARRIED_COLS = [
    "PropertyID",
    "CurrencyCode", "BookedRoomRevenue_RoomNight", "RealizedRoomRevenue_RoomNight",
    "RevenueState", "BookedRevenueDerivationMethod", "RealizedRevenueDerivationMethod",
]
CARRIED_NOT_NULL_INPUT_COLS = [c for c in CARRIED_COLS if c not in NULLABLE_CARRIED_COLS]

# Gate: carried not-null columns clean on the input side.
for c in CARRIED_NOT_NULL_INPUT_COLS:
    n_null = df_in.filter(F.col(c).isNull()).count()
    if n_null:
        raise RuntimeError(f"Input column '{c}' has {n_null} NULLs but is governed "
                           "not-null on the fact. Fix upstream — no silent patching.")
print("Carried not-null columns (excl. PropertyID + revenue, D-236/D-241/D-242) are clean on input.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Current-state resolution + status resolve gate (D-209)
# 
# **Plain words:** two jobs. (1) Every reservation status is kept — Optional, Canceled and
# UNKNOWN rows now stay in the fact (D-209 removes the old deduct filter). (2) For each
# reservation we keep only its **latest version**: the rows whose `SnapshotDateTime` equals
# `MAX(SnapshotDateTime)` for that `ReservationID`. Older versions drop out completely.
# 
# Two safety rules kept from before:
# 
# - Every status key in the input **must resolve** to a `D_ReservationStatus` row. An
#   unmatched key would vanish through the join — that is silent row loss, so it is a hard stop.
# - `InventoryDeduct` is still joined, but only as a **diagnostic split** (how many current
#   rows deduct vs not). It no longer removes rows.
# 
# Why resolve at reservation grain, not on `IsLatestCurrent`: that flag is per room-night key,
# so a shortened or moved stay would leave its dropped nights flagged TRUE ("ghost nights").
# Taking MAX per `ReservationID` drops the whole superseded version, ghost nights included.


# CELL ********************

# ---------------------------------------------------------------
# 5. Current-state resolution (D-209) + status-key resolve gate.
#    D-209 SUPERSEDES the D-208/D-125 deduct row filter:
#      * keep ALL reservation statuses - deduct no longer removes rows
#      * keep the resolve gate - every input status key MUST match a
#        D_ReservationStatus row (unmatched = hard stop, never a silent drop)
#      * keep the deduct join only as a diagnostic column (visibility)
#      * resolve current state at BUILD, per RESERVATION: keep each
#        reservation's latest version (MAX(SnapshotDateTime) per ReservationID)
#
#    Variable names df_pass / n_pass / df_excluded / n_excluded are RETAINED so
#    sections 6 and 7 need no edit, but their meaning changes:
#      df_pass / n_pass          = current-state rows (enter the fact)
#      df_excluded / n_excluded  = superseded older-version rows (dropped)
# ---------------------------------------------------------------
from pyspark.sql.window import Window

df_drs = (spark.read.table(TBL_D_RESERVATIONSTATUS)
          .select("ReservationStatusKey",
                  F.col("InventoryDeduct").cast("boolean").alias("_InventoryDeduct")))

# Gate: dimension side must be unique + fully populated on the join columns.
n_dup_key = df_drs.groupBy("ReservationStatusKey").count().filter("count > 1").count()
if n_dup_key:
    raise RuntimeError("Duplicate ReservationStatusKey in D_ReservationStatus - "
                       "status join would be ambiguous.")
n_null_flag = df_drs.filter(F.col("_InventoryDeduct").isNull()).count()
if n_null_flag:
    raise RuntimeError(f"{n_null_flag} D_ReservationStatus rows have NULL/unreadable "
                       "InventoryDeduct - governance does not permit guessing this flag.")

# Gate: every input status key must resolve (left_anti finds the unmatched ones).
df_unmatched = (df_in.select("ReservationStatusKey").distinct()
                .join(df_drs, "ReservationStatusKey", "left_anti"))
unmatched = [r["ReservationStatusKey"] for r in df_unmatched.collect()]
if unmatched:
    raise RuntimeError(f"Status key(s) {unmatched} in I_RoomNights have no "
                       f"D_ReservationStatus row. Unresolvable keys must not be "
                       f"silently dropped - fix the seed/dimension first.")
print("All input ReservationStatusKey values resolve in D_ReservationStatus.")

# Guard: current-state resolution needs a usable SnapshotDateTime on every row.
n_null_snap = df_in.filter(F.col("SnapshotDateTime").isNull()).count()
if n_null_snap:
    raise RuntimeError(f"{n_null_snap} I_RoomNights rows have NULL SnapshotDateTime - "
                       "current-state resolution (D-209) cannot rank them. "
                       "Fix I_RoomNights first.")

# Attach the deduct flag for diagnostics only (D-209: it does NOT filter rows).
df_joined = df_in.join(df_drs, "ReservationStatusKey", "inner")

# --- Current-state resolution at RESERVATION grain (D-209) ---
# Partition by ReservationID so MAX(SnapshotDateTime) spans every night of every
# version of the reservation; keeping rows equal to that max keeps the whole latest
# version and drops every superseded version, including nights a later (shortened or
# moved) version no longer contains. Filtering on IsLatestCurrent instead would leave
# those nights behind, because that flag is defined per room-night key, not per version.
w_res = Window.partitionBy("ReservationID")
df_tagged   = df_joined.withColumn("_MaxSnap", F.max("SnapshotDateTime").over(w_res))
df_pass     = df_tagged.filter(F.col("SnapshotDateTime") == F.col("_MaxSnap")).drop("_MaxSnap")
df_excluded = df_tagged.filter(F.col("SnapshotDateTime") != F.col("_MaxSnap")).drop("_MaxSnap")

n_pass     = df_pass.count()       # current-state rows (enter the fact)
n_excluded = df_excluded.count()   # superseded older-version rows (dropped)

# Accounting: every input row is either current or superseded - nothing vanishes.
if n_pass + n_excluded != n_in_rows:
    raise RuntimeError("Row accounting broken: current + superseded != input. Stop.")

# Diagnostic deduct split on the CURRENT set (visibility only - deduct no longer filters).
n_ded_true    = df_pass.filter(F.col("_InventoryDeduct")).count()
n_ded_false   = df_pass.filter(~F.col("_InventoryDeduct")).count()
n_res_current = df_pass.select("ReservationID").distinct().count()
n_res_ded_t   = df_pass.filter(F.col("_InventoryDeduct")).select("ReservationID").distinct().count()

print(f"Input rows (all versions, all statuses):          {n_in_rows}")
print(f"Current-state rows entering fact (D-209):          {n_pass}")
print(f"Superseded older-version rows dropped:             {n_excluded}")
print(f"  current, InventoryDeduct = TRUE:   {n_ded_true}  ({n_res_ded_t} reservations)")
print(f"  current, InventoryDeduct = FALSE:  {n_ded_false}")
print(f"Distinct reservations in current-state fact:       {n_res_current}")

if n_pass == 0:
    raise RuntimeError(
        "0 current-state rows - the fact would be empty and the D_Date coverage rule "
        "(D-208) cannot be evaluated. Unexpected for the OSL slice; investigate "
        "I_RoomNights before continuing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Assemble the governed 33-column fact (D-208 NULL rule + D-245 revenue carry)
# 
# **Plain words:** the `F_RoomNights` contract has **33 columns** (`03_Columns`, C-198..C-226
# plus C-382, C-402, C-403, C-415). This build fills **24 columns**: the 17 original carries,
# the six revenue columns carried unchanged from the D-209-selected current rows (D-245), and
# `RoomRevenue`, computed at build as realized-else-booked under the D-125 precedence —
# realized when not blank, otherwise booked, blank when both are blank. `coalesce` implements
# exactly that rule; a realized value of 0 is a value, not a blank.
# 
# The **9 remaining columns are written as literal NULL** — no fallback, no placeholder, no
# UNKNOWN key. D-245 keeps the D-208 write-NULL override in force for exactly these nine:
# 
# - `RoomTypeKey`, `RatePlanKey`, `ChannelKey`, `SegmentKey` — lookups not governed
# - `MarketCountryKey` — upstream ungoverned
# - `BlockID`, `EventID`, `IsBlockPickupRoomNight` — block/event linkage deferred
# - `RevenueStreamKey` — remediation open
# 
# **One nuance — `IsLatestCurrent`:** carried as-is from `I_RoomNights` (C-226). Under D-209
# the fact already holds only each reservation's latest version. The flag is not recomputed
# here — that would be invented logic.
# 
# The physical column order below follows the `03_Columns` listing order.


# CELL ********************

# ---------------------------------------------------------------
# 6. Final projection: 33 columns — carried values + D-245 revenue + governed NULLs.
# ---------------------------------------------------------------
def null_str():   return F.lit(None).cast("string")
def null_dbl():   return F.lit(None).cast("double")
def null_bool():  return F.lit(None).cast("boolean")

df_fact = df_pass.select(
    # --- carried (populated) ---
    F.col("RoomNightID"),                                   # C-198  identity = I_RoomNights.RoomNightID
    F.col("ReservationID"),                                 # C-199
    F.col("PropertyKey"),                                   # C-200
    F.col("PropertyID"),                                    # C-201
    F.col("StayDate"),                                      # C-202  R-005 date key
    F.col("SnapshotDateTime"),                              # C-203  snapshot-aware
    # --- governed NULLs (D-208 override, retained by D-245) ---
    null_str().alias("RoomTypeKey"),                        # C-204
    null_str().alias("RatePlanKey"),                        # C-205
    null_str().alias("ChannelKey"),                         # C-206
    null_str().alias("SegmentKey"),                         # C-207
    # --- carried ---
    F.col("ReservationStatusKey"),                          # C-208
    # --- governed NULLs ---
    null_str().alias("MarketCountryKey"),                   # C-209
    null_str().alias("BlockID"),                            # C-210
    null_str().alias("EventID"),                            # C-211
    # --- carried ---
    F.col("IsGroupReservation"),                            # C-212  D-200 carry
    F.col("BookingDateTime"),                               # C-213
    F.col("ArrivalDate"),                                   # C-214
    F.col("DepartureDate"),                                 # C-215
    F.col("LOS_Nights"),                                    # C-216
    F.col("BookingWindowDays"),                             # C-217
    # --- D-245 revenue carry: unchanged from the D-209-selected row ---
    F.col("CurrencyCode"),                                  # C-218  D-242 via carry
    F.col("BookedRoomRevenue_RoomNight"),                   # C-219  carry
    F.col("RealizedRoomRevenue_RoomNight"),                 # C-220  carry
    # --- D-245 computed at build: realized else booked, blank when both blank (D-125) ---
    F.coalesce(F.col("RealizedRoomRevenue_RoomNight"),
               F.col("BookedRoomRevenue_RoomNight")).alias("RoomRevenue"),  # C-221
    F.col("RevenueState"),                                  # C-222  carry (D-241 values)
    F.col("BookedRevenueDerivationMethod"),                 # C-223  carry
    F.col("RealizedRevenueDerivationMethod"),               # C-224  carry
    # --- governed NULL ---
    null_bool().alias("IsBlockPickupRoomNight"),            # C-225 (D-080, deferred)
    # --- carried ---
    F.col("IsLatestCurrent"),                               # C-226  carried as-is (see note)
    # --- governed NULL ---
    null_str().alias("RevenueStreamKey"),                   # C-382
    # --- carried ---
    F.col("TenantKey"),                                     # C-402
    F.col("TenantID"),                                      # C-403
    F.col("IsDayUse"),                                      # C-415  D-206 carry
)

n_fact = df_fact.count()
print(f"F_RoomNights rows to write: {n_fact}")
print(f"Column count:               {len(df_fact.columns)}  (expected 33)")
if len(df_fact.columns) != 33:
    raise RuntimeError("Column count is not 33 — projection does not match the contract.")
if n_fact != n_pass:
    raise RuntimeError("Row count changed during projection — must be one fact row per "
                       "qualifying I_RoomNights row (D-208).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write `F_RoomNights`
# 
# **Plain words:** save the fact as a Delta table with `overwrite` (full rebuild — safe
# because `I_RoomNights` is retained and this table can always be rebuilt from it).
# Incremental append logic is explicitly out of scope under D-208.

# CELL ********************

# ---------------------------------------------------------------
# 7. Write the fact as Delta.
# ---------------------------------------------------------------
(df_fact.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_FACT))
print(f"Wrote {TARGET_FACT} ({WRITE_MODE}): {n_fact} rows, 33 columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Build `D_Date` (D-047 / D-208)
# 
# **Plain words:** one row per calendar date across the configured range, with exactly the
# **7 ACTIVE governed columns** (C-029..C-035). Nothing else — the PLANNED measure-only flags
# (C-036 `IsToDate`, C-037 `IsLastTwelveMonths`) are *not* built, per D-208.
# 
# | Column | Rule (governed) |
# |---|---|
# | `DateKey` | yyyymmdd whole number, one-to-one with `Date` (D-208) |
# | `Date` | one row per calendar date (C-030) |
# | `Year` | derived directly from `Date` (C-031) |
# | `MonthNumber` | 1..12, sorts `MonthName` (C-032) |
# | `MonthName` | English month name, sorted by `MonthNumber` (C-033) |
# | `WeekdayNumber` | **Monday = 1 .. Sunday = 7** (ISO 8601) — the v1 week-start convention confirmed FINAL in D-208 Notes | 
# | `WeekdayName` | English weekday name, sorted by `WeekdayNumber` (C-035) |
# 
# Names are produced by **explicit mappings**, not locale-dependent formatting, so the output
# is deterministic on any cluster. Local-language names and alternative week starts are future
# *additive* work (I-197) and are intentionally not here.
# 
# **Coverage guarantee (D-208):** before anything is written, the configured range must cover
# the earliest and latest `StayDate` in the fact. If not, the notebook **fails** and tells you
# which config value to change. It never trims or extends the range on its own.


# CELL ********************

# ---------------------------------------------------------------
# 8a. Coverage gate — the configured range must cover all fact StayDates.
# ---------------------------------------------------------------
import datetime

start_d = datetime.date.fromisoformat(D_DATE_START)
end_d   = datetime.date.fromisoformat(D_DATE_END)
if start_d > end_d:
    raise RuntimeError(f"D_DATE_START {D_DATE_START} is after D_DATE_END {D_DATE_END}.")

stay_bounds = df_fact.agg(F.min("StayDate").alias("min_stay"),
                          F.max("StayDate").alias("max_stay")).collect()[0]
min_stay, max_stay = stay_bounds["min_stay"], stay_bounds["max_stay"]
print(f"Fact StayDate range: {min_stay} .. {max_stay}")
print(f"Configured D_Date range: {start_d} .. {end_d}")

if min_stay < start_d:
    raise RuntimeError(f"D_DATE_START {start_d} is after the earliest StayDate {min_stay}. "
                       f"Lower D_DATE_START in Section 1 — the build must not drop coverage.")
if max_stay > end_d:
    raise RuntimeError(f"D_DATE_END {end_d} is before the latest StayDate {max_stay}. "
                       f"Raise D_DATE_END in Section 1 — the build must not drop coverage.")

horizon_days = (end_d - max_stay).days
print(f"Coverage OK. Forward horizon beyond latest StayDate: {horizon_days} days.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 8b. Generate D_Date — 7 governed columns, deterministic mappings.
# ---------------------------------------------------------------
MONTH_NAMES = {1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
               7:"July",8:"August",9:"September",10:"October",11:"November",12:"December"}
WEEKDAY_NAMES = {1:"Monday",2:"Tuesday",3:"Wednesday",4:"Thursday",
                 5:"Friday",6:"Saturday",7:"Sunday"}

month_map   = F.create_map([F.lit(x) for kv in MONTH_NAMES.items()   for x in kv])
weekday_map = F.create_map([F.lit(x) for kv in WEEKDAY_NAMES.items() for x in kv])

df_date = (spark.sql(
        f"SELECT explode(sequence(to_date('{D_DATE_START}'), "
        f"to_date('{D_DATE_END}'), interval 1 day)) AS Date")
    .withColumn("DateKey",
                (F.year("Date") * 10000 + F.month("Date") * 100
                 + F.dayofmonth("Date")).cast("int"))                    # C-029 yyyymmdd
    .withColumn("Year", F.year("Date").cast("int"))                     # C-031
    .withColumn("MonthNumber", F.month("Date").cast("int"))             # C-032
    .withColumn("MonthName", month_map[F.col("MonthNumber")])           # C-033
    # ISO weekday: Spark dayofweek() has Sunday=1; shift so Monday=1..Sunday=7 (D-208 Notes).
    .withColumn("WeekdayNumber",
                (((F.dayofweek("Date") + 5) % 7) + 1).cast("int"))      # C-034
    .withColumn("WeekdayName", weekday_map[F.col("WeekdayNumber")])     # C-035
    .select("DateKey", "Date", "Year", "MonthNumber", "MonthName",
            "WeekdayNumber", "WeekdayName")
)

n_dates = df_date.count()
expected_dates = (end_d - start_d).days + 1
print(f"D_Date rows generated: {n_dates} (expected {expected_dates})")
if n_dates != expected_dates:
    raise RuntimeError("Date sequence has gaps or duplicates — generation broken.")
if len(df_date.columns) != 7:
    raise RuntimeError("D_Date must have exactly the 7 ACTIVE governed columns (D-208).")

(df_date.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_DATE))
print(f"Wrote {TARGET_DATE} ({WRITE_MODE}): {n_dates} rows, 7 columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Validation — read the written tables back and prove it
# 
# **Plain words:** every check below reads the **written** tables, so what you check is what
# actually landed. If any line prints **FAIL**, do not build anything on top of these tables.
# 
# **Fact checks:**
# 
# 1. **Row funnel** - input versions = current-state rows + superseded rows; no row vanishes.
# 2. **Identity** — `RoomNightID` unique in the fact and identical to `I_RoomNights` (C-198):
#    every fact key exists upstream, one-to-one.
# 3. **Column contract** — exactly 33 columns; the 16 ungoverned columns 100% NULL; the 17
#    carried columns 0% NULL.
# 4. **Deduct split** - joining the written fact back to `D_ReservationStatus` shows the
#    `InventoryDeduct` TRUE/FALSE counts (visibility). Under D-209, FALSE rows are expected.
# 5. **Carry integrity** — `IsDayUse` consistency; `IsLatestCurrent` equals the upstream value
#    per `RoomNightID`; at most one TRUE per `ReservationID + StayDate` group (the slice has
#    `BookedRoomIndex = 1` everywhere, so this grouping equals the governed room-night group).
# 
# **D_Date checks:**
# 
# 6. `DateKey` unique, correct yyyymmdd form, one-to-one with `Date`; sequence has no gaps.
# 7. Week-start spot check: a known Monday must have `WeekdayNumber = 1`.
# 8. **Coverage guarantee (D-208 / R-005):** every distinct fact `StayDate` has a matching
#    `D_Date.Date` row — zero misses allowed.


# CELL ********************

# ---------------------------------------------------------------
# 9a. Fact — row funnel + identity vs I_RoomNights.
# ---------------------------------------------------------------
t_fact = spark.read.table(TARGET_FACT)
t_in   = spark.read.table(SOURCE_TABLE)

n_written = t_fact.count()
print(f"Fact rows written: {n_written} (expected {n_pass}) -> "
      f"{'OK' if n_written == n_pass else 'FAIL'}")
print(f"Funnel: input {n_in_rows} = fact {n_written} + excluded {n_excluded} -> "
      f"{'OK' if n_in_rows == n_written + n_excluded else 'FAIL'}")

n_dup_fact_id = t_fact.groupBy("RoomNightID").count().filter("count > 1").count()
print(f"Duplicate RoomNightID in fact: {n_dup_fact_id} -> "
      f"{'OK' if n_dup_fact_id == 0 else 'FAIL'}")

# Every fact key must exist upstream (identity, C-198).
n_orphan = t_fact.select("RoomNightID").join(
    t_in.select("RoomNightID"), "RoomNightID", "left_anti").count()
print(f"Fact RoomNightIDs missing upstream: {n_orphan} -> "
      f"{'OK' if n_orphan == 0 else 'FAIL'}")

# Completeness: every deduct-TRUE upstream row must be in the fact.
df_drs_chk = (spark.read.table(TBL_D_RESERVATIONSTATUS)
              .select("ReservationStatusKey",
                      F.col("InventoryDeduct").cast("boolean").alias("_ded")))
t_in_deduct = (t_in.join(df_drs_chk, "ReservationStatusKey", "inner")
                   .filter(F.col("_ded")))
n_missing = t_in_deduct.select("RoomNightID").join(
    t_fact.select("RoomNightID"), "RoomNightID", "left_anti").count()
print(f"Deduct-TRUE upstream rows missing from fact: {n_missing} -> "
      f"{'OK' if n_missing == 0 else 'FAIL'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 9b. Fact — column contract: 33 columns, NULL profile.
# ---------------------------------------------------------------
print(f"Fact column count: {len(t_fact.columns)} -> "
      f"{'OK' if len(t_fact.columns) == 33 else 'FAIL'}")

# The seven revenue columns left this list on 2026-08-11 under D-245.
# Their governed-rule checks live in Section 9e.
UNGOVERNED_NULL_COLS = [
    "RoomTypeKey", "RatePlanKey", "ChannelKey", "SegmentKey", "MarketCountryKey",
    "BlockID", "EventID", "IsBlockPickupRoomNight", "RevenueStreamKey",
]
CARRIED_NOT_NULL_COLS = [
    "RoomNightID", "ReservationID", "PropertyKey", "StayDate",
    "SnapshotDateTime", "ReservationStatusKey", "IsGroupReservation", "BookingDateTime",
    "ArrivalDate", "DepartureDate", "LOS_Nights", "BookingWindowDays", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
]
# D-236 (FINAL): PropertyID (C-201) is nullable and checked separately below.
# The seven revenue columns (C-218..C-224) are nullable BY GOVERNED RULE
# (D-241 blank-when-no-revenue, D-242 blank-on-unresolved-currency) and are
# validated against those rules in Section 9e, not against a NULL profile here.

all_ok = True
for c in UNGOVERNED_NULL_COLS:
    n_not_null = t_fact.filter(F.col(c).isNotNull()).count()
    ok = n_not_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_not_null} non-NULL values -> FAIL (must be 100% NULL, D-208/D-245)")
print(f"100% NULL on all 9 ungoverned columns -> {'OK' if all_ok else 'FAIL'}")

all_ok = True
for c in CARRIED_NOT_NULL_COLS:
    n_null = t_fact.filter(F.col(c).isNull()).count()
    ok = n_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_null} NULL values -> FAIL (governed not-null)")
print(f"0% NULL on all 16 carried not-null columns -> {'OK' if all_ok else 'FAIL'}")

# D-236 (FINAL): PropertyID (C-201) expected to remain 100% NULL for now.
n_propertyid_not_null = t_fact.filter(F.col("PropertyID").isNotNull()).count()
print(f"  PropertyID: {n_propertyid_not_null} non-NULL values -> "
      f"{'OK' if n_propertyid_not_null == 0 else 'FAIL'} (D-236: expected 100% NULL for now)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 9c. Fact — deduct proof + carry integrity.
# ---------------------------------------------------------------
# Deduct split on the WRITTEN table (D-209: all statuses retained; deduct is diagnostic, not a filter).
t_fact_ded    = t_fact.join(df_drs_chk, "ReservationStatusKey", "left")
n_ded_true_w  = t_fact_ded.filter(F.col("_ded") == True).count()
n_ded_false_w = t_fact_ded.filter((F.col("_ded").isNull()) | (~F.col("_ded"))).count()
print(f"Fact rows InventoryDeduct=TRUE:  {n_ded_true_w}")
print(f"Fact rows InventoryDeduct=FALSE: {n_ded_false_w}")
print(f"Deduct split covers all fact rows: "
      f"{'OK' if n_ded_true_w + n_ded_false_w == n_written else 'FAIL'}")

# IsDayUse consistency (carried, must still hold).
n_bad_du = t_fact.filter(
    (F.col("IsDayUse") != (F.col("ArrivalDate") == F.col("DepartureDate")))).count()
print(f"IsDayUse flag vs dates mismatches: {n_bad_du} -> "
      f"{'OK' if n_bad_du == 0 else 'FAIL'}")

# IsLatestCurrent must equal the upstream value per RoomNightID (carried as-is).
df_cmp = (t_fact.select("RoomNightID", F.col("IsLatestCurrent").alias("_f"))
          .join(t_in.select("RoomNightID", F.col("IsLatestCurrent").alias("_i")),
                "RoomNightID", "inner"))
n_flag_diff = df_cmp.filter(F.col("_f") != F.col("_i")).count()
print(f"IsLatestCurrent values differing from I_RoomNights: {n_flag_diff} -> "
      f"{'OK' if n_flag_diff == 0 else 'FAIL'}")

# At most one TRUE per room-night group. (BookedRoomIndex = 1 everywhere in this slice,
# so ReservationID + StayDate equals the governed group. Zero TRUEs is legitimate when
# the newest snapshot of a group is non-deducting — see Section 6 note.)
n_multi_true = (t_fact.filter("IsLatestCurrent")
                .groupBy("ReservationID", "StayDate")
                .count().filter("count > 1").count())
print(f"Groups with more than one IsLatestCurrent=TRUE: {n_multi_true} -> "
      f"{'OK' if n_multi_true == 0 else 'FAIL'}")

print("Latest-current day-use / overnight split (visibility):")
t_fact.filter("IsLatestCurrent").groupBy("IsDayUse").count().show(truncate=False)
print("Fact rows by status (visibility):")
t_fact.groupBy("ReservationStatusKey").count().orderBy("ReservationStatusKey") \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 9d. D_Date — structure, week start, and the R-005 coverage guarantee.
# ---------------------------------------------------------------
t_date = spark.read.table(TARGET_DATE)

n_rows_d = t_date.count()
print(f"D_Date rows: {n_rows_d} (expected {expected_dates}) -> "
      f"{'OK' if n_rows_d == expected_dates else 'FAIL'}")
print(f"D_Date column count: {len(t_date.columns)} -> "
      f"{'OK' if len(t_date.columns) == 7 else 'FAIL'}")

n_dup_dk = t_date.groupBy("DateKey").count().filter("count > 1").count()
n_dup_dt = t_date.groupBy("Date").count().filter("count > 1").count()
print(f"Duplicate DateKey: {n_dup_dk} -> {'OK' if n_dup_dk == 0 else 'FAIL'}")
print(f"Duplicate Date:    {n_dup_dt} -> {'OK' if n_dup_dt == 0 else 'FAIL'}")

# DateKey must equal yyyymmdd of Date on every row (one-to-one, D-208).
n_bad_dk = t_date.filter(
    F.col("DateKey") != (F.year("Date")*10000 + F.month("Date")*100
                         + F.dayofmonth("Date"))).count()
print(f"DateKey not matching yyyymmdd(Date): {n_bad_dk} -> "
      f"{'OK' if n_bad_dk == 0 else 'FAIL'}")

# No gaps: distinct dates must equal the full span.
n_distinct_dates = t_date.select("Date").distinct().count()
print(f"Distinct dates: {n_distinct_dates} (expected {expected_dates}) -> "
      f"{'OK' if n_distinct_dates == expected_dates else 'FAIL'}")

# Week-start spot check (D-208 Notes: Monday = 1). 2026-07-13 is a Monday.
spot = t_date.filter(F.col("Date") == F.lit("2026-07-13")) \
             .select("WeekdayNumber", "WeekdayName").collect()
if spot:
    ok = spot[0]["WeekdayNumber"] == 1 and spot[0]["WeekdayName"] == "Monday"
    print(f"Spot check 2026-07-13 -> WeekdayNumber={spot[0]['WeekdayNumber']}, "
          f"WeekdayName={spot[0]['WeekdayName']} -> {'OK' if ok else 'FAIL'}")
else:
    print("Spot-check date 2026-07-13 outside configured range — CHECK manually "
          "that a known Monday has WeekdayNumber = 1.")

# Coverage guarantee (D-208 / R-005): every fact StayDate must exist in D_Date.Date.
n_uncovered = (t_fact.select(F.col("StayDate").alias("Date")).distinct()
               .join(t_date.select("Date"), "Date", "left_anti").count())
print(f"Fact StayDates without a D_Date row: {n_uncovered} -> "
      f"{'OK' if n_uncovered == 0 else 'FAIL'}")
if n_uncovered:
    raise RuntimeError("Coverage guarantee violated (D-208) — widen the D_Date range "
                       "in Section 1 and re-run. Do not use these tables downstream.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9e. Revenue carry validation (NEW — D-245 / D-241 / D-242)
# 
# **Plain words:** prove, on the **written** fact, that the carry did exactly what D-245
# governs and nothing else:
# 
# 1. **Carry equality:** each of the six carried revenue values equals its upstream
#    `I_RoomNights` value on the same `RoomNightID` — null-safe, so a governed blank must
#    stay blank. Zero differences allowed; any difference means re-derivation happened.
# 2. **`RoomRevenue` rule:** realized when realized is not blank, otherwise booked, blank
#    when both are blank. Zero violations allowed.
# 3. **Value lists and pairing (D-241)** and **currency coherence (D-242)** re-checked on
#    the fact, so a carry bug cannot silently break them downstream.
# 4. **Visibility, not a check:** revenue split by `InventoryDeduct`. Under I-186 (OPEN), a
#    charged no-show maps to `Canceled` (`InventoryDeduct` FALSE) while D-209 keeps its room
#    nights in the fact — so its revenue drops out of every deduct-filtered figure and
#    appears in every unfiltered one. Printed so the numbers are read correctly.


# CELL ********************

# ---------------------------------------------------------------
# 9e. Revenue carry checks (D-245 / D-241 / D-242).
# ---------------------------------------------------------------
REV_CARRY_COLS = ["CurrencyCode", "BookedRoomRevenue_RoomNight",
                  "RealizedRoomRevenue_RoomNight", "RevenueState",
                  "BookedRevenueDerivationMethod", "RealizedRevenueDerivationMethod"]

# 1. Carry equality per RoomNightID (null-safe; carried unchanged means exact equality).
_f = t_fact.select(["RoomNightID"] + [F.col(c).alias(f"_f_{c}") for c in REV_CARRY_COLS])
_i = t_in.select(["RoomNightID"] + [F.col(c).alias(f"_i_{c}") for c in REV_CARRY_COLS])
_cmp = _f.join(_i, "RoomNightID", "inner")
all_ok = True
for c in REV_CARRY_COLS:
    n_diff = _cmp.filter(~F.col(f"_f_{c}").eqNullSafe(F.col(f"_i_{c}"))).count()
    ok = n_diff == 0
    all_ok = all_ok and ok
    print(f"Carry equality {c}: {n_diff} differences -> {'OK' if ok else 'FAIL'}")
print(f"Six-column carry unchanged (D-245) -> {'OK' if all_ok else 'FAIL'}")

# 2. RoomRevenue = realized else booked, blank when both blank (D-245 / D-125).
n_rr_bad = t_fact.filter(
    (F.col("RealizedRoomRevenue_RoomNight").isNotNull() &
     ~F.col("RoomRevenue").eqNullSafe(F.col("RealizedRoomRevenue_RoomNight"))) |
    (F.col("RealizedRoomRevenue_RoomNight").isNull() &
     F.col("BookedRoomRevenue_RoomNight").isNotNull() &
     ~F.col("RoomRevenue").eqNullSafe(F.col("BookedRoomRevenue_RoomNight"))) |
    (F.col("RealizedRoomRevenue_RoomNight").isNull() &
     F.col("BookedRoomRevenue_RoomNight").isNull() &
     F.col("RoomRevenue").isNotNull())).count()
print(f"RoomRevenue precedence violations: {n_rr_bad} -> {'OK' if n_rr_bad == 0 else 'FAIL'}")

# 3. D-241 value lists + pairing, and D-242 coherence, on the fact.
n_bad_state = t_fact.filter(F.col("RevenueState").isNotNull() &
    ~F.col("RevenueState").isin(["BOOKED", "REALIZED"])).count()
n_bad_meth = t_fact.filter(
    (F.col("BookedRevenueDerivationMethod").isNotNull() &
     (F.col("BookedRevenueDerivationMethod") != "CONSUMED")) |
    (F.col("RealizedRevenueDerivationMethod").isNotNull() &
     (F.col("RealizedRevenueDerivationMethod") != "CONSUMED"))).count()
n_pair = t_fact.filter(
    (F.col("BookedRoomRevenue_RoomNight").isNotNull() !=
     F.col("BookedRevenueDerivationMethod").isNotNull()) |
    (F.col("RealizedRoomRevenue_RoomNight").isNotNull() !=
     F.col("RealizedRevenueDerivationMethod").isNotNull()) |
    (F.col("RevenueState").isNotNull() !=
     (F.col("BookedRoomRevenue_RoomNight").isNotNull() |
      F.col("RealizedRoomRevenue_RoomNight").isNotNull()))).count()
n_ccy = t_fact.filter(F.col("CurrencyCode").isNull() &
    (F.col("BookedRoomRevenue_RoomNight").isNotNull() |
     F.col("RealizedRoomRevenue_RoomNight").isNotNull() |
     F.col("RoomRevenue").isNotNull() |
     F.col("RevenueState").isNotNull())).count()
print(f"Value-list violations:        {n_bad_state + n_bad_meth} -> "
      f"{'OK' if n_bad_state + n_bad_meth == 0 else 'FAIL'}")
print(f"Pairing violations:           {n_pair} -> {'OK' if n_pair == 0 else 'FAIL'}")
print(f"Currency coherence breaks:    {n_ccy} -> {'OK' if n_ccy == 0 else 'FAIL'}")

# 4. Visibility: revenue by deduct flag (I-186 context, not a check).
print("\nRevenue by InventoryDeduct (I-186: no-show revenue sits on non-deducting "
      "Canceled rows — present in unfiltered figures, absent from deduct-filtered ones):")
(t_fact.join(df_drs_chk, "ReservationStatusKey", "left")
    .groupBy("_ded")
    .agg(F.count(F.when(F.col("RevenueState").isNotNull(), 1)).alias("RevenueRows"),
         F.sum("BookedRoomRevenue_RoomNight").alias("BookedSum"),
         F.sum("RealizedRoomRevenue_RoomNight").alias("RealizedSum"),
         F.sum("RoomRevenue").alias("RoomRevenueSum"))
    .show(truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Wrap-up — honest status
# 
# **Plain words:** when this notebook has run, `F_RoomNights` and `D_Date` exist in the DEV
# lakehouse as Delta tables, with the seven D-245 revenue columns populated — nothing more.
# This is **not** done, committed, or governed-complete until:
# 
# 1. You confirm the run and inspect the Section 9 results, including the new 9e revenue
#    carry checks.
# 2. You commit the notebook to GitHub (`menja-bi/menja-bi-v1`) if you want it versioned.
# 3. Any follow-up documentation is handed to Copilot.
# 
# **Not created here, by design:** the R-005 relationship (semantic model), measures — no
# revenue, ADR or RevPAR measure exists yet and none is created here — and everything on the
# D-208 exclusion list.
# 
# **Out of scope, still blocked, unchanged:** room-type / rate-plan / channel / segment
# lookups, block/event linkage (incl. `IsBlockPickupRoomNight`, I-101), `RevenueStreamKey`,
# no-show treatment (I-186), version handling in measures (I-198), international calendar
# conventions (I-197), multi-room mechanics, incremental loads.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working, to avoid
# unnecessary cost.**


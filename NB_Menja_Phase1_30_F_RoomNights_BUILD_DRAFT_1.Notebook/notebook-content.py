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

# # F_RoomNights + D_Date — BUILD_DRAFT — Phase-1 Mews slice
# 
# **Plain words:** this notebook reads the already-built `I_RoomNights` table and creates the
# first **gold-layer** tables: `F_RoomNights` (the fact table Power BI will read) and `D_Date`
# (the calendar table). It keeps only the room-nights that actually take a room out of
# inventory (Confirmed / Started / Processed), and it keeps **every stored snapshot version**
# of those room-nights, so history stays visible.
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until the user
# runs it in Fabric and confirms the results.
# 
# **Governance basis (all FINAL, validated against the uploaded `Menja_Schema_Governance_0626.xlsx` on 2026-07-14):**
# 
# | Rule | Decision / source |
# |---|---|
# | Build slice scope: input = built `I_RoomNights` only; carry all snapshot versions; NULL override for ungoverned columns; parameterized `D_Date` build with coverage guarantee | D-208 (FINAL, locked 14.07.2026) |
# | Inventory-deduct row filter: keep a row only when `ReservationStatusKey` maps to `InventoryDeduct = TRUE` in `D_ReservationStatus`; applied per snapshot-version row | D-125 / C-208, classification per D-190 |
# | Snapshot-aware fact; current state resolved later in measures as `MAX(SnapshotDateTime)` | D-143 / D-049 / C-203 |
# | `F_RoomNights.RoomNightID` stays identical to `I_RoomNights.RoomNightID`; no separate key | C-198 / D-125 |
# | `IsDayUse` carried from `I_RoomNights`; overnight KPIs exclude it by default (later, in measures) | C-415 / D-206 / D-048 |
# | `D_Date` structure: 7 ACTIVE columns C-029..C-035; `DateKey` yyyymmdd integer, one-to-one with `Date` | D-047 / D-208 |
# | Week-start convention for v1: **Monday = 1** (ISO 8601) | D-208 Notes, confirmed FINAL 2026-07-14 |
# | `F_RoomNights.StayDate` → `D_Date.Date` relationship (semantic model); this notebook guarantees date **coverage** only | R-005 / D-125 |
# | Governed F_RoomNights column contract: 33 columns | 03_Columns C-198..C-226, C-382, C-402, C-403, C-415 |
# 
# **Explicitly NOT in this notebook (per D-208 out-of-scope):** revenue population or allocation,
# `CurrencyCode` sourcing, room-type / rate-plan / channel / segment lookups, block or event
# linkage, `F_GroupBlockSnapshot`, the room-night influence bridge, **measures** (including the
# PLANNED `D_Date` measure-only flags C-036 / C-037), no-show treatment (I-186), multi-room
# beyond `BookedRoomIndex = 1`, incremental/change-aware append logic, and international /
# localized calendar conventions (alternative week starts, local-language names, week numbering,
# fiscal calendars — future additive work under I-197). Those columns are written as honest
# NULLs (see Section 6) or simply not built.


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

# The 17 columns this slice carries (populated), per 03_Columns + D-208.
CARRIED_COLS = [
    "RoomNightID", "ReservationID", "PropertyKey", "PropertyID", "StayDate",
    "SnapshotDateTime", "ReservationStatusKey", "IsGroupReservation", "BookingDateTime",
    "ArrivalDate", "DepartureDate", "LOS_Nights", "BookingWindowDays", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
]
missing = [c for c in CARRIED_COLS if c not in df_in.columns]
if missing:
    raise RuntimeError(f"I_RoomNights is missing carried column(s) {missing}. "
                       "The input does not match the governed contract — stop.")
print("All 17 carried columns present in input.")

# Gate: RoomNightID not NULL + unique.
n_null_id = df_in.filter(F.col("RoomNightID").isNull()).count()
if n_null_id:
    raise RuntimeError(f"{n_null_id} input rows have NULL RoomNightID — identity broken.")
n_dup_id = df_in.groupBy("RoomNightID").count().filter("count > 1").count()
if n_dup_id:
    raise RuntimeError(f"{n_dup_id} duplicate RoomNightID values in input — "
                       "fix I_RoomNights before building the fact.")
print("RoomNightID null/uniqueness gates OK.")

# Gate: carried not-null columns clean on the input side.
for c in CARRIED_COLS:
    n_null = df_in.filter(F.col(c).isNull()).count()
    if n_null:
        raise RuntimeError(f"Input column '{c}' has {n_null} NULLs but is governed "
                           "not-null on the fact. Fix upstream — no silent patching.")
print("Carried not-null columns are clean on input.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Inventory-deduct filter (D-125 / D-190 / D-208)
# 
# **Plain words:** a room-night belongs in the fact only if its reservation status **takes a
# room out of inventory**. Which statuses do that is decided *only* by the
# `D_ReservationStatus.InventoryDeduct` flag (from the governed seed): Confirmed / Started /
# Processed deduct; Optional / Canceled / UNKNOWN do not. The filter is applied **per
# snapshot-version row**, so a reservation that was Confirmed and later Canceled keeps its
# Confirmed-era versions in the fact and simply has no Canceled-era versions there.
# 
# Two safety rules:
# 
# - Every status key in the input **must resolve** to a dimension row. An unmatched key would
#   otherwise vanish through the join — that would be silent row loss, so it is a hard stop.
# - Excluded rows are **counted and shown by status**, so nothing disappears without a trace.
#   Exclusion here is *correct governed behaviour*, not a data-quality problem, so there is no
#   exception table for it.

# CELL ********************

# ---------------------------------------------------------------
# 5. Deduct filter with resolve gate + visible exclusions.
# ---------------------------------------------------------------
df_drs = (spark.read.table(TBL_D_RESERVATIONSTATUS)
          .select("ReservationStatusKey",
                  F.col("InventoryDeduct").cast("boolean").alias("_InventoryDeduct")))

# Gate: dimension side must be unique + fully populated on the filter columns.
n_dup_key = df_drs.groupBy("ReservationStatusKey").count().filter("count > 1").count()
if n_dup_key:
    raise RuntimeError("Duplicate ReservationStatusKey in D_ReservationStatus — "
                       "deduct join would be ambiguous.")
n_null_flag = df_drs.filter(F.col("_InventoryDeduct").isNull()).count()
if n_null_flag:
    raise RuntimeError(f"{n_null_flag} D_ReservationStatus rows have NULL/unreadable "
                       "InventoryDeduct — governance does not permit guessing this flag.")

# Gate: every input status key must resolve (left_anti finds the unmatched ones).
df_unmatched = (df_in.select("ReservationStatusKey").distinct()
                .join(df_drs, "ReservationStatusKey", "left_anti"))
unmatched = [r["ReservationStatusKey"] for r in df_unmatched.collect()]
if unmatched:
    raise RuntimeError(f"Status key(s) {unmatched} in I_RoomNights have no "
                       f"D_ReservationStatus row. Unresolvable keys must not be "
                       f"silently dropped — fix the seed/dimension first.")
print("All input ReservationStatusKey values resolve in D_ReservationStatus.")

df_joined = df_in.join(df_drs, "ReservationStatusKey", "inner")

df_pass     = df_joined.filter(F.col("_InventoryDeduct"))
df_excluded = df_joined.filter(~F.col("_InventoryDeduct"))

n_pass     = df_pass.count()
n_excluded = df_excluded.count()
print(f"Rows passing deduct filter (enter F_RoomNights): {n_pass}")
print(f"Rows excluded (non-deducting status):            {n_excluded}")
if n_pass + n_excluded != n_in_rows:
    raise RuntimeError("Filter accounting broken: pass + excluded != input. Stop.")

print("Excluded rows by status (visibility only — governed behaviour, not an error):")
df_excluded.groupBy("ReservationStatusKey").count().orderBy("ReservationStatusKey") \
    .show(truncate=False)

if n_pass == 0:
    raise RuntimeError(
        "0 rows pass the deduct filter — the fact would be empty and the D_Date "
        "coverage rule (D-208) cannot be evaluated. This is unexpected for the OSL "
        "slice; investigate the D_ReservationStatus seed before continuing.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Assemble the governed 33-column fact (D-208 NULL rule)
# 
# **Plain words:** the `F_RoomNights` contract has **33 columns** (`03_Columns`, C-198..C-226
# plus C-382, C-402, C-403, C-415). This slice fills the **17 carried columns** straight from
# `I_RoomNights` and writes the **16 ungoverned columns as literal NULL** — no fallback, no
# placeholder, no UNKNOWN key, no guessed lookup. NULL means, honestly: "not governed / not
# implemented in this slice".
# 
# Columns intentionally NULL, with the blocking issue:
# 
# - `CurrencyCode`, `BookedRoomRevenue_RoomNight`, `RealizedRoomRevenue_RoomNight`,
#   `RoomRevenue`, `RevenueState`, `BookedRevenueDerivationMethod`,
#   `RealizedRevenueDerivationMethod`, `RevenueStreamKey` — revenue/currency allocation open (I-196)
# - `RoomTypeKey`, `RatePlanKey`, `ChannelKey`, `SegmentKey` — lookups not governed
# - `MarketCountryKey` — upstream ungoverned
# - `BlockID`, `EventID`, `IsBlockPickupRoomNight` — block/event linkage deferred
# 
# **Transparency note:** several of these are marked not-null in `03_Columns`. **D-208
# explicitly overrides `NullableFlag = No` for these columns for this slice.** The not-null
# contract applies when they are actually built later.
# 
# **One nuance worth understanding — `IsLatestCurrent`:** the flag is **carried as-is** from
# `I_RoomNights` (C-226). Because the deduct filter removes non-deducting versions, a
# room-night whose *newest* snapshot is, say, Canceled will have **no** `IsLatestCurrent = TRUE`
# row inside `F_RoomNights` — its TRUE row was filtered out. That is correct governed
# behaviour, not a bug: it means "this room-night no longer deducts inventory in its current
# state". Current-state analysis is resolved later in measures via `MAX(SnapshotDateTime)`
# (C-203). This notebook must not recompute the flag — that would be invented logic.
# 
# The physical column order below follows the `03_Columns` listing order.


# CELL ********************

# ---------------------------------------------------------------
# 6. Final projection: 33 columns — carried values + governed NULLs.
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
    # --- governed NULLs (D-208 override) ---
    null_str().alias("RoomTypeKey"),                        # C-204
    null_str().alias("RatePlanKey"),                        # C-205
    null_str().alias("ChannelKey"),                         # C-206
    null_str().alias("SegmentKey"),                         # C-207
    # --- carried ---
    F.col("ReservationStatusKey"),                          # C-208  deduct filter key
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
    # --- governed NULLs (I-196) ---
    null_str().alias("CurrencyCode"),                       # C-218
    null_dbl().alias("BookedRoomRevenue_RoomNight"),        # C-219
    null_dbl().alias("RealizedRoomRevenue_RoomNight"),      # C-220
    null_dbl().alias("RoomRevenue"),                        # C-221 (derived later, NULL now)
    null_str().alias("RevenueState"),                       # C-222
    null_str().alias("BookedRevenueDerivationMethod"),      # C-223
    null_str().alias("RealizedRevenueDerivationMethod"),    # C-224
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
# 1. **Row funnel** — input versions = fact rows + excluded rows; every deduct-TRUE input row
#    is in the fact; no non-deduct row leaked in.
# 2. **Identity** — `RoomNightID` unique in the fact and identical to `I_RoomNights` (C-198):
#    every fact key exists upstream, one-to-one.
# 3. **Column contract** — exactly 33 columns; the 16 ungoverned columns 100% NULL; the 17
#    carried columns 0% NULL.
# 4. **Deduct proof** — joining the written fact back to `D_ReservationStatus` shows
#    `InventoryDeduct = TRUE` on every row.
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

UNGOVERNED_NULL_COLS = [
    "RoomTypeKey", "RatePlanKey", "ChannelKey", "SegmentKey", "MarketCountryKey",
    "BlockID", "EventID", "CurrencyCode", "BookedRoomRevenue_RoomNight",
    "RealizedRoomRevenue_RoomNight", "RoomRevenue", "RevenueState",
    "BookedRevenueDerivationMethod", "RealizedRevenueDerivationMethod",
    "IsBlockPickupRoomNight", "RevenueStreamKey",
]
CARRIED_NOT_NULL_COLS = [
    "RoomNightID", "ReservationID", "PropertyKey", "PropertyID", "StayDate",
    "SnapshotDateTime", "ReservationStatusKey", "IsGroupReservation", "BookingDateTime",
    "ArrivalDate", "DepartureDate", "LOS_Nights", "BookingWindowDays", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
]

all_ok = True
for c in UNGOVERNED_NULL_COLS:
    n_not_null = t_fact.filter(F.col(c).isNotNull()).count()
    ok = n_not_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_not_null} non-NULL values -> FAIL (must be 100% NULL, D-208)")
print(f"100% NULL on all 16 ungoverned columns -> {'OK' if all_ok else 'FAIL'}")

all_ok = True
for c in CARRIED_NOT_NULL_COLS:
    n_null = t_fact.filter(F.col(c).isNull()).count()
    ok = n_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_null} NULL values -> FAIL (governed not-null)")
print(f"0% NULL on all 17 carried columns -> {'OK' if all_ok else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 9c. Fact — deduct proof + carry integrity.
# ---------------------------------------------------------------
# Deduct proof on the WRITTEN table.
n_bad_deduct = (t_fact.join(df_drs_chk, "ReservationStatusKey", "left")
                      .filter((F.col("_ded").isNull()) | (~F.col("_ded"))).count())
print(f"Fact rows whose status is not InventoryDeduct=TRUE: {n_bad_deduct} -> "
      f"{'OK' if n_bad_deduct == 0 else 'FAIL'}")

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

# ## 10. Wrap-up — honest status
# 
# **Plain words:** when this notebook has run, `F_RoomNights` and `D_Date` exist in the DEV
# lakehouse as Delta tables — nothing more. This is **not** done, committed, or
# governed-complete until:
# 
# 1. You confirm the run and inspect the Section 9 results (paste them into the session if
#    you want them documented).
# 2. You commit the notebook to GitHub (`menja-bi/menja-bi-v1`) if you want it versioned.
# 3. Any follow-up documentation is handed to Copilot.
# 
# **Not created here, by design:** the R-005 relationship itself (that lives in the semantic
# model, not the lakehouse — this notebook only guarantees the date coverage behind it),
# measures, and everything on the D-208 exclusion list.
# 
# **Out of scope, still blocked, unchanged:** revenue and `CurrencyCode` (I-196), room-type /
# rate-plan / channel / segment lookups, block/event linkage (incl. `IsBlockPickupRoomNight`,
# I-101), no-show treatment (I-186), international calendar conventions (I-197), multi-room
# mechanics, incremental loads.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working, to avoid
# unnecessary cost.**


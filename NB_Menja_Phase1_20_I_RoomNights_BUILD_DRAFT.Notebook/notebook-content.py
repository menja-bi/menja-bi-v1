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

# # I_RoomNights — BUILD_DRAFT — Phase-1 Mews slice
# 
# **Plain words:** this notebook takes the already-built `I_Reservations` table and turns every
# stored reservation version into one row **per room, per night** (plus one row per same-day
# day-use stay). `I_RoomNights` is the room-night ingestion table — the step between the
# reservation history and the future `F_RoomNights` fact.
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until the user
# runs it in Fabric and confirms the results.
# 
# **Governance basis (all FINAL):**
# 
# | Rule | Decision / binding |
# |---|---|
# | Same-day day-use row creation and `IsDayUse` flag | D-206 |
# | Build slice scope, input = built `I_Reservations` only, invalid-date routing, `RoomNightID` format, `BookingWindowDays` date basis | D-207 |
# | Grain `ReservationID + SnapshotDateTime + StayDate + BookedRoomIndex` | D-124 / LIN_MEWS_I_ROOMNIGHTS_001 |
# | `BookedRooms = 1` in this slice, `BookedRoomIndex` sequence rule | D-199 / BND-RN-013 |
# | `IsLatestCurrent` per `ReservationID + StayDate + BookedRoomIndex` | D-143 / BND-RN-041 |
# | Field-by-field population | 44 `BND-RN-*` rows in `10_I_Field_Bindings` (25 GOVERNED_FINAL, 19 UNGOVERNED → NULL) |
# | Seed workbook consumption | D-198 |
# 
# **Explicitly NOT in this notebook (per D-207 out-of-scope):** revenue population or allocation,
# `CurrencyCode` sourcing, room-type / rate-plan / channel / segment lookups, block or event
# linkage, `F_RoomNights`, measures, multi-room mechanics beyond `BookedRoomIndex = 1`.
# Those columns are written as honest NULLs (see Section 8).


# MARKDOWN ********************

# ## 1. Configuration
# 
# **Plain words:** one place for every name and path. `# <-- CONFIRM` marks values you should
# double-check before running. Table names for the target and the exception output are
# implementation details (not governed) and mirror the `I_Reservations` naming convention.

# CELL ********************

# ---------------------------------------------------------------
# 1. Configuration.
# ---------------------------------------------------------------

# --- input: the built I_Reservations table (D-207: the ONLY input) ---
SOURCE_TABLE = "I_Reservations"

# --- output tables ---
TARGET_TABLE = "I_RoomNights"
DQ_TABLE     = "I_RoomNights_DQ_Exceptions"   # naming is implementation detail, not governed

# --- seed workbook (D-198): runtime copy in lakehouse Files ---
# Needed ONLY for D_Property.TimeZone (BookingWindowDays local-date basis per D-207).
SEED_XLSX_PATH    = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"  # <-- CONFIRM
SEED_SHEETS_NEEDED = ["D_Property"]

# --- constants ---
SOURCE_SYSTEM = "MEWS"          # BND-RN-017: CONSTANT binding

# --- write mode for this BUILD_DRAFT ---
# Full rebuild from the retained I_Reservations table. Incremental logic is a later,
# separately governed step — not this draft.
WRITE_MODE = "overwrite"

# --- RoomNightID rendering of SnapshotDateTime (D-207: "ISO 8601 as stored") ---
# I_Reservations stores SnapshotDateTime as a Spark timestamp (microsecond precision).
# This pattern renders that stored value losslessly and deterministically.
SNAPSHOT_ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"   # <-- CONFIRM rendering precision


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1b. Environment guards
# 
# **Plain words:** two fail-loudly checks before touching data.
# 
# 1. The default lakehouse must be attached, otherwise nothing can be read or written.
# 2. The Spark session time zone must be **UTC**. Both the ISO rendering of
#    `SnapshotDateTime` inside `RoomNightID` and the `BookingDateTime` → property-local
#    conversion assume timestamps are stored and displayed as UTC instants — the same
#    assumption the confirmed `I_Reservations` build ran under. If the session is not UTC,
#    stop instead of producing silently shifted values.

# CELL ********************

# ---------------------------------------------------------------
# 1b. Lakehouse attachment + UTC session guard — fail loudly.
# ---------------------------------------------------------------
import os

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"

if not os.path.isdir(LAKEHOUSE_FILES_ROOT):
    raise RuntimeError(
        "No default lakehouse attached. Attach LH_Menja_BI_v1_Mews_DEV to this "
        "notebook, then re-run this cell."
    )
print("Default lakehouse Files area is reachable:", LAKEHOUSE_FILES_ROOT)

session_tz = spark.conf.get("spark.sql.session.timeZone")
if session_tz != "UTC":
    raise RuntimeError(
        f"Spark session time zone is '{session_tz}', expected 'UTC'. "
        "RoomNightID rendering and BookingWindowDays depend on UTC-stored timestamps. "
        "Do not override silently — investigate before running."
    )
print("Session time zone OK: UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Read the input and profile it
# 
# **Plain words:** read the built `I_Reservations` table — every stored version, with **no
# status filter** (BND-RN-037: cancelled versions stay visible at the I-layer; the
# inventory-deduct filter belongs to `F_RoomNights` later).
# 
# Hard checks, per governance:
# 
# - `ReservationID` and `SnapshotDateTime` must never be NULL (they are half the grain).
# - The input must be unique per `ReservationID + SnapshotDateTime` (the parent grain, D-199).
# - `BookedRooms` must equal `1` on every row. D-199 governs `BookedRooms = 1` for this slice
#   and D-207 keeps multi-room mechanics out of scope. The general BND-RN-013 rule is a
#   sequence `1..BookedRooms`; with `BookedRooms = 1` that sequence is exactly `{1}`. If a
#   row ever shows something else, this notebook must **stop**, not quietly fan out rows
#   whose mechanics are not yet governed.

# CELL ********************

# ---------------------------------------------------------------
# 2. Read I_Reservations and run input gates.
# ---------------------------------------------------------------
from pyspark.sql import functions as F, Window

df_in = spark.read.table(SOURCE_TABLE)

n_in_rows = df_in.count()
n_in_res  = df_in.select("ReservationID").distinct().count()
print(f"Input rows (reservation versions): {n_in_rows}")
print(f"Distinct reservations:             {n_in_res}")

# Gate 1: identity columns not NULL.
for c in ["ReservationID", "SnapshotDateTime"]:
    n_null = df_in.filter(F.col(c).isNull()).count()
    if n_null:
        raise RuntimeError(f"{n_null} input rows have NULL {c} — grain cannot be built. "
                           "Fix I_Reservations first.")
print("Identity null checks OK.")

# Gate 2: parent grain uniqueness (one row per ReservationID + SnapshotDateTime).
n_dup_parent = (df_in.groupBy("ReservationID", "SnapshotDateTime")
                     .count().filter("count > 1").count())
if n_dup_parent:
    raise RuntimeError(f"{n_dup_parent} duplicate ReservationID + SnapshotDateTime keys in "
                       f"{SOURCE_TABLE} — fix upstream before expanding to room nights.")
print("Parent grain uniqueness OK.")

# Gate 3: BookedRooms must be 1 in this slice (D-199 / D-207 scope boundary).
n_not_one = df_in.filter(
    F.col("BookedRooms").isNull() | (F.col("BookedRooms") != 1)).count()
if n_not_one:
    raise RuntimeError(f"{n_not_one} input rows have BookedRooms <> 1. Multi-room mechanics "
                       "are out of scope under D-207 — stop, do not fan out ungoverned rows.")
print("BookedRooms = 1 check OK (slice boundary respected).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Load the `D_Property` seed — time zone only (D-198)
# 
# **Plain words:** the only lookup this notebook is allowed is the property **time zone**,
# needed for one governed field: `BookingWindowDays` (BND-RN-014, D-207). The rule is
# "`ArrivalDate` minus the property-local calendar date of `BookingDateTime`", using the same
# UTC → local-first-then-date mechanics that D-193 already established for arrival/departure.
# 
# D-198 rules applied literally: explicit allow-list, `_`-prefixed sheets never read, seed is
# runtime input — not authority. The readiness gate requires `PropertyKey` and `TimeZone`
# filled on every seed row; the join later must resolve a time zone for **every** room-night
# row or the notebook stops — no guessed time zones.

# CELL ********************

# ---------------------------------------------------------------
# 3. Seed loading with explicit allow-list (D-198) + readiness gate.
# ---------------------------------------------------------------
import pandas as pd

if not os.path.isfile(SEED_XLSX_PATH):
    raise RuntimeError(f"Seed workbook not found at {SEED_XLSX_PATH}. "
                       "Upload the runtime copy to the lakehouse Files area first.")

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

prop_pdf = seed["D_Property"]
required_prop_cols = ["PropertyKey", "TimeZone"]
missing_cols = [c for c in required_prop_cols if c not in prop_pdf.columns]
if missing_cols:
    raise RuntimeError(f"D_Property seed is missing required columns {missing_cols}.")

blank = prop_pdf[required_prop_cols].isna().any(axis=1)
if blank.any():
    raise RuntimeError("D_Property seed readiness FAILED — rows with blank "
                       "PropertyKey / TimeZone:\n" + prop_pdf[blank].to_string())

dup = prop_pdf["PropertyKey"].duplicated()
if dup.any():
    raise RuntimeError("Duplicate PropertyKey values in D_Property seed — "
                       "time-zone join would be ambiguous:\n" + prop_pdf[dup].to_string())

df_seed_tz = spark.createDataFrame(
    prop_pdf[required_prop_cols]).withColumnRenamed("TimeZone", "_TimeZone")
print("D_Property time-zone seed OK:", prop_pdf.shape[0], "properties.")
df_seed_tz.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Classify every reservation version (D-206 / D-207)
# 
# **Plain words:** every stored version falls into exactly one of three buckets:
# 
# | Bucket | Rule | What happens |
# |---|---|---|
# | **Overnight** | `ArrivalDate < DepartureDate` | one row per night, `IsDayUse = FALSE` |
# | **Day-use** | `ArrivalDate = DepartureDate` | exactly one row, `StayDate = ArrivalDate`, `IsDayUse = TRUE` |
# | **Invalid** | `ArrivalDate > DepartureDate`, or either date NULL | **zero** room-night rows, routed to the exception table — never silently corrected |
# 
# `ArrivalDate` and `DepartureDate` are consumed exactly as stored (they are already
# property-local dates from the D-193 conversion in `I_Reservations`) and are **never
# re-derived from UTC** here (D-207, BND-RN-026/027).
# 
# Note: the confirmed OSL `I_Reservations` build already stops on bad or missing stay dates,
# so the invalid bucket is expected to be **0** today. The routing still has to exist,
# because D-207 demands it and future inputs are not guaranteed to be this clean.


# CELL ********************

# ---------------------------------------------------------------
# 4. Three-way classification of reservation versions.
# ---------------------------------------------------------------
is_invalid  = (F.col("ArrivalDate").isNull() |
               F.col("DepartureDate").isNull() |
               (F.col("ArrivalDate") > F.col("DepartureDate")))
is_dayuse   = (F.col("ArrivalDate") == F.col("DepartureDate"))
is_overnight= (F.col("ArrivalDate") <  F.col("DepartureDate"))

df_invalid   = df_in.filter(is_invalid)
df_dayuse    = df_in.filter(~is_invalid & is_dayuse)
df_overnight = df_in.filter(~is_invalid & is_overnight)

n_invalid   = df_invalid.count()
n_dayuse    = df_dayuse.count()
n_overnight = df_overnight.count()

print(f"Reservation versions classified:")
print(f"  Overnight (Arrival < Departure): {n_overnight}")
print(f"  Day-use   (Arrival = Departure): {n_dayuse}")
print(f"  Invalid   (bad/NULL dates):      {n_invalid}  -> exception table, zero output rows")

if n_overnight + n_dayuse + n_invalid != n_in_rows:
    raise RuntimeError("Classification buckets do not add up to the input row count — "
                       "logic error, stop.")
print("Classification arithmetic OK.")

# Expected output volumes, computed BEFORE any expansion (used by Section 10 validation):
expected_overnight_rows = (df_overnight
    .select(F.datediff("DepartureDate", "ArrivalDate").alias("_nights"))
    .agg(F.coalesce(F.sum("_nights"), F.lit(0)).alias("s")).collect()[0]["s"])
expected_dayuse_rows = n_dayuse
print(f"Expected overnight room-night rows (sum of nights): {expected_overnight_rows}")
print(f"Expected day-use rows (one per version):            {expected_dayuse_rows}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Expand to room-night rows (D-206)
# 
# **Plain words:**
# 
# - **Overnight:** build the list of dates from `ArrivalDate` up to `DepartureDate - 1`
#   (departure day is not a night) and make one row per date. `sequence()` builds that
#   date list; `explode()` turns the list into rows.
# - **Day-use:** no date list needed — exactly one row with `StayDate = ArrivalDate`.
# 
# `IsDayUse` is set per D-206 / BND-RN-044: `TRUE` only for the same-day rows.

# CELL ********************

# ---------------------------------------------------------------
# 5. Overnight explosion + day-use single rows, then union.
# ---------------------------------------------------------------
df_on_rows = (df_overnight
    .withColumn("StayDate",
        F.explode(F.sequence(F.col("ArrivalDate"), F.date_sub(F.col("DepartureDate"), 1))))
    .withColumn("IsDayUse", F.lit(False)))

df_du_rows = (df_dayuse
    .withColumn("StayDate", F.col("ArrivalDate"))
    .withColumn("IsDayUse", F.lit(True)))

df_rows = df_on_rows.unionByName(df_du_rows)

n_on_rows = df_on_rows.count()
n_du_rows = df_du_rows.count()
print(f"Overnight room-night rows generated: {n_on_rows} "
      f"(expected {expected_overnight_rows}) -> "
      f"{'OK' if n_on_rows == expected_overnight_rows else 'FAIL'}")
print(f"Day-use rows generated:              {n_du_rows} "
      f"(expected {expected_dayuse_rows}) -> "
      f"{'OK' if n_du_rows == expected_dayuse_rows else 'FAIL'}")
if n_on_rows != expected_overnight_rows or n_du_rows != expected_dayuse_rows:
    raise RuntimeError("Expansion produced a different row count than the pre-computed "
                       "expectation — stop and inspect.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Governed derived fields
# 
# **Plain words:** four derived columns, each with its exact governed rule:
# 
# | Column | Rule | Binding |
# |---|---|---|
# | `BookedRoomIndex` | sequence `1..BookedRooms`; `BookedRooms = 1` was hard-checked in Section 2, so the value is literally `1` | BND-RN-013 (D-199) |
# | `LOS_Nights` | `DepartureDate - ArrivalDate` in whole days — automatically `0` for day-use, **never forced to 1** | BND-RN-011 (D-125/D-206) |
# | `BookingWindowDays` | `ArrivalDate` minus the **property-local calendar date** of `BookingDateTime` (UTC → property time zone first, then take the date — the D-193 order) | BND-RN-014 (D-207) |
# | `RoomNightID` | `ReservationID` + `\|` + `SnapshotDateTime` (ISO 8601 as stored) + `\|` + `StayDate` (`yyyy-MM-dd`) + `\|` + `BookedRoomIndex` | BND-RN-001 (D-207) |
# 
# Two fail-loudly rules in this cell: every row must resolve a time zone (no guessed zones),
# and `BookingDateTime` must be present (it is not-null in the contract and
# `BookingWindowDays` cannot be honestly computed without it).
# 
# `BookingWindowDays` is **not clamped**: if a booking timestamp lands after the arrival
# date, the value goes negative and stays negative — no governed rule says otherwise, and
# Section 10 reports the min/max so oddities stay visible.


# CELL ********************

# ---------------------------------------------------------------
# 6. BookedRoomIndex, LOS_Nights, BookingWindowDays, RoomNightID.
# ---------------------------------------------------------------

# 6a. Time-zone join on PropertyKey (governed carry from parent) — must fully resolve.
df_tz = df_rows.join(df_seed_tz, on="PropertyKey", how="left")

n_tz_missing = df_tz.filter(F.col("_TimeZone").isNull()).count()
if n_tz_missing:
    raise RuntimeError(f"{n_tz_missing} room-night rows found no TimeZone for their "
                       "PropertyKey in the D_Property seed. No guessing — fix the seed "
                       "or the parent build first.")
print("Time zone resolved for all rows.")

n_bdt_null = df_tz.filter(F.col("BookingDateTime").isNull()).count()
if n_bdt_null:
    raise RuntimeError(f"{n_bdt_null} rows have NULL BookingDateTime — BookingWindowDays "
                       "(not-null contract column) cannot be computed. Fix upstream.")
print("BookingDateTime present on all rows.")

# 6b. Derived columns.
df_derived = (df_tz
    .withColumn("BookedRoomIndex", F.lit(1).cast("long"))                     # BND-RN-013
    .withColumn("LOS_Nights",
        F.datediff(F.col("DepartureDate"), F.col("ArrivalDate")).cast("long"))# BND-RN-011
    .withColumn("_BookingDateLocal",
        F.to_date(F.from_utc_timestamp(F.col("BookingDateTime"), F.col("_TimeZone"))))
    .withColumn("BookingWindowDays",
        F.datediff(F.col("ArrivalDate"), F.col("_BookingDateLocal")).cast("long")) # BND-RN-014
    .withColumn("_SnapshotISO",
        F.date_format(F.col("SnapshotDateTime"), SNAPSHOT_ISO_FORMAT))
    .withColumn("RoomNightID",                                                # BND-RN-001
        F.concat_ws("|",
            F.col("ReservationID"),
            F.col("_SnapshotISO"),
            F.date_format(F.col("StayDate"), "yyyy-MM-dd"),
            F.col("BookedRoomIndex").cast("string"))))

print("Sample RoomNightID values:")
df_derived.select("RoomNightID", "IsDayUse", "LOS_Nights", "BookingWindowDays") \
          .show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. `IsLatestCurrent` at room-night grain (D-143)
# 
# **Plain words:** for each combination of `ReservationID + StayDate + BookedRoomIndex`, the
# row with the newest `SnapshotDateTime` is the "current" version of that room night and gets
# `IsLatestCurrent = TRUE`; older versions get `FALSE`. This is computed fresh at the
# room-night grain per BND-RN-041 — it is **not** simply copied from the parent reservation
# flag, because a stay date that only exists in an older version still has its own latest row.
# 
# Ties are impossible: the parent grain is unique per `ReservationID + SnapshotDateTime`,
# so within one `ReservationID + StayDate + BookedRoomIndex` group every `SnapshotDateTime`
# is distinct.

# CELL ********************

# ---------------------------------------------------------------
# 7. IsLatestCurrent per ReservationID + StayDate + BookedRoomIndex (D-143).
# ---------------------------------------------------------------
w_latest = (Window
    .partitionBy("ReservationID", "StayDate", "BookedRoomIndex")
    .orderBy(F.col("SnapshotDateTime").desc()))

df_flagged = (df_derived
    .withColumn("_rn", F.row_number().over(w_latest))
    .withColumn("IsLatestCurrent", F.col("_rn") == 1)
    .drop("_rn"))

n_latest = df_flagged.filter("IsLatestCurrent").count()
n_groups = (df_flagged.select("ReservationID", "StayDate", "BookedRoomIndex")
                      .distinct().count())
print(f"IsLatestCurrent = TRUE rows: {n_latest}")
print(f"Distinct room-night groups:  {n_groups} -> "
      f"{'OK' if n_latest == n_groups else 'FAIL (must be equal)'}")
if n_latest != n_groups:
    raise RuntimeError("IsLatestCurrent flagging is inconsistent — stop.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Assemble the full governed column set (D-207 NULL rule)
# 
# **Plain words:** the `I_RoomNights` contract has **44 columns** (`03_Columns`, C-336 to
# C-414). This slice fills the 25 columns whose `10_I_Field_Bindings` rows are
# **GOVERNED_FINAL**; **every one of the 19 UNGOVERNED columns is written as literal NULL** —
# no fallback, no placeholder, no UNKNOWN key, no guessed lookup. NULL means, honestly:
# "not governed / not implemented in this slice".
# 
# Columns intentionally NULL, with the blocking issue:
# 
# - `BookedRoomRevenue_RoomNight`, `BookedRevenueDerivationMethod`,
#   `RealizedRoomRevenue_RoomNight`, `RealizedRevenueDerivationMethod`,
#   `RevenueState`, `CurrencyCode` — revenue/currency allocation open (I-196)
# - `EventID`, `BlockID` — block/event linkage deferred (D-162 / I-143)
# - `CustomerID`, `AccountID`, `MarketCountryKey` — upstream ungoverned (I-076)
# - `RoomTypeKey`, `RatePlanKey`, `SegmentKey` — lookups not governed (I-157)
# - `PMS_RoomTypeCode`, `PMS_RatePlanCode`, `PMS_SegmentCode` — upstream ungoverned (I-076)
# - `ChannelKey`, `PMS_ChannelCode` — channel mapping parked (D-170 / I-075)
# 
# **Transparency note:** several of these (`RevenueState`, `CurrencyCode`, `RoomTypeKey`,
# `PMS_RoomTypeCode`, `RatePlanKey`, `PMS_RatePlanCode`, `ChannelKey`, `PMS_ChannelCode`,
# `SegmentKey`, `PMS_SegmentCode`) are marked not-null in `03_Columns`. **D-207 explicitly
# overrides `NullableFlag = No` for the ungoverned columns in this slice.** The Delta table
# therefore allows NULLs; the not-null contract applies when those columns are actually
# built later.
# 
# The physical column order below follows the `03_Columns` listing order.


# CELL ********************

# ---------------------------------------------------------------
# 8. Final projection: 44 columns — governed values + governed NULLs.
# ---------------------------------------------------------------
def null_str():  return F.lit(None).cast("string")
def null_dbl():  return F.lit(None).cast("double")

df_final = df_flagged.select(
    # --- governed populated ---
    F.col("RoomNightID"),                                     # BND-RN-001  D-207
    F.col("ReservationID"),                                   # BND-RN-002  D-192
    F.col("PMSReservationID"),                                # BND-RN-003  D-192
    # --- governed NULLs (D-207 / I-196) ---
    null_dbl().alias("BookedRoomRevenue_RoomNight"),          # BND-RN-004
    null_str().alias("BookedRevenueDerivationMethod"),        # BND-RN-005
    null_dbl().alias("RealizedRoomRevenue_RoomNight"),        # BND-RN-006
    null_str().alias("RealizedRevenueDerivationMethod"),      # BND-RN-007
    null_str().alias("RevenueState"),                         # BND-RN-008 (override)
    # --- governed NULL (D-162 / I-143) ---
    null_str().alias("EventID"),                              # BND-RN-009
    # --- governed populated ---
    F.col("Children"),                                        # BND-RN-010  D-201
    F.col("LOS_Nights"),                                      # BND-RN-011  D-125/D-206
    F.col("BookedRooms").alias("RoomsBooked"),                # BND-RN-012  D-199 (naming drift, D-204 cleanup)
    F.col("BookedRoomIndex"),                                 # BND-RN-013  D-199
    F.col("BookingWindowDays"),                               # BND-RN-014  D-207
    F.col("SnapshotDateTime"),                                # BND-RN-015  D-189
    # --- governed NULL (I-196) ---
    null_str().alias("CurrencyCode"),                         # BND-RN-016 (override)
    # --- governed populated ---
    F.lit(SOURCE_SYSTEM).alias("SourceSystem"),               # BND-RN-017  CONSTANT
    F.col("PropertyKey"),                                     # BND-RN-018  D-195
    F.col("PropertyID"),                                      # BND-RN-019  D-195
    # --- governed NULLs (I-076 / D-162) ---
    null_str().alias("CustomerID"),                           # BND-RN-020
    null_str().alias("AccountID"),                            # BND-RN-021
    null_str().alias("MarketCountryKey"),                     # BND-RN-022
    null_str().alias("BlockID"),                              # BND-RN-023
    # --- governed populated ---
    F.col("IsGroupReservation"),                              # BND-RN-024  D-200 carry
    F.col("BookingDateTime"),                                 # BND-RN-025  D-192
    F.col("ArrivalDate"),                                     # BND-RN-026  D-193 carry
    F.col("DepartureDate"),                                   # BND-RN-027  D-193 carry
    F.col("StayDate"),                                        # BND-RN-028  D-206
    # --- governed NULLs (I-157 / I-076 / I-075, all with D-207 override) ---
    null_str().alias("RoomTypeKey"),                          # BND-RN-029
    null_str().alias("PMS_RoomTypeCode"),                     # BND-RN-030
    null_str().alias("RatePlanKey"),                          # BND-RN-031
    null_str().alias("PMS_RatePlanCode"),                     # BND-RN-032
    null_str().alias("ChannelKey"),                           # BND-RN-033
    null_str().alias("PMS_ChannelCode"),                      # BND-RN-034
    null_str().alias("SegmentKey"),                           # BND-RN-035
    null_str().alias("PMS_SegmentCode"),                      # BND-RN-036
    # --- governed populated ---
    F.col("ReservationStatusKey"),                            # BND-RN-037  D-190 (no status filter)
    F.col("PMSStatusCode"),                                   # BND-RN-038  D-190
    F.col("StatusDateTime"),                                  # BND-RN-039  D-191
    F.col("Adults"),                                          # BND-RN-040  D-201
    F.col("IsLatestCurrent"),                                 # BND-RN-041  D-143
    F.col("TenantKey"),                                       # BND-RN-042  D-177
    F.col("TenantID"),                                        # BND-RN-043  D-177
    F.col("IsDayUse"),                                        # BND-RN-044  D-206
)

n_final = df_final.count()
print(f"Final I_RoomNights rows:  {n_final}")
print(f"Final column count:       {len(df_final.columns)}  (expected 44)")
if len(df_final.columns) != 44:
    raise RuntimeError("Column count is not 44 — projection does not match the contract.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Write `I_RoomNights` and the exception table
# 
# **Plain words:** the finished dataframe is saved as a Delta table with `overwrite` (full
# rebuild — safe because `I_Reservations` is retained and this table can always be rebuilt).
# 
# The **invalid** versions from Section 4 go to `I_RoomNights_DQ_Exceptions` with a reason
# code and their stay dates, so nothing disappears silently (D-207: zero output rows, routed,
# never corrected). The exception table is only written when there is something to write.

# CELL ********************

# ---------------------------------------------------------------
# 9. Write target + DQ exceptions as Delta.
# ---------------------------------------------------------------
(df_final.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))
print(f"Wrote {TARGET_TABLE} ({WRITE_MODE}).")

if n_invalid > 0:
    df_exceptions = df_invalid.select(
        F.lit("INVALID_STAY_DATES_D207").alias("ExceptionType"),
        F.col("ReservationID"),
        F.col("SnapshotDateTime"),
        F.col("PMSReservationID"),
        F.col("ArrivalDate"),
        F.col("DepartureDate"),
        F.current_timestamp().alias("QuarantinedUtc"))
    (df_exceptions.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(DQ_TABLE))
    print(f"Wrote {DQ_TABLE}: {n_invalid} rows.")
else:
    print(f"No invalid versions in this build — {DQ_TABLE} not written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Validation — read the written table back and prove it
# 
# **Plain words:** every check below reads the **written** table, so what you check is what
# actually landed. The packet's six required checks, plus consistency extras:
# 
# 1. **Row-count funnel** — overnight rows = sum of nights, day-use rows = one per same-day
#    version, invalid versions contribute zero rows.
# 2. **`RoomNightID` uniqueness** — and uniqueness of the full grain tuple.
# 3. **`IsLatestCurrent`** — exactly one TRUE per `ReservationID + StayDate + BookedRoomIndex`,
#    and it sits on the newest `SnapshotDateTime`.
# 4. **NULL profile** — the 19 ungoverned columns must be 100% NULL; governed not-null
#    columns must be 0% NULL.
# 5. **Day-use consistency** — `IsDayUse` ⇔ `ArrivalDate = DepartureDate` ⇔ `LOS_Nights = 0`
#    ⇔ `StayDate = ArrivalDate`; overnight `StayDate` stays inside `[ArrivalDate,
#    DepartureDate - 1]`.
# 6. **Parent coverage** — every valid input version appears in the output; no invalid
#    version leaked in; `BookingWindowDays` profile printed for eyeballing.
# 
# If any line prints **FAIL**, do not build anything on top of this table.


# CELL ********************

# ---------------------------------------------------------------
# 10a. Row-count funnel (packet checks 1-3).
# ---------------------------------------------------------------
t = spark.read.table(TARGET_TABLE)
n_written    = t.count()
n_written_on = t.filter(~F.col("IsDayUse")).count()
n_written_du = t.filter(F.col("IsDayUse")).count()

print("Row-count funnel")
print(f"  Input reservation versions:        {n_in_rows}")
print(f"    overnight / day-use / invalid:   {n_overnight} / {n_dayuse} / {n_invalid}")
print(f"  Overnight rows written:  {n_written_on} (expected {expected_overnight_rows}) -> "
      f"{'OK' if n_written_on == expected_overnight_rows else 'FAIL'}")
print(f"  Day-use rows written:    {n_written_du} (expected {expected_dayuse_rows}) -> "
      f"{'OK' if n_written_du == expected_dayuse_rows else 'FAIL'}")
print(f"  Total rows written:      {n_written} (expected "
      f"{expected_overnight_rows + expected_dayuse_rows}) -> "
      f"{'OK' if n_written == expected_overnight_rows + expected_dayuse_rows else 'FAIL'}")

# Invalid versions must contribute zero output rows.
if n_invalid > 0:
    n_leaked = (t.join(df_invalid.select("ReservationID", "SnapshotDateTime").distinct(),
                       ["ReservationID", "SnapshotDateTime"], "inner").count())
else:
    n_leaked = 0
print(f"  Rows from invalid versions in output: {n_leaked} -> "
      f"{'OK' if n_leaked == 0 else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 10b. RoomNightID + grain uniqueness (packet check 4).
# ---------------------------------------------------------------
n_dup_id = t.groupBy("RoomNightID").count().filter("count > 1").count()
print(f"Duplicate RoomNightID values:   {n_dup_id} -> {'OK' if n_dup_id == 0 else 'FAIL'}")

n_dup_grain = (t.groupBy("ReservationID", "SnapshotDateTime", "StayDate", "BookedRoomIndex")
                .count().filter("count > 1").count())
print(f"Duplicate grain tuples:         {n_dup_grain} -> "
      f"{'OK' if n_dup_grain == 0 else 'FAIL'}")

n_null_id = t.filter(F.col("RoomNightID").isNull()).count()
print(f"NULL RoomNightID values:        {n_null_id} -> {'OK' if n_null_id == 0 else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 10c. IsLatestCurrent correctness (packet check 5).
# ---------------------------------------------------------------
grp = (t.groupBy("ReservationID", "StayDate", "BookedRoomIndex")
        .agg(F.sum(F.col("IsLatestCurrent").cast("int")).alias("_n_true"),
             F.max("SnapshotDateTime").alias("_max_snap"),
             F.max(F.when(F.col("IsLatestCurrent"), F.col("SnapshotDateTime")))
              .alias("_snap_of_true")))

n_bad_count = grp.filter(F.col("_n_true") != 1).count()
n_bad_pick  = grp.filter(F.col("_snap_of_true") != F.col("_max_snap")).count()
print(f"Groups without exactly one IsLatestCurrent=TRUE: {n_bad_count} -> "
      f"{'OK' if n_bad_count == 0 else 'FAIL'}")
print(f"Groups where TRUE is not the newest snapshot:    {n_bad_pick} -> "
      f"{'OK' if n_bad_pick == 0 else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 10d. NULL profile (packet check 6).
# ---------------------------------------------------------------
UNGOVERNED_NULL_COLS = [
    "BookedRoomRevenue_RoomNight", "BookedRevenueDerivationMethod",
    "RealizedRoomRevenue_RoomNight", "RealizedRevenueDerivationMethod",
    "RevenueState", "EventID", "CurrencyCode",
    "CustomerID", "AccountID", "MarketCountryKey", "BlockID",
    "RoomTypeKey", "PMS_RoomTypeCode", "RatePlanKey", "PMS_RatePlanCode",
    "ChannelKey", "PMS_ChannelCode", "SegmentKey", "PMS_SegmentCode",
]
NOT_NULL_GOVERNED_COLS = [
    "RoomNightID", "ReservationID", "LOS_Nights", "RoomsBooked", "BookedRoomIndex",
    "BookingWindowDays", "SnapshotDateTime", "SourceSystem", "PropertyKey", "PropertyID",
    "IsGroupReservation", "BookingDateTime", "ArrivalDate", "DepartureDate", "StayDate",
    "ReservationStatusKey", "PMSStatusCode", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
]
# (PMSReservationID, Children, Adults, StatusDateTime are nullable in the contract.)

print(f"Ungoverned columns written NULL this slice: {len(UNGOVERNED_NULL_COLS)}")
all_ok = True
for c in UNGOVERNED_NULL_COLS:
    n_not_null = t.filter(F.col(c).isNotNull()).count()
    ok = n_not_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_not_null} non-NULL values -> FAIL (must be 100% NULL)")
print(f"  100% NULL on all 19 ungoverned columns -> {'OK' if all_ok else 'FAIL'}")

all_ok = True
for c in NOT_NULL_GOVERNED_COLS:
    n_null = t.filter(F.col(c).isNull()).count()
    ok = n_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_null} NULL values -> FAIL (governed not-null)")
print(f"  0% NULL on governed not-null columns -> {'OK' if all_ok else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 10e. Day-use consistency, StayDate bounds, BookingWindowDays profile,
#      parent coverage.
# ---------------------------------------------------------------
n_bad_flag = t.filter(
    (F.col("IsDayUse") != (F.col("ArrivalDate") == F.col("DepartureDate")))).count()
n_bad_los = t.filter(
    (F.col("IsDayUse") & (F.col("LOS_Nights") != 0)) |
    (~F.col("IsDayUse") & (F.col("LOS_Nights") <= 0))).count()
n_bad_du_stay = t.filter(
    F.col("IsDayUse") & (F.col("StayDate") != F.col("ArrivalDate"))).count()
n_bad_on_stay = t.filter(
    ~F.col("IsDayUse") &
    ((F.col("StayDate") < F.col("ArrivalDate")) |
     (F.col("StayDate") >= F.col("DepartureDate")))).count()

print(f"IsDayUse flag vs dates mismatches:        {n_bad_flag} -> "
      f"{'OK' if n_bad_flag == 0 else 'FAIL'}")
print(f"LOS_Nights inconsistencies:               {n_bad_los} -> "
      f"{'OK' if n_bad_los == 0 else 'FAIL'}")
print(f"Day-use StayDate <> ArrivalDate:          {n_bad_du_stay} -> "
      f"{'OK' if n_bad_du_stay == 0 else 'FAIL'}")
print(f"Overnight StayDate outside stay window:   {n_bad_on_stay} -> "
      f"{'OK' if n_bad_on_stay == 0 else 'FAIL'}")

# Parent coverage: every valid input version appears, invalid versions do not.
n_versions_out = t.select("ReservationID", "SnapshotDateTime").distinct().count()
n_versions_expected = n_overnight + n_dayuse
print(f"Distinct versions in output: {n_versions_out} (expected {n_versions_expected}) -> "
      f"{'OK' if n_versions_out == n_versions_expected else 'FAIL'}")

print("\nBookingWindowDays profile (visibility only, no governed clamp):")
t.agg(F.min("BookingWindowDays").alias("Min"),
      F.max("BookingWindowDays").alias("Max"),
      F.sum(F.col("BookingWindowDays").isNull().cast("int")).alias("Nulls")) \
 .show(truncate=False)

print("Latest-current day-use / overnight split (visibility):")
t.filter("IsLatestCurrent").groupBy("IsDayUse").count().show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Wrap-up — honest status
# 
# **Plain words:** when this notebook has run, `I_RoomNights` exists in the DEV lakehouse as
# a Delta table — nothing more. It is **not** done, committed, or governed-complete until:
# 
# 1. You confirm the run and paste/inspect the Section 10 results.
# 2. You commit the notebook to GitHub (`menja-bi/menja-bi-v1`) if you want it versioned.
# 3. Any follow-up documentation is handed to Copilot.
# 
# Out of scope, still blocked, unchanged: revenue and `CurrencyCode` (I-196), room-type /
# rate-plan / channel / segment lookups, block/event linkage, `F_RoomNights`, measures,
# multi-room mechanics.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working, to avoid
# unnecessary cost.**

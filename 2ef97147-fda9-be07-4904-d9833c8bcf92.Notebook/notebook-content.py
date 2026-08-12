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

# # I_RoomNights — BUILD_DRAFT — Phase-1 Mews slice + governed revenue
# 
# **Plain words:** this notebook takes the already-built `I_Reservations` table and turns every
# stored reservation version into one row **per room, per night** (plus one row per same-day
# day-use stay). It then populates the six governed revenue columns from the landed raw Mews
# order items, following the FINAL revenue rule chain. `I_RoomNights` is the room-night
# ingestion table — the step between the reservation history and the `F_RoomNights` fact.
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until the user
# runs it in Fabric and confirms the results.
# 
# **Governance basis (all FINAL):**
# 
# | Rule | Decision / binding |
# |---|---|
# | Same-day day-use row creation and `IsDayUse` flag | D-206 |
# | Build slice scope, invalid-date routing, `RoomNightID` format, `BookingWindowDays` date basis | D-207 |
# | Grain `ReservationID + SnapshotDateTime + StayDate + BookedRoomIndex` | D-124 / LIN_MEWS_I_ROOMNIGHTS_001 |
# | `BookedRooms = 1` in this slice, `BookedRoomIndex` sequence rule | D-199 / BND-RN-013 |
# | `IsLatestCurrent` per `ReservationID + StayDate + BookedRoomIndex` | D-143 / BND-RN-041 |
# | Revenue source = order items, net of tax, seed-driven charge-type classification | D-197 |
# | Order-item-to-reservation link: `ServiceOrderId` = `Reservations[].Id`, whole-value equality only | D-238 |
# | Recognition: realized when consumed at or before the snapshot creation time; `Canceled` / `Inactive` items contribute nothing | D-239 |
# | Allocation: room night whose `StayDate` = property-local date of `ConsumedUtc`; rebates follow `Data.Rebate.RebatedItemId`; nothing split, no rounding | D-240 |
# | Value lists: `RevenueState` ∈ {BOOKED, REALIZED}; derivation methods = CONSUMED | D-241 |
# | Currency from `D_Property.CurrencyCode`; on unresolved/mismatch, every currency-dependent value on the row is blank | D-242 |
# | Field-by-field population | 44 `BND-RN-*` rows in `10_I_Field_Bindings` (31 GOVERNED_FINAL, 13 UNGOVERNED → NULL) |
# | Seed workbook consumption | D-198 |
# 
# **Explicitly NOT in this notebook:** room-type / rate-plan / channel / segment lookups,
# block or event linkage, `F_RoomNights`, measures, multi-room mechanics beyond
# `BookedRoomIndex = 1`, no-show treatment (I-186), any extraction. The remaining 13
# ungoverned columns are written as honest NULLs (see Section 8).


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

# ## 1r. Revenue configuration (NEW)
# 
# **Plain words:** names, paths and fixed governed values for the revenue build. The pinned
# seed expectations mirror the seed state governed on 2026-08-11 (I-223): if the seed
# legitimately changes under governance, update these constants deliberately — the point is
# that a *stale or wrong* seed stops the run instead of silently producing wrong revenue,
# which cost a day on 2026-08-10.

# CELL ********************

# ---------------------------------------------------------------
# 1r. Revenue configuration.
# ---------------------------------------------------------------

# --- landed raw order items (read-only; no extraction is run here) ---
RAW_ORDER_ITEMS_DIR = "/lakehouse/default/Files/Raw/Mews/orderItems/getAll"   # <-- CONFIRM

# --- governed constant values (D-241) ---
REVENUE_STATE_BOOKED     = "BOOKED"
REVENUE_STATE_REALIZED   = "REALIZED"
DERIVATION_METHOD        = "CONSUMED"     # BND-RN-005 / BND-RN-007: CONSTANT

# --- D-239 dead accounting states (contribute to neither booked nor realized) ---
DEAD_ACCOUNTING_STATES = ["Canceled", "Inactive"]

# --- exception type codes for I_RoomNights_DQ_Exceptions ---
# Three governed populations must stay distinguishable (D-238 / D-197 / D-240),
# plus the existing D-207 invalid-date population.
EXC_INVALID_DATES   = "INVALID_STAY_DATES_D207"     # existing
EXC_UNRESOLVED_LINK = "UNRESOLVED_LINK_D238"
EXC_UNMAPPED_TYPE   = "UNMAPPED_CHARGE_TYPE_D197"
EXC_UNALLOCATED     = "UNALLOCATED_ITEM_D240"

# --- pinned runtime-seed expectations (state governed 2026-08-11 under I-223) ---
# Purpose: stale-seed guard. Update deliberately on a governed seed change.
SEED_RTM_SHEET                = "Revenue_Type_Mapping"
SEED_RTM_EXPECTED_FINAL_ROWS  = 16
SEED_RTM_EXPECTED_ROOM_TRUE   = ["SpaceOrder", "NightRebate", "ResourceUpgradeFee"]

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

# ## 3r. Revenue seed: `Revenue_Type_Mapping` + property currency (NEW)
# 
# **Plain words:** two more things come from the seed workbook, under the same D-198 rules
# (explicit allow-list, `_`-sheets never read, seed is runtime input, not authority):
# 
# 1. **`Revenue_Type_Mapping`** — the D-197 charge-type classification. A charge type is room
#    revenue only if its row is `FINAL` and `IncludeInRoomRevenue` is `TRUE`. A missing row,
#    a non-FINAL row, or a blank flag means the item is excluded and recorded as a
#    data-quality exception (D-197 / D-198). `IncludeInTotalRevenue` is read but not used —
#    no governed total-revenue column exists in this build.
# 2. **`D_Property.CurrencyCode`** — the only governed currency source (D-242). It may be
#    blank; a blank is a governed case that blanks the row, not an error, so it is **not**
#    part of the readiness gate.
# 
# The stale-seed gate re-verifies, at run time and against the copy Fabric actually reads,
# what was checked in review on 2026-08-11: exactly 16 FINAL rows and the three room-revenue
# types flagged TRUE. Property-specific seed rows (non-blank `PropertyID`) hard-stop: D-197
# **does** govern the precedence — property-specific first, then global PMS, then unmapped
# handling — but which identifier the seed's `PropertyID` column holds is ambiguous (D-197:
# "PropertyID or equivalent property scope column"). The stop guards that ambiguous match
# key pending clarification; every current row is global, so it never fires today.


# CELL ********************

# ---------------------------------------------------------------
# 3r. Revenue_Type_Mapping load + stale-seed gate + property currency.
# ---------------------------------------------------------------

if SEED_RTM_SHEET.startswith("_"):
    raise RuntimeError("Classification sheet name must not be a _-prefixed sheet (D-198).")
if SEED_RTM_SHEET not in xls.sheet_names:
    raise RuntimeError(f"Required seed sheet '{SEED_RTM_SHEET}' not found (D-198 allow-list).")

rtm_pdf = pd.read_excel(xls, sheet_name=SEED_RTM_SHEET, dtype=object)
rtm_pdf = rtm_pdf.dropna(how="all")

REQUIRED_RTM_COLS = ["PMS", "TypeCode", "PropertyID", "IncludeInRoomRevenue",
                     "IncludeInTotalRevenue", "IsActive", "ClassificationStatus"]
missing_cols = [c for c in REQUIRED_RTM_COLS if c not in rtm_pdf.columns]
if missing_cols:
    raise RuntimeError(f"{SEED_RTM_SHEET} is missing required columns {missing_cols}.")

def _norm_flag(v, ctx):
    """Excel booleans arrive as bool; guard against text or junk. Blank stays None."""
    if v is None or (not isinstance(v, bool) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1"):  return True
    if s in ("false", "0"): return False
    raise RuntimeError(f"Unexpected flag value {v!r} in {ctx} — fix the seed, no guessing.")

def _norm_str(v):
    if v is None or (not isinstance(v, bool) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s if s else None

rtm_rows = []
for _, row in rtm_pdf.iterrows():
    pms = _norm_str(row["PMS"])
    if pms is None:
        continue
    if pms != "MEWS":
        raise RuntimeError(f"Unexpected PMS '{pms}' in {SEED_RTM_SHEET} — this build is "
                           "Mews-only (D-197 scope). Stop and review the seed.")
    if _norm_str(row["PropertyID"]) is not None:
        raise RuntimeError(
            "Property-specific Revenue_Type_Mapping row found (non-blank PropertyID). "
            "D-197 governs property-first precedence, but which identifier the seed's "
            "PropertyID column holds is ambiguous (D-197: 'PropertyID or equivalent "
            "property scope column'). Stop pending clarification — do not guess the "
            "match key.")
    rtm_rows.append({
        "TypeCode":  _norm_str(row["TypeCode"]),
        "Status":    (_norm_str(row["ClassificationStatus"]) or "").upper() or None,
        "IncRoom":   _norm_flag(row["IncludeInRoomRevenue"],  f"IncludeInRoomRevenue/{row['TypeCode']}"),
        "IncTotal":  _norm_flag(row["IncludeInTotalRevenue"], f"IncludeInTotalRevenue/{row['TypeCode']}"),
        "IsActive":  _norm_flag(row["IsActive"],              f"IsActive/{row['TypeCode']}"),
    })

if any(r["TypeCode"] is None for r in rtm_rows):
    raise RuntimeError("Blank TypeCode in Revenue_Type_Mapping — fix the seed.")
_seen = set()
for r in rtm_rows:
    if r["TypeCode"] in _seen:
        raise RuntimeError(f"Duplicate global MEWS row for TypeCode '{r['TypeCode']}' — "
                           "classification would be ambiguous. Fix the seed.")
    _seen.add(r["TypeCode"])

# --- stale-seed gate (pinned to the state governed 2026-08-11, I-223) ---
n_final = sum(1 for r in rtm_rows if r["Status"] == "FINAL")
if n_final != SEED_RTM_EXPECTED_FINAL_ROWS:
    raise RuntimeError(
        f"Stale-seed gate: {n_final} FINAL rows found, expected "
        f"{SEED_RTM_EXPECTED_FINAL_ROWS}. Either the runtime seed copy is stale "
        "(see 2026-08-10 incident) or the seed changed under governance — if the "
        "latter, update SEED_RTM_* in Section 1r deliberately.")
for tc in SEED_RTM_EXPECTED_ROOM_TRUE:
    r = next((x for x in rtm_rows if x["TypeCode"] == tc), None)
    if r is None or r["Status"] != "FINAL" or r["IncRoom"] is not True:
        raise RuntimeError(f"Stale-seed gate: '{tc}' is not FINAL with "
                           "IncludeInRoomRevenue = TRUE. Same instruction as above.")
if any(r["IsActive"] is not True for r in rtm_rows):
    raise RuntimeError("Stale-seed gate: a Revenue_Type_Mapping row is not IsActive = TRUE. "
                       "No governed treatment for inactive rows exists — stop.")
print(f"Revenue_Type_Mapping OK: {len(rtm_rows)} global MEWS rows, {n_final} FINAL, "
      f"room-revenue TRUE on {SEED_RTM_EXPECTED_ROOM_TRUE}.")

from pyspark.sql import types as T
rtm_schema = T.StructType([
    T.StructField("Type",        T.StringType(),  False),
    T.StructField("_SeedStatus", T.StringType(),  True),
    T.StructField("_SeedIncRoom",T.BooleanType(), True),
])
df_rtm = spark.createDataFrame(
    [(r["TypeCode"], r["Status"], r["IncRoom"]) for r in rtm_rows], rtm_schema)

# --- property currency (D-242): optional value, blank is a governed case ---
if "CurrencyCode" not in prop_pdf.columns:
    raise RuntimeError("D_Property seed has no CurrencyCode column — D-242 source missing.")
prop_ccy_rows = [(str(k).strip(), (str(c).strip() or None) if pd.notna(c) else None)
                 for k, c in zip(prop_pdf["PropertyKey"], prop_pdf["CurrencyCode"])]
ccy_schema = T.StructType([
    T.StructField("PropertyKey",   T.StringType(), False),
    T.StructField("_PropCurrency", T.StringType(), True),
])
df_prop_ccy = spark.createDataFrame(prop_ccy_rows, ccy_schema)
print("Property currency seed:")
df_prop_ccy.show(truncate=False)

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

# ## 7a. Read the landed raw order items (NEW — scaffolding, not business logic)
# 
# **Plain words:** read the raw Mews order-item JSON pages already landed by NB00. Only the
# fields the governed rules need are projected, with an explicit schema — no inference, so a
# future landing without rebates cannot silently change the shape. Duplicate item Ids across
# page files would mean a double-landed page: identical duplicates are dropped once,
# conflicting duplicates stop the run.
# 
# This cell runs **no extraction** and reads nothing outside the lakehouse `Files` area.
# Driver-side JSON parsing is deliberate at Phase-1 volume (5,559 items); revisit before any
# LIVE-scale load.

# CELL ********************

# ---------------------------------------------------------------
# 7a. Raw order items -> typed dataframe (explicit schema).
# ---------------------------------------------------------------
import json, glob

_oi_files = sorted(glob.glob(os.path.join(RAW_ORDER_ITEMS_DIR, "*.json")))
if not _oi_files:
    raise RuntimeError(f"No order-item files found under {RAW_ORDER_ITEMS_DIR}. "
                       "Nothing to build revenue from — stop.")
print(f"Order-item files found: {len(_oi_files)}")

_seen_items = {}
_n_dup_identical = 0
for fp in _oi_files:
    with open(fp) as fh:
        payload = json.load(fh)
    if "OrderItems" not in payload:
        raise RuntimeError(f"{fp} has no OrderItems key — unexpected file shape, stop.")
    for it in payload["OrderItems"]:
        data   = it.get("Data") or {}
        rebate = data.get("Rebate") or {} if isinstance(data, dict) else {}
        amount = it.get("Amount") or {}
        rec = (
            it.get("Id"),
            it.get("ServiceOrderId"),
            it.get("Type"),
            it.get("AccountingState"),
            it.get("ConsumedUtc"),
            float(amount["NetValue"]) if amount.get("NetValue") is not None else None,
            amount.get("Currency"),
            rebate.get("RebatedItemId"),
        )
        if rec[0] is None:
            raise RuntimeError(f"Order item with null Id in {fp} — stop.")
        if rec[0] in _seen_items:
            if _seen_items[rec[0]] == rec:
                _n_dup_identical += 1
                continue
            raise RuntimeError(f"Conflicting duplicate order item Id {rec[0]} across "
                               "files — landed data is inconsistent, stop.")
        _seen_items[rec[0]] = rec

oi_schema = T.StructType([
    T.StructField("OrderItemId",    T.StringType(), False),
    T.StructField("ServiceOrderId", T.StringType(), True),
    T.StructField("ItemType",       T.StringType(), True),
    T.StructField("AccountingState",T.StringType(), True),
    T.StructField("_ConsumedRaw",   T.StringType(), True),
    T.StructField("NetValue",       T.DoubleType(), True),
    T.StructField("ItemCurrency",   T.StringType(), True),
    T.StructField("RebatedItemId",  T.StringType(), True),
])
df_items = (spark.createDataFrame(list(_seen_items.values()), oi_schema)
    .withColumn("ConsumedUtc", F.to_timestamp("_ConsumedRaw")))

n_items = df_items.count()
n_bad_ts = df_items.filter(F.col("_ConsumedRaw").isNotNull() &
                           F.col("ConsumedUtc").isNull()).count()
if n_bad_ts:
    raise RuntimeError(f"{n_bad_ts} ConsumedUtc values failed timestamp parsing — "
                       "format defect in landed files, stop.")
df_items = df_items.drop("_ConsumedRaw")
print(f"Order items loaded: {n_items}  (identical duplicates dropped: {_n_dup_identical})")
print("By AccountingState:"); df_items.groupBy("AccountingState").count().show(truncate=False)
print("By Type:");            df_items.groupBy("ItemType").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7b. Link items to reservations (NEW — D-238)
# 
# **Plain words:** an order item belongs to a reservation if and only if its `ServiceOrderId`
# equals the reservation's raw Mews Id, which `I_Reservations.ReservationID` carries verbatim
# (BND-RES-001, GOVERNED_FINAL). Whole-value equality, nothing else. Items that do not
# resolve are excluded and recorded as `UNRESOLVED_LINK_D238` exceptions.
# 
# **Expect a real unresolved population here.** The landed items were pulled tenant-wide on
# 2026-08-02, before D-243 existed, so some belong to reservations that were never extracted.
# That is a data-scope artifact, not a defect. Under D-238 these counts are recorded
# observations only — never a quality measure, threshold, or alert.

# CELL ********************

# ---------------------------------------------------------------
# 7b. D-238 link: ServiceOrderId = ReservationID, whole-value equality.
# ---------------------------------------------------------------
df_res_ids = df_in.select("ReservationID").distinct()

df_items_linked = (df_items
    .join(df_res_ids.withColumn("_Linked", F.lit(True)),
          df_items.ServiceOrderId == df_res_ids.ReservationID, "left")
    .drop(df_res_ids.ReservationID))

df_exc_unresolved = (df_items_linked.filter(F.col("_Linked").isNull())
    .withColumn("ExceptionType", F.lit(EXC_UNRESOLVED_LINK))
    .withColumn("ExceptionDetail",
        F.when(F.col("ServiceOrderId").isNull(), F.lit("SERVICE_ORDER_ID_MISSING"))
         .otherwise(F.lit("NO_MATCHING_RESERVATION"))))
df_linked = (df_items_linked.filter(F.col("_Linked") == True)
    .withColumn("ReservationID", F.col("ServiceOrderId"))
    .drop("_Linked"))

n_unresolved = df_exc_unresolved.count()
n_linked     = df_linked.count()
print(f"Linked items:     {n_linked}")
print(f"Unresolved items: {n_unresolved}  -> {EXC_UNRESOLVED_LINK} "
      "(recorded observation only under D-238 — not a quality measure)")
if n_linked + n_unresolved != n_items:
    raise RuntimeError("Link accounting broken: linked + unresolved != total. Stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7c. Classify charge types through the seed (NEW — D-197 / D-198)
# 
# **Plain words:** each linked item's `Type` is looked up in `Revenue_Type_Mapping`. Three
# outcomes, all governed:
# 
# | Seed row for the type | Outcome |
# |---|---|
# | `FINAL` and `IncludeInRoomRevenue = TRUE` | qualifies as room revenue |
# | `FINAL` and `IncludeInRoomRevenue = FALSE` | excluded from room revenue — governed classification, **no** exception |
# | missing, not FINAL, or flag blank | excluded **and** recorded as `UNMAPPED_CHARGE_TYPE_D197` exception |
# 
# The four DRAFT rows (`Surcharge`, three `Allowance` types) fall in the third bucket by
# design: none appears in the landed data, and a future occurrence should surface as a
# decision, not be absorbed silently.

# CELL ********************

# ---------------------------------------------------------------
# 7c. D-197 seed classification (global MEWS rows; precedence gate in 3r).
# ---------------------------------------------------------------
df_classified = df_linked.join(F.broadcast(df_rtm),
                               df_linked.ItemType == df_rtm.Type, "left").drop("Type")

# eqNullSafe: a blank flag or a missing seed row must land in the exception
# bucket, never disappear into SQL three-valued logic.
is_qualifying = (F.col("_SeedStatus").eqNullSafe(F.lit("FINAL")) &
                 F.col("_SeedIncRoom").eqNullSafe(F.lit(True)))
is_excl_noexc = (F.col("_SeedStatus").eqNullSafe(F.lit("FINAL")) &
                 F.col("_SeedIncRoom").eqNullSafe(F.lit(False)))

df_exc_unmapped = (df_classified.filter(~(is_qualifying | is_excl_noexc) |
                                        F.col("_SeedStatus").isNull())
    .withColumn("ExceptionType", F.lit(EXC_UNMAPPED_TYPE))
    .withColumn("ExceptionDetail",
        F.when(F.col("_SeedStatus").isNull(), F.lit("NO_SEED_ROW"))
         .when(F.col("_SeedStatus") != "FINAL", F.lit("SEED_ROW_NOT_FINAL"))
         .otherwise(F.lit("INCLUDE_FLAG_BLANK"))))
df_qualifying_all = df_classified.filter(is_qualifying)
n_excl_noexc      = df_classified.filter(is_excl_noexc).count()

n_unmapped   = df_exc_unmapped.count()
n_qualifying = df_qualifying_all.count()
print(f"Qualifying room-revenue items:              {n_qualifying}")
print(f"Excluded by governed classification:        {n_excl_noexc} (no exception)")
print(f"Unmapped / blank-flag / non-FINAL items:    {n_unmapped} -> {EXC_UNMAPPED_TYPE}")
if n_qualifying + n_excl_noexc + n_unmapped != n_linked:
    raise RuntimeError("Classification accounting broken — stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7d. Exclude dead items (NEW — D-239)
# 
# **Plain words:** items whose `AccountingState` is `Canceled` or `Inactive` contribute to
# neither booked nor realized revenue. Their source values stay untouched in the raw layer;
# they are simply not part of the revenue computation. This is governed classification, not
# an exception. Roughly half of the landed items fall out here — expected.

# CELL ********************

# ---------------------------------------------------------------
# 7d. D-239 dead-item exclusion (values retained, no exception).
# ---------------------------------------------------------------
df_dead = df_qualifying_all.filter(F.col("AccountingState").isin(DEAD_ACCOUNTING_STATES))
df_live = df_qualifying_all.filter(~F.col("AccountingState").isin(DEAD_ACCOUNTING_STATES))

n_dead = df_dead.count()
n_live = df_live.count()
print(f"Qualifying items in dead states {DEAD_ACCOUNTING_STATES}: {n_dead} (contribute nothing)")
print(f"Qualifying live items entering recognition/allocation:  {n_live}")
if n_dead + n_live != n_qualifying:
    raise RuntimeError("Dead-item accounting broken — stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7e. Recognise and allocate, per snapshot version (NEW — D-239 / D-240)
# 
# **Plain words:** every qualifying live item is evaluated once per stored snapshot version
# of its reservation:
# 
# - **Recognition (D-239):** realized when `ConsumedUtc` is at or before the version's
#   `SnapshotDateTime`; booked when later. Two timestamps compared — no dates derived.
# - **Allocation (D-240):** the item lands on the room night whose `StayDate` equals the
#   property-local calendar date of `ConsumedUtc` — converted to `D_Property.TimeZone`
#   **first**, then truncated to a date (the D-193 order), same reservation, same version.
# - **Rebates (D-240):** an item carrying `Data.Rebate.RebatedItemId` goes instead to the
#   night of the item it rebates, following the chain to the original where a rebate rebates
#   a rebate. A rebate whose target cannot be resolved, or is not itself allocated in that
#   version, is not allocated and is recorded as an exception. An item without the pointer —
#   credit or not — takes its own consumption date; guessing what it offsets is excluded.
# - Nothing is divided, weighted, prorated or spread, so no rounding rule exists and none is
#   introduced. An item with no matching night is excluded and recorded as
#   `UNALLOCATED_ITEM_D240` — never attached to a nearby night.
# 
# **Basis note, stated plainly:** exactly one order-item snapshot is landed (2026-08-02).
# Every reservation version is calculated against that one item set. When D-243-governed
# extraction exists, items will arrive run-aligned; this build works from what is landed.
# 
# The rebate chain is resolved driver-side over the full landed item set (60 pointers) with
# cycle detection — transparent at this volume, revisit at LIVE scale. A pointer chain that
# cycles stops the run: D-240 defines following the chain to the original, and a cycle has
# no original, which is a data defect to surface, not to absorb.


# CELL ********************

# ---------------------------------------------------------------
# 7e. D-239 recognition + D-240 allocation, per reservation version.
# ---------------------------------------------------------------

# Snapshot versions and their property timezone (from Section 6 join, D-193 order).
df_versions = df_flagged.select("ReservationID", "SnapshotDateTime",
                                "PropertyKey", "_TimeZone").distinct()
n_versions = df_versions.count()
print(f"Reservation versions to calculate: {n_versions}")

# Item x version pairs for this item's reservation.
df_iv = df_live.join(df_versions, "ReservationID")
n_iv_total = df_iv.count()

# D-239 recognition: timestamp comparison only.
df_iv = df_iv.withColumn("_IsRealized", F.col("ConsumedUtc") <= F.col("SnapshotDateTime"))

# Items with no consumption timestamp cannot be recognised or placed on a night:
# no matching night exists for them -> D-240 exception with an explicit detail.
df_iv_nots  = df_iv.filter(F.col("ConsumedUtc").isNull())
df_iv       = df_iv.filter(F.col("ConsumedUtc").isNotNull())

# --- rebate chain resolution (driver-side, full landed item set) ---
_ptr = {r["OrderItemId"]: r["RebatedItemId"]
        for r in df_items.select("OrderItemId", "RebatedItemId").collect()}
_root = {}      # OrderItemId -> (root_id or None, fail_reason or None)
for iid, tgt in _ptr.items():
    if tgt is None:
        continue
    seen, cur = {iid}, tgt
    while True:
        if cur not in _ptr:
            _root[iid] = (None, "REBATE_TARGET_NOT_LANDED"); break
        if cur in seen:
            raise RuntimeError(f"Rebate pointer cycle involving item {iid} — "
                               "chain has no original (D-240). Data defect, stop.")
        seen.add(cur)
        nxt = _ptr[cur]
        if nxt is None:
            _root[iid] = (cur, None); break
        cur = nxt
print(f"Rebate pointers resolved: {len(_root)} "
      f"(unresolvable targets: {sum(1 for v in _root.values() if v[0] is None)})")

root_schema = T.StructType([
    T.StructField("OrderItemId", T.StringType(), False),
    T.StructField("_RootItemId", T.StringType(), True),
    T.StructField("_RootFail",   T.StringType(), True),
])
df_roots = spark.createDataFrame(
    [(k, v[0], v[1]) for k, v in _root.items()], root_schema) if _root else \
    spark.createDataFrame([], root_schema)

# Split pointer / non-pointer item-version rows.
df_iv_direct = df_iv.filter(F.col("RebatedItemId").isNull())
df_iv_rebate = df_iv.filter(F.col("RebatedItemId").isNotNull()).join(df_roots, "OrderItemId", "left")

# --- direct allocation: property-local date of ConsumedUtc (D-193 order) ---
df_iv_direct = df_iv_direct.withColumn(
    "_AllocDate", F.to_date(F.from_utc_timestamp(F.col("ConsumedUtc"), F.col("_TimeZone"))))

df_nights = (df_flagged.select(
    F.col("ReservationID").alias("_nRes"),
    F.col("SnapshotDateTime").alias("_nSnap"),
    F.col("StayDate").alias("_nStay"))
    .distinct()
    .withColumn("_NightExists", F.lit(True)))

df_direct_j = df_iv_direct.join(
    df_nights,
    (df_iv_direct.ReservationID == df_nights._nRes) &
    (df_iv_direct.SnapshotDateTime == df_nights._nSnap) &
    (df_iv_direct._AllocDate == df_nights._nStay),
    "left").drop("_nRes", "_nSnap", "_nStay")

df_direct_alloc = (df_direct_j.filter(F.col("_NightExists") == True)
    .withColumn("StayDate", F.col("_AllocDate")))
df_exc_unalloc_direct = (df_direct_j.filter(F.col("_NightExists").isNull())
    .withColumn("ExceptionType", F.lit(EXC_UNALLOCATED))
    .withColumn("ExceptionDetail", F.lit("NO_MATCHING_NIGHT")))

# --- rebate allocation: follow the chain to the original's allocated night ---
df_targets = df_direct_alloc.select(
    F.col("OrderItemId").alias("_tItem"),
    F.col("ReservationID").alias("_tRes"),
    F.col("SnapshotDateTime").alias("_tSnap"),
    F.col("StayDate").alias("_tStay"))

df_rebate_j = df_iv_rebate.join(
    df_targets,
    (df_iv_rebate._RootItemId == df_targets._tItem) &
    (df_iv_rebate.ReservationID == df_targets._tRes) &
    (df_iv_rebate.SnapshotDateTime == df_targets._tSnap),
    "left")

df_rebate_alloc = (df_rebate_j.filter(F.col("_tStay").isNotNull())
    .withColumn("StayDate", F.col("_tStay"))
    .drop("_tItem", "_tRes", "_tSnap", "_tStay"))
df_exc_unalloc_rebate = (df_rebate_j.filter(F.col("_tStay").isNull())
    .drop("_tItem", "_tRes", "_tSnap", "_tStay")
    .withColumn("ExceptionType", F.lit(EXC_UNALLOCATED))
    .withColumn("ExceptionDetail",
        F.coalesce(F.col("_RootFail"), F.lit("REBATE_TARGET_NOT_ALLOCATED"))))

df_exc_unalloc_nots = (df_iv_nots
    .withColumn("ExceptionType", F.lit(EXC_UNALLOCATED))
    .withColumn("ExceptionDetail", F.lit("CONSUMED_UTC_MISSING")))

CONTRIB_COLS = ["ReservationID", "SnapshotDateTime", "StayDate", "PropertyKey",
                "OrderItemId", "ItemType", "NetValue", "ItemCurrency", "_IsRealized"]
df_contrib = (df_direct_alloc.select(*CONTRIB_COLS)
              .unionByName(df_rebate_alloc.select(*CONTRIB_COLS)))

n_contrib    = df_contrib.count()
n_reb_alloc  = df_rebate_alloc.count()
n_un_direct  = df_exc_unalloc_direct.count()
n_un_rebate  = df_exc_unalloc_rebate.count()
n_un_nots    = df_exc_unalloc_nots.count()
print(f"Item x version evaluations:      {n_iv_total}")
print(f"Allocated contributions:         {n_contrib} "
      f"(direct {n_contrib - n_reb_alloc}, rebate {n_reb_alloc})")
print(f"Unallocated -> {EXC_UNALLOCATED}: direct {n_un_direct}, "
      f"rebate {n_un_rebate}, missing-ConsumedUtc {n_un_nots}")
if n_contrib + n_un_direct + n_un_rebate + n_un_nots != n_iv_total:
    raise RuntimeError("Allocation accounting broken: contributions + exceptions != "
                       "item-version evaluations. Stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7f. Aggregate to room-night grain, currency rule, labels (NEW — D-241 / D-242)
# 
# **Plain words:** contributions are summed onto their room-night row: booked items into
# `BookedRoomRevenue_RoomNight`, realized items into `RealizedRoomRevenue_RoomNight`.
# Sums only — nothing is divided (D-240), and no rounding is applied because none is governed.
# 
# - **Currency (D-242):** the property currency comes from `D_Property.CurrencyCode`. Where
#   it is blank, or any contributing item's `Amount.Currency` does not equal it, **every**
#   currency-dependent value on that row is written blank: both amounts, both derivation
#   methods, `RevenueState` and `CurrencyCode`. No placeholder, no UNKNOWN, and — by explicit
#   D-242 decision — no exception row.
# - **Labels (D-241, as amended 2026-08-11 under I-229):** `RevenueState` is `REALIZED`
#   when the realized amount is populated, else `BOOKED` when the booked amount is
#   populated, else blank. The mixed case is governed directly: "Where both amount columns
#   are populated for the same room night, RevenueState is REALIZED" — the column separates
#   a booked-only night from a night with realized support, the same precedence D-125
#   applies to `RoomRevenue`. A partly realized night is not distinguishable from a fully
#   realized one; D-241 records that trade-off as accepted. Each derivation-method column
#   is the constant `CONSUMED` exactly when its paired amount is populated.
# 
# Expected totals and exception counts are computed here, **before** the write; Section 10f
# compares the written table against them.


# CELL ********************

# ---------------------------------------------------------------
# 7f. Aggregate + D-242 currency blanking + D-241 labels.
# ---------------------------------------------------------------
df_contrib_c = df_contrib.join(df_prop_ccy, "PropertyKey", "left")

_mismatch = (F.col("_PropCurrency").isNull() |
             F.col("ItemCurrency").isNull() |
             (F.col("ItemCurrency") != F.col("_PropCurrency")))

df_rev_raw = (df_contrib_c
    .groupBy("ReservationID", "SnapshotDateTime", "StayDate")
    .agg(
        F.sum(F.when(~F.col("_IsRealized"), F.col("NetValue"))).alias("_BookedSum"),
        F.sum(F.when(F.col("_IsRealized"),  F.col("NetValue"))).alias("_RealizedSum"),
        F.max(F.when(_mismatch, F.lit(True)).otherwise(F.lit(False))).alias("_RevCurrencyAffected"),
    ))

df_rev = df_rev_raw.select(
    "ReservationID", "SnapshotDateTime", "StayDate", "_RevCurrencyAffected",
    F.when(F.col("_RevCurrencyAffected"), F.lit(None).cast("double"))
     .otherwise(F.col("_BookedSum")).alias("_RevBooked"),
    F.when(F.col("_RevCurrencyAffected"), F.lit(None).cast("double"))
     .otherwise(F.col("_RealizedSum")).alias("_RevRealized"),
).withColumn("_RevState",
    F.when(F.col("_RevRealized").isNotNull(), F.lit(REVENUE_STATE_REALIZED))
     .when(F.col("_RevBooked").isNotNull(),   F.lit(REVENUE_STATE_BOOKED))
     .otherwise(F.lit(None).cast("string"))
).withColumn("_RevBookedMethod",
    F.when(F.col("_RevBooked").isNotNull(), F.lit(DERIVATION_METHOD))
     .otherwise(F.lit(None).cast("string"))
).withColumn("_RevRealizedMethod",
    F.when(F.col("_RevRealized").isNotNull(), F.lit(DERIVATION_METHOD))
     .otherwise(F.lit(None).cast("string")))

# Expected values for Section 10f (computed BEFORE the write).
_exp = df_rev.agg(
    F.count(F.when(F.col("_RevState").isNotNull(), 1)).alias("n_rev_rows"),
    F.sum("_RevBooked").alias("booked_total"),
    F.sum("_RevRealized").alias("realized_total"),
    F.count(F.when(F.col("_RevCurrencyAffected"), 1)).alias("n_blanked_rows"),
).collect()[0]
EXPECTED_REV = {
    "n_rev_rows":     _exp["n_rev_rows"],
    "booked_total":   _exp["booked_total"],
    "realized_total": _exp["realized_total"],
    "n_blanked_rows": _exp["n_blanked_rows"],
    "n_exc_unresolved": n_unresolved,
    "n_exc_unmapped":   n_unmapped,
    "n_exc_unallocated": n_un_direct + n_un_rebate + n_un_nots,
}
print("Expected (pre-write):", EXPECTED_REV)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Assemble the full governed column set (D-207 NULL rule + governed revenue)
# 
# **Plain words:** the `I_RoomNights` contract has **44 columns** (`03_Columns`, C-336 to
# C-414). This build fills the 31 columns whose `10_I_Field_Bindings` rows are
# **GOVERNED_FINAL** — including the six revenue columns populated in Sections 7a–7f — and
# writes **every one of the 13 UNGOVERNED columns as literal NULL**: no fallback, no
# placeholder, no UNKNOWN key, no guessed lookup.
# 
# The six revenue columns are nullable **by governed rule**, not by omission: blank means no
# qualifying revenue on the night (D-241) or a currency that could not be resolved (D-242).
# 
# Columns intentionally NULL, with the blocking issue:
# 
# - `EventID`, `BlockID` — block/event linkage deferred (D-162 / I-143)
# - `CustomerID`, `AccountID`, `MarketCountryKey` — upstream ungoverned (I-076)
# - `RoomTypeKey`, `RatePlanKey`, `SegmentKey` — lookups not governed (I-157)
# - `PMS_RoomTypeCode`, `PMS_RatePlanCode`, `PMS_SegmentCode` — upstream ungoverned (I-076)
# - `ChannelKey`, `PMS_ChannelCode` — channel mapping parked (D-170 / I-075)
# 
# **Transparency note:** several of these are marked not-null in `03_Columns`. **D-207
# explicitly overrides `NullableFlag = No` for the ungoverned columns in this slice.** The
# Delta table therefore allows NULLs; the not-null contract applies when those columns are
# actually built later.
# 
# The physical column order below follows the `03_Columns` listing order.


# CELL ********************

# ---------------------------------------------------------------
# 8. Final projection: 44 columns — governed values + governed NULLs.
# ---------------------------------------------------------------
def null_str():  return F.lit(None).cast("string")
def null_dbl():  return F.lit(None).cast("double")

# Join the revenue aggregates (7f) and the property currency (3r) onto the rows.
df_proj_in = (df_flagged
    .join(df_rev, ["ReservationID", "SnapshotDateTime", "StayDate"], "left")
    .join(df_prop_ccy, "PropertyKey", "left"))

df_final = df_proj_in.select(
    # --- governed populated ---
    F.col("RoomNightID"),                                     # BND-RN-001  D-207
    F.col("ReservationID"),                                   # BND-RN-002  D-192
    F.col("PMSReservationID"),                                # BND-RN-003  D-192
    # --- governed revenue (I-196 RESOLVED by D-240; GOVERNED_FINAL under D-244) ---
    F.col("_RevBooked").alias("BookedRoomRevenue_RoomNight"),         # BND-RN-004  D-197/D-238/D-239/D-240
    F.col("_RevBookedMethod").alias("BookedRevenueDerivationMethod"), # BND-RN-005  D-241 CONSTANT
    F.col("_RevRealized").alias("RealizedRoomRevenue_RoomNight"),     # BND-RN-006  D-197/D-238/D-239/D-240
    F.col("_RevRealizedMethod").alias("RealizedRevenueDerivationMethod"), # BND-RN-007  D-241 CONSTANT
    F.col("_RevState").alias("RevenueState"),                         # BND-RN-008  D-239/D-241
    # --- governed NULL (D-162 / I-143) ---
    null_str().alias("EventID"),                              # BND-RN-009
    # --- governed populated ---
    F.col("Children"),                                        # BND-RN-010  D-201
    F.col("LOS_Nights"),                                      # BND-RN-011  D-125/D-206
    F.col("BookedRooms").alias("RoomsBooked"),                # BND-RN-012  D-199 (naming drift, D-204 cleanup)
    F.col("BookedRoomIndex"),                                 # BND-RN-013  D-199
    F.col("BookingWindowDays"),                               # BND-RN-014  D-207
    F.col("SnapshotDateTime"),                                # BND-RN-015  D-189
    # --- governed revenue: currency (D-242) ---
    F.when(F.col("_RevCurrencyAffected") == True, null_str())
     .otherwise(F.col("_PropCurrency")).alias("CurrencyCode"),# BND-RN-016  D-242 LOOKUP
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
if n_final != n_on_rows + n_du_rows:
    raise RuntimeError("Revenue join changed the row count — a duplicate revenue key "
                       "fanned out rows. Stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Write `I_RoomNights` and the exception table
# 
# **Plain words:** the finished dataframe is saved as a Delta table with `overwrite` (full
# rebuild — safe because `I_Reservations` and the raw files are retained, so this table can
# always be rebuilt).
# 
# Four exception populations go to `I_RoomNights_DQ_Exceptions`, one row per exception,
# distinguishable by `ExceptionType` (RevMan's build decision of 2026-08-11):
# 
# | ExceptionType | Grain | Governed by |
# |---|---|---|
# | `INVALID_STAY_DATES_D207` | reservation version | D-207 |
# | `UNRESOLVED_LINK_D238` | order item | D-238 |
# | `UNMAPPED_CHARGE_TYPE_D197` | order item | D-197 |
# | `UNALLOCATED_ITEM_D240` | order item x snapshot version | D-240 |
# 
# Reservation-grain rows leave the item columns NULL and vice versa. The table is written
# only when there is at least one exception row.

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

def _dq_shape(df, has_item_cols, has_snapshot):
    """Project any exception population onto the one shared DQ schema."""
    return df.select(
        F.col("ExceptionType"),
        (F.col("ExceptionDetail") if "ExceptionDetail" in df.columns
         else F.lit(None).cast("string")).alias("ExceptionDetail"),
        (F.col("ReservationID") if "ReservationID" in df.columns
         else F.lit(None).cast("string")).alias("ReservationID"),
        (F.col("SnapshotDateTime") if has_snapshot
         else F.lit(None).cast("timestamp")).alias("SnapshotDateTime"),
        (F.col("PMSReservationID") if "PMSReservationID" in df.columns
         else F.lit(None).cast("string")).alias("PMSReservationID"),
        (F.col("ArrivalDate") if "ArrivalDate" in df.columns
         else F.lit(None).cast("date")).alias("ArrivalDate"),
        (F.col("DepartureDate") if "DepartureDate" in df.columns
         else F.lit(None).cast("date")).alias("DepartureDate"),
        (F.col("OrderItemId") if has_item_cols
         else F.lit(None).cast("string")).alias("OrderItemId"),
        (F.col("ServiceOrderId") if has_item_cols
         else F.lit(None).cast("string")).alias("ServiceOrderId"),
        (F.col("ItemType") if has_item_cols
         else F.lit(None).cast("string")).alias("OrderItemType"),
        (F.col("AccountingState") if has_item_cols
         else F.lit(None).cast("string")).alias("AccountingState"),
        (F.col("ConsumedUtc") if has_item_cols
         else F.lit(None).cast("timestamp")).alias("ConsumedUtc"),
        (F.col("NetValue") if has_item_cols
         else F.lit(None).cast("double")).alias("NetValue"),
        (F.col("ItemCurrency") if has_item_cols
         else F.lit(None).cast("string")).alias("ItemCurrency"),
        F.current_timestamp().alias("QuarantinedUtc"),
    )

_dq_parts = []
if n_invalid > 0:
    _dq_parts.append(_dq_shape(
        df_invalid.withColumn("ExceptionType", F.lit(EXC_INVALID_DATES)),
        has_item_cols=False, has_snapshot=True))
if n_unresolved > 0:
    _dq_parts.append(_dq_shape(df_exc_unresolved, has_item_cols=True, has_snapshot=False))
if n_unmapped > 0:
    _dq_parts.append(_dq_shape(df_exc_unmapped, has_item_cols=True, has_snapshot=False))
for _df_u in (df_exc_unalloc_direct, df_exc_unalloc_rebate, df_exc_unalloc_nots):
    if _df_u.limit(1).count() > 0:
        _dq_parts.append(_dq_shape(_df_u, has_item_cols=True, has_snapshot=True))

if _dq_parts:
    df_dq_all = _dq_parts[0]
    for _p in _dq_parts[1:]:
        df_dq_all = df_dq_all.unionByName(_p)
    n_dq = df_dq_all.count()
    (df_dq_all.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(DQ_TABLE))
    print(f"Wrote {DQ_TABLE}: {n_dq} rows.")
    df_dq_all.groupBy("ExceptionType").count().show(truncate=False)
else:
    print(f"No exceptions in this build — {DQ_TABLE} not written.")

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
# The six revenue columns left this list on 2026-08-11: their bindings are
# GOVERNED_FINAL under D-244 and they are populated by Sections 7a-7f.
# Their governed-rule checks live in Section 10f.
UNGOVERNED_NULL_COLS = [
    "EventID",
    "CustomerID", "AccountID", "MarketCountryKey", "BlockID",
    "RoomTypeKey", "PMS_RoomTypeCode", "RatePlanKey", "PMS_RatePlanCode",
    "ChannelKey", "PMS_ChannelCode", "SegmentKey", "PMS_SegmentCode",
]
NOT_NULL_GOVERNED_COLS = [
    "RoomNightID", "ReservationID", "LOS_Nights", "RoomsBooked", "BookedRoomIndex",
    "BookingWindowDays", "SnapshotDateTime", "SourceSystem", "PropertyKey",
    "IsGroupReservation", "BookingDateTime", "ArrivalDate", "DepartureDate", "StayDate",
    "ReservationStatusKey", "PMSStatusCode", "IsLatestCurrent",
    "TenantKey", "TenantID", "IsDayUse",
]
# (PMSReservationID, Children, Adults, StatusDateTime are nullable in the contract.
#  The six revenue columns are nullable BY GOVERNED RULE: blank when no qualifying
#  revenue exists on the night (D-241) or when currency is unresolved (D-242).)
# D-236 (FINAL): PropertyID (C-356) is nullable and remains an unpopulated,
# traceability-only attribute for as long as I_Reservations.PropertyID (C-283) is
# unpopulated under D-235/BND-RES-004. It is intentionally excluded from
# NOT_NULL_GOVERNED_COLS above. PropertyKey (already governed not-null, unchanged
# by D-236) remains the sole relationship key.

print(f"Ungoverned columns written NULL this slice: {len(UNGOVERNED_NULL_COLS)}")
all_ok = True
for c in UNGOVERNED_NULL_COLS:
    n_not_null = t.filter(F.col(c).isNotNull()).count()
    ok = n_not_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_not_null} non-NULL values -> FAIL (must be 100% NULL)")
print(f"  100% NULL on all 13 ungoverned columns -> {'OK' if all_ok else 'FAIL'}")

all_ok = True
for c in NOT_NULL_GOVERNED_COLS:
    n_null = t.filter(F.col(c).isNull()).count()
    ok = n_null == 0
    all_ok = all_ok and ok
    if not ok:
        print(f"  {c}: {n_null} NULL values -> FAIL (governed not-null)")
print(f"  0% NULL on governed not-null columns -> {'OK' if all_ok else 'FAIL'}")

# D-236 (FINAL): PropertyID (C-356) is nullable, ACTIVE, and expected to remain
# 100% NULL for now (traceability-only; never a resolution/relationship key).
n_propertyid_not_null = t.filter(F.col("PropertyID").isNotNull()).count()
print(f"  PropertyID: {n_propertyid_not_null} non-NULL values -> "
      f"{'OK' if n_propertyid_not_null == 0 else 'FAIL'} (D-236: expected 100% NULL for now)")

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

# ## 10f. Revenue validation (NEW) — prove the governed rules on the written table
# 
# **Plain words:** the old check asserted the six columns were 100% NULL. That assertion is
# replaced by checks that test the governed rules themselves, on the **written** table:
# 
# 1. **Value lists (D-241):** `RevenueState` only `BOOKED`/`REALIZED`; methods only `CONSUMED`.
# 2. **Pairing (D-241):** each derivation method present exactly when its paired amount is;
#    `RevenueState` present exactly when at least one amount is.
# 3. **Currency coherence (D-242):** a blank `CurrencyCode` row carries no currency-dependent
#    value; a row with an amount always has a `CurrencyCode`.
# 4. **Reconciliation:** written totals and revenue-row counts equal the pre-write
#    expectations from Section 7f (small float tolerance for sum association only — no
#    rounding rule exists and none is applied).
# 5. **Exception accounting:** the written DQ table carries exactly the expected counts per
#    `ExceptionType`, and the three revenue populations stay distinguishable.
# 6. **Visibility, not a check:** revenue split by reservation status. Under I-186 (OPEN), a
#    charged no-show maps to `Canceled`, so its revenue vanishes from every deduct-filtered
#    figure and appears in every unfiltered one. Printed so the number is read correctly —
#    no fix is invented here.


# CELL ********************

# ---------------------------------------------------------------
# 10f. Revenue checks on the written table (D-241 / D-242 / totals / DQ).
# ---------------------------------------------------------------
TOL = 0.01   # float-sum association tolerance only; no governed rounding exists

# 1. Value lists (D-241).
n_bad_state = t.filter(F.col("RevenueState").isNotNull() &
    ~F.col("RevenueState").isin([REVENUE_STATE_BOOKED, REVENUE_STATE_REALIZED])).count()
n_bad_bm = t.filter(F.col("BookedRevenueDerivationMethod").isNotNull() &
    (F.col("BookedRevenueDerivationMethod") != DERIVATION_METHOD)).count()
n_bad_rm = t.filter(F.col("RealizedRevenueDerivationMethod").isNotNull() &
    (F.col("RealizedRevenueDerivationMethod") != DERIVATION_METHOD)).count()
print(f"RevenueState outside governed list:      {n_bad_state} -> {'OK' if n_bad_state == 0 else 'FAIL'}")
print(f"Booked method outside governed list:     {n_bad_bm} -> {'OK' if n_bad_bm == 0 else 'FAIL'}")
print(f"Realized method outside governed list:   {n_bad_rm} -> {'OK' if n_bad_rm == 0 else 'FAIL'}")

# 2. Pairing (D-241).
n_pair_b = t.filter(F.col("BookedRoomRevenue_RoomNight").isNotNull() !=
                    F.col("BookedRevenueDerivationMethod").isNotNull()).count()
n_pair_r = t.filter(F.col("RealizedRoomRevenue_RoomNight").isNotNull() !=
                    F.col("RealizedRevenueDerivationMethod").isNotNull()).count()
n_pair_s = t.filter(F.col("RevenueState").isNotNull() !=
                    (F.col("BookedRoomRevenue_RoomNight").isNotNull() |
                     F.col("RealizedRoomRevenue_RoomNight").isNotNull())).count()
print(f"Booked amount/method pairing breaks:     {n_pair_b} -> {'OK' if n_pair_b == 0 else 'FAIL'}")
print(f"Realized amount/method pairing breaks:   {n_pair_r} -> {'OK' if n_pair_r == 0 else 'FAIL'}")
print(f"RevenueState presence breaks:            {n_pair_s} -> {'OK' if n_pair_s == 0 else 'FAIL'}")

# 3. Currency coherence (D-242).
n_ccy_a = t.filter(F.col("CurrencyCode").isNull() &
    (F.col("BookedRoomRevenue_RoomNight").isNotNull() |
     F.col("RealizedRoomRevenue_RoomNight").isNotNull() |
     F.col("BookedRevenueDerivationMethod").isNotNull() |
     F.col("RealizedRevenueDerivationMethod").isNotNull() |
     F.col("RevenueState").isNotNull())).count()
print(f"Blank-CurrencyCode rows carrying values: {n_ccy_a} -> {'OK' if n_ccy_a == 0 else 'FAIL'}")

# 4. Reconciliation against pre-write expectations (Section 7f).
_w = t.agg(
    F.count(F.when(F.col("RevenueState").isNotNull(), 1)).alias("n_rev_rows"),
    F.sum("BookedRoomRevenue_RoomNight").alias("booked_total"),
    F.sum("RealizedRoomRevenue_RoomNight").alias("realized_total")).collect()[0]
def _close(a, b):
    if a is None and b is None: return True
    if a is None or b is None:  return False
    return abs(a - b) <= TOL
ok_rows  = _w["n_rev_rows"] == EXPECTED_REV["n_rev_rows"]
ok_book  = _close(_w["booked_total"],   EXPECTED_REV["booked_total"])
ok_real  = _close(_w["realized_total"], EXPECTED_REV["realized_total"])
print(f"Revenue rows written:   {_w['n_rev_rows']} (expected {EXPECTED_REV['n_rev_rows']}) -> {'OK' if ok_rows else 'FAIL'}")
print(f"Booked total written:   {_w['booked_total']} (expected {EXPECTED_REV['booked_total']}) -> {'OK' if ok_book else 'FAIL'}")
print(f"Realized total written: {_w['realized_total']} (expected {EXPECTED_REV['realized_total']}) -> {'OK' if ok_real else 'FAIL'}")
print(f"Rows blanked under D-242: {EXPECTED_REV['n_blanked_rows']} (visibility)")

# 5. Exception accounting on the written DQ table.
_expected_dq = {
    EXC_UNRESOLVED_LINK: EXPECTED_REV["n_exc_unresolved"],
    EXC_UNMAPPED_TYPE:   EXPECTED_REV["n_exc_unmapped"],
    EXC_UNALLOCATED:     EXPECTED_REV["n_exc_unallocated"],
    EXC_INVALID_DATES:   n_invalid,
}
if any(v > 0 for v in _expected_dq.values()):
    t_dq = spark.read.table(DQ_TABLE)
    _got = {r["ExceptionType"]: r["count"]
            for r in t_dq.groupBy("ExceptionType").count().collect()}
    for k, v in _expected_dq.items():
        g = _got.get(k, 0)
        print(f"DQ {k}: written {g} (expected {v}) -> {'OK' if g == v else 'FAIL'}")
else:
    print("No exceptions expected; DQ table intentionally not written.")

# 6. Visibility: revenue by reservation status (I-186 context, not a check).
print("\nRevenue by ReservationStatusKey (I-186: no-show revenue sits on Canceled "
      "status rows and drops out of any deduct-filtered figure):")
t.groupBy("ReservationStatusKey").agg(
    F.sum("BookedRoomRevenue_RoomNight").alias("BookedSum"),
    F.sum("RealizedRoomRevenue_RoomNight").alias("RealizedSum"),
    F.count(F.when(F.col("RevenueState").isNotNull(), 1)).alias("RevenueRows"),
).orderBy("ReservationStatusKey").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Wrap-up — honest status
# 
# **Plain words:** when this notebook has run, `I_RoomNights` exists in the DEV lakehouse
# with the six governed revenue columns populated — nothing more. It is **not** done,
# committed, or governed-complete until:
# 
# 1. You confirm the run and inspect the Section 10 results, including the new 10f revenue
#    checks and the DQ exception counts.
# 2. You commit the notebook to GitHub (`menja-bi/menja-bi-v1`) if you want it versioned.
# 3. Any follow-up documentation is handed to Copilot.
# 
# Out of scope, still blocked, unchanged: room-type / rate-plan / channel / segment lookups,
# block/event linkage, `F_RoomNights` (NB30 carries revenue under D-245 in its own
# amendment), measures, multi-room mechanics, no-show treatment (I-186), service-scope
# guard (I-226), missing-timezone treatment (I-227).
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working, to avoid
# unnecessary cost.**

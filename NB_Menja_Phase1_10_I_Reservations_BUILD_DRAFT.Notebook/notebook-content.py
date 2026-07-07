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

# # I_Reservations — BUILD_DRAFT — Phase-1 Mews slice
# 
# **Status: DRAFT. Not run. Not confirmed. Not governance authority.**
# 
# **What this notebook does, in plain words:**
# It reads the raw Mews reservation files already landed in the lakehouse, plus the raw services
# and age-category files, plus the governed manual seed workbook, and builds the first governed
# version of the `I_Reservations` table. `I_Reservations` is the cleaned, standardized reservation
# table that everything else will later be built from.
# 
# **Governance basis (all FINAL):**
# - D-203 — defines exactly which columns this slice may fill. Everything else is written as NULL.
# - D-189 — `SnapshotDateTime` = Mews `UpdatedUtc` (when Mews last changed the reservation).
# - D-190 — reservation status resolved through the `D_ReservationStatus` seed; unmatched → UNKNOWN.
# - D-191 — `StatusDateTime` per status, with an EXACT / OBSERVED basis marker.
# - D-192 — identity columns: `ReservationID`, `PMSReservationID`, `BookingDateTime`.
# - D-193 — arrival/departure dates: convert UTC to the property's local time zone FIRST, then take the date.
# - D-195 + D-205 — property found via: reservation → service → enterprise → `D_Property.PMS_PropertyCode`.
# - D-198 — seed workbook read with an explicit sheet allow-list; `_` sheets ignored.
# - D-199 — one row = one booked room/space unit; `BookedRooms = 1`.
# - D-200 — `IsGroupReservation = FALSE` (safe interim rule).
# - D-201 — Adults/Children from `PersonCounts` joined to age-category `Classification`.
# - D-177 — tenant resolved from the governed property → tenant manual input.
# - D-143 — `IsLatestCurrent` = latest stored row per reservation.
# 
# **Explicitly NOT in this notebook (per D-203):**
# rate plan, segment, channel, room type, account/company, customer, revenue, revenue normalization,
# market country, block pickup, any positive group classification, I_RoomNights, D tables, F tables,
# gold layer. Those columns are written as NULL, with no fallback and no invented values.
# 
# **Rows that fail a governed lookup are not guessed.** They go to a quarantine table and are
# excluded from `I_Reservations`, so the numbers stay honest and the failures stay visible.


# MARKDOWN ********************

# ## 1. Parameters and lakehouse check
# 
# **Plain words:** everything you might need to adjust lives here, in one visible place.
# No hidden defaults further down.
# 
# Things you must confirm before running:
# - `SEED_XLSX_PATH` — where you uploaded the seed workbook in the lakehouse Files area.
# - `RESERVATIONS_ENDPOINT` — the exact `Endpoint` string used in `ExtractionRunLog` for reservations
#   (Section 2 shows you the real values so you can copy the right one).
# - `TENANT_*` — which seed sheet carries the property → tenant mapping (see Section 6c).
# 
# The last cell here just checks that the notebook is attached to the right lakehouse.
# If it is not, stop and attach `LH_Menja_BI_v1_Mews_DEV` before running anything else.


# CELL ********************

# ---------------------------------------------------------------
# 1. PARAMETERS — the only place with settings. No hidden defaults.
# ---------------------------------------------------------------

# --- source system constant (D-122 / BND-RES-032) ---
SOURCE_SYSTEM = "MEWS"

# --- extraction log tables (D-186) ---
RUN_LOG_TABLE  = "ExtractionRunLog"
FILE_LOG_TABLE = "ExtractionFileLog"

# --- endpoint names as they appear in ExtractionRunLog.Endpoint ---
# CONFIRM against the discovery cell in Section 2 before running Section 3.
RESERVATIONS_ENDPOINT   = "reservations/getAll"     # <-- CONFIRM exact value
SERVICES_ENDPOINT       = "services/getAll"
AGE_CATEGORIES_ENDPOINT = "ageCategories/getAll"

# --- top-level JSON array key per endpoint (Mews response shape) ---
RESERVATIONS_JSON_KEY   = "Reservations"
SERVICES_JSON_KEY       = "Services"
AGE_CATEGORIES_JSON_KEY = "AgeCategories"

# --- seed workbook (D-198): runtime copy in lakehouse Files ---
# Authoritative copy stays in OneDrive. This is only the runtime copy.
SEED_XLSX_PATH = "/lakehouse/default/Files/Seeds/Menja_Dimension_Seed_Input_DRAFT.xlsx"  # <-- CONFIRM

# D-198 explicit allow-list. Only sheets this notebook actually needs.
# Sheets starting with "_" are documentation/control and are never read.
SEED_SHEETS_NEEDED = ["D_Property", "D_ReservationStatus"]

# --- D_ReservationStatus seed: column that holds the verbatim Mews State ---
# D-190: the seed match key must equal the exact Mews State spelling/casing.
# CONFIRM the real column name in your seed sheet (Section 6b fails loudly if wrong).
STATUS_SEED_PMS_STATE_COL = "PMSStatusCode"   # <-- CONFIRM column name in seed sheet

# --- governed UNKNOWN status member key (D-190) ---
STATUS_UNKNOWN_KEY = "UNKNOWN"                # <-- CONFIRM key value in seed sheet

# --- tenant mapping (D-177): property -> tenant manual input ---
# The binding references the T_MI_Property manual input. Where that mapping
# lives inside the current seed workbook must be CONFIRMED by you.
TENANT_SEED_SHEET      = "D_Property"         # <-- CONFIRM sheet name
TENANT_PROPERTY_COL    = "PropertyID"         # <-- CONFIRM join column
TENANT_KEY_COL         = "TenantKey"          # <-- CONFIRM
TENANT_ID_COL          = "TenantID"           # <-- CONFIRM

# --- target + quarantine tables ---
TARGET_TABLE     = "I_Reservations"
QUARANTINE_TABLE = "I_Reservations_DQ_Quarantine"   # naming is implementation detail, not governed

# --- write mode for this BUILD_DRAFT ---
# Initial full build: rebuild from retained raw (allowed by D-148/D-151 retention).
# Incremental change-aware appends (D-188 trigger comparison) are a LATER section, not this draft.
WRITE_MODE = "overwrite"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 1b. Lakehouse attachment check — fail loudly if not attached.
# ---------------------------------------------------------------
import os

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"

if not os.path.isdir(LAKEHOUSE_FILES_ROOT):
    raise RuntimeError(
        "No default lakehouse attached. Attach LH_Menja_BI_v1_Mews_DEV to this "
        "notebook, then re-run this cell."
    )

print("Default lakehouse Files area is reachable:", LAKEHOUSE_FILES_ROOT)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Discovery — what did the extractor actually log?
# 
# **Plain words:** instead of guessing folder paths, this notebook uses the two governed
# log tables written by the landing notebook (D-186):
# 
# - `ExtractionRunLog` — one row per extraction run ("I called Mews at this time, status Success/Failed").
# - `ExtractionFileLog` — one row per raw file written, linked to its run.
# 
# This cell only *shows* what is in the run log. Use it to confirm the three
# `*_ENDPOINT` parameter values in Section 1 match the real `Endpoint` strings.
# Nothing is transformed here.


# CELL ********************

# ---------------------------------------------------------------
# 2. Show distinct endpoints and run statuses from ExtractionRunLog.
# ---------------------------------------------------------------
from pyspark.sql import functions as F

run_log = spark.read.table(RUN_LOG_TABLE)

print("Distinct Endpoint / Status combinations in ExtractionRunLog:")
(run_log
 .groupBy("Endpoint", "Status")
 .agg(F.count("*").alias("Runs"),
      F.max("RunEndUtc").alias("LatestRunEndUtc"))
 .orderBy("Endpoint", "Status")
 .show(50, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Pick raw files through the logs
# 
# **Plain words:** we now choose which raw files to read.
# 
# - **Reservations:** read the files of **all successful runs**. Raw is never overwritten
#   (D-148/D-151), and the snapshot logic later in this notebook collapses duplicates,
#   so reading everything is safe and lets the table be rebuilt from scratch.
# - **Services and age categories:** these are slow-changing configuration, and the governed
#   rule (D-195, D-201) says use the **current/latest snapshot** — so we take only the files
#   of the **latest successful run** per endpoint.
# 
# If any endpoint has no successful run, the cell stops with a clear message instead of
# continuing with missing inputs.


# CELL ********************

# ---------------------------------------------------------------
# 3. Resolve raw file paths from the D-186 logs.
# ---------------------------------------------------------------
file_log = spark.read.table(FILE_LOG_TABLE)

def _success_runs(endpoint):
    df = run_log.filter((F.col("Endpoint") == endpoint) & (F.col("Status") == "Success"))
    if df.count() == 0:
        raise RuntimeError(
            f"No ExtractionRunLog row with Status='Success' for Endpoint='{endpoint}'. "
            "Check the Section 2 output and fix the endpoint parameter or the logs."
        )
    return df

def files_for_all_success_runs(endpoint):
    runs = _success_runs(endpoint).select("RunID")
    return [r["FilePath"] for r in
            file_log.join(runs, "RunID").select("FilePath").collect()]

def files_for_latest_success_run(endpoint):
    latest = (_success_runs(endpoint)
              .orderBy(F.col("RunEndUtc").desc())
              .select("RunID").first()["RunID"])
    return [r["FilePath"] for r in
            file_log.filter(F.col("RunID") == latest).select("FilePath").collect()]

reservation_files = files_for_all_success_runs(RESERVATIONS_ENDPOINT)
service_files     = files_for_latest_success_run(SERVICES_ENDPOINT)
agecat_files      = files_for_latest_success_run(AGE_CATEGORIES_ENDPOINT)

print(f"Reservation raw files (all successful runs): {len(reservation_files)}")
print(f"Service raw files (latest successful run):   {len(service_files)}")
print(f"Age-category raw files (latest run):         {len(agecat_files)}")
for p in (reservation_files + service_files + agecat_files):
    print("  ", p)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 3b. Verify every logged file actually exists on disk.
#     FilePath values may be lakehouse-relative; normalize visibly.
# ---------------------------------------------------------------
def normalize_path(p):
    if p.startswith("/lakehouse/"):
        return p
    if p.startswith("Files/"):
        return "/lakehouse/default/" + p
    if p.startswith("/Files/"):
        return "/lakehouse/default" + p
    return p  # unknown shape — the existence check below will catch it

reservation_files = [normalize_path(p) for p in reservation_files]
service_files     = [normalize_path(p) for p in service_files]
agecat_files      = [normalize_path(p) for p in agecat_files]

missing = [p for p in reservation_files + service_files + agecat_files
           if not os.path.isfile(p)]
if missing:
    raise RuntimeError("Logged raw files not found on disk:\n" + "\n".join(missing))

print("All logged raw files exist on disk. OK.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Parse the raw JSON into three dataframes
# 
# **Plain words:** each raw file is one JSON document from Mews, holding one big array
# (`Reservations`, `Services`, or `AgeCategories`). We load each file, pull out that array,
# and stack everything into one Spark dataframe per endpoint. A **dataframe** is just a
# table held in memory while the notebook runs.
# 
# No filtering, no renaming, no business logic here — parse only.
# The validation prints show the raw row counts so you can compare them
# with the `RecordCount` values you verified in the logs (services 495, age categories 333).


# CELL ********************

# ---------------------------------------------------------------
# 4. Parse raw JSON files. Parse only — no business logic.
# ---------------------------------------------------------------
import json as _json

def load_json_array(file_paths, top_key):
    """Read each raw file, take the array under top_key, return list of dicts."""
    records = []
    for path in file_paths:
        with open(path, "r", encoding="utf-8") as fh:
            doc = _json.load(fh)
        if top_key not in doc:
            raise RuntimeError(f"Key '{top_key}' not found in {path}. "
                               f"Top-level keys: {list(doc.keys())}")
        arr = doc[top_key]
        if not isinstance(arr, list):
            raise RuntimeError(f"'{top_key}' in {path} is not a list.")
        records.extend(arr)
    return records

raw_reservations = load_json_array(reservation_files, RESERVATIONS_JSON_KEY)
raw_services     = load_json_array(service_files,     SERVICES_JSON_KEY)
raw_agecats      = load_json_array(agecat_files,      AGE_CATEGORIES_JSON_KEY)

print(f"Parsed reservation records (all runs, before dedupe): {len(raw_reservations)}")
print(f"Parsed service records (latest run):                  {len(raw_services)}")
print(f"Parsed age-category records (latest run):             {len(raw_agecats)}")

# Spark dataframes. Full sampling so nested fields are typed correctly.
spark.conf.set("spark.sql.session.timeZone", "UTC")
df_res_raw = spark.read.json(spark.sparkContext.parallelize(
    [_json.dumps(r) for r in raw_reservations]))
df_svc_raw = spark.read.json(spark.sparkContext.parallelize(
    [_json.dumps(r) for r in raw_services]))
df_age_raw = spark.read.json(spark.sparkContext.parallelize(
    [_json.dumps(r) for r in raw_agecats]))

print("\nReservation raw schema (top level):")
for f in df_res_raw.schema.fields:
    print("  ", f.name, "-", f.dataType.simpleString()[:60])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Load the governed seeds (D-198)
# 
# **Plain words:** the seed workbook is the hand-maintained Excel file with reference data
# that Mews cannot give us — the governed property list with time zones, and the governed
# reservation-status mapping.
# 
# D-198 rules applied here, literally:
# - Only sheets on the explicit allow-list are read.
# - Any sheet starting with `_` is documentation and is never imported.
# - Seed content is runtime input, **not** governance authority.
# 
# After loading, a **readiness gate** checks the D-203 hard prerequisite:
# every `D_Property` seed row must have `PMS_PropertyCode` (the Mews EnterpriseId)
# and `TimeZone` filled. If not, the notebook stops — because arrival/departure
# dates and the property lookup are not allowed to run without them.


# CELL ********************

# ---------------------------------------------------------------
# 5. Seed loading with explicit allow-list (D-198).
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
    pdf = pdf.dropna(how="all")                      # drop fully empty rows
    seed[sheet] = pdf
    print(f"Loaded seed sheet '{sheet}': {len(pdf)} rows, columns: {list(pdf.columns)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 5b. D_Property seed readiness gate (hard prerequisite in D-203).
# ---------------------------------------------------------------
prop_pdf = seed["D_Property"]

required_prop_cols = ["PropertyKey", "PropertyID", "PMS_PropertyCode", "TimeZone"]
missing_cols = [c for c in required_prop_cols if c not in prop_pdf.columns]
if missing_cols:
    raise RuntimeError(
        f"D_Property seed is missing required columns {missing_cols}. "
        "D-203 blocks Phase-1 until the seed carries PMS_PropertyCode and TimeZone."
    )

blank = prop_pdf[required_prop_cols].isna().any(axis=1)
if blank.any():
    raise RuntimeError(
        "D_Property seed readiness FAILED — rows with blank "
        "PropertyKey / PropertyID / PMS_PropertyCode / TimeZone:\n"
        + prop_pdf[blank].to_string()
    )

dup = prop_pdf["PMS_PropertyCode"].duplicated()
if dup.any():
    raise RuntimeError("Duplicate PMS_PropertyCode values in D_Property seed — "
                       "property lookup would be ambiguous:\n"
                       + prop_pdf[dup].to_string())

df_seed_property = spark.createDataFrame(prop_pdf[required_prop_cols])
print("D_Property seed readiness OK:", prop_pdf.shape[0], "properties.")
df_seed_property.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 5c. D_ReservationStatus seed checks (D-190).
# ---------------------------------------------------------------
status_pdf = seed["D_ReservationStatus"]

required_status_cols = ["ReservationStatusKey", STATUS_SEED_PMS_STATE_COL]
missing_cols = [c for c in required_status_cols if c not in status_pdf.columns]
if missing_cols:
    raise RuntimeError(
        f"D_ReservationStatus seed is missing columns {missing_cols}. "
        f"Actual columns: {list(status_pdf.columns)}. "
        "Confirm STATUS_SEED_PMS_STATE_COL in Section 1 against the real sheet."
    )

if STATUS_UNKNOWN_KEY not in set(status_pdf["ReservationStatusKey"].dropna()):
    raise RuntimeError(
        f"Governed UNKNOWN member '{STATUS_UNKNOWN_KEY}' not found in the "
        "D_ReservationStatus seed. D-190 requires it before unmatched states can be routed."
    )

dup = status_pdf[STATUS_SEED_PMS_STATE_COL].dropna().duplicated()
if dup.any():
    raise RuntimeError("Duplicate PMS state values in D_ReservationStatus seed — "
                       "status lookup would be ambiguous.")

df_seed_status = spark.createDataFrame(
    status_pdf[["ReservationStatusKey", STATUS_SEED_PMS_STATE_COL]]
    .rename(columns={STATUS_SEED_PMS_STATE_COL: "_SeedPMSState"}))
print("D_ReservationStatus seed OK.")
df_seed_status.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Base reservation columns — the governed backbone
# 
# **Plain words:** we now take, from each raw reservation, only the columns whose source
# is FINAL-governed and needs no lookup:
# 
# | Target column | Source | Decision |
# |---|---|---|
# | `ReservationID` | `Reservations[].Id`, as text | D-192 |
# | `PMSReservationID` | `Reservations[].Number`, as text | D-192 |
# | `BookingDateTime` | `Reservations[].CreatedUtc` | D-192 |
# | `PMSStatusCode` | `Reservations[].State`, verbatim | D-190 |
# | `SnapshotDateTime` | `Reservations[].UpdatedUtc` | D-189 |
# | `BookedRooms` | constant 1 | D-199 |
# | `IsGroupReservation` | constant FALSE (interim safe rule) | D-200 |
# | `SourceSystem` | constant `MEWS` | D-122 |
# 
# We also keep a few raw helper fields (`ServiceId`, timestamps, `PersonCounts`) that later
# sections need. Helper fields never end up in the final table.


# CELL ********************

# ---------------------------------------------------------------
# 6. Governed backbone columns from raw reservations.
# ---------------------------------------------------------------
from pyspark.sql import types as T

df_base = df_res_raw.select(
    F.col("Id").cast("string").alias("ReservationID"),                     # D-192
    F.col("Number").cast("string").alias("PMSReservationID"),              # D-192
    F.to_timestamp("CreatedUtc").alias("BookingDateTime"),                 # D-192
    F.col("State").cast("string").alias("PMSStatusCode"),                  # D-190, verbatim
    F.to_timestamp("UpdatedUtc").alias("SnapshotDateTime"),                # D-189
    F.lit(1).cast("long").alias("BookedRooms"),                            # D-199
    F.lit(False).alias("IsGroupReservation"),                              # D-200
    F.lit(SOURCE_SYSTEM).alias("SourceSystem"),                            # D-122
    # helper fields for later governed sections (dropped before write):
    F.col("ServiceId").cast("string").alias("_ServiceId"),                 # D-195 input
    F.to_timestamp("ScheduledStartUtc").alias("_ScheduledStartUtc"),       # D-193 input
    F.to_timestamp("ScheduledEndUtc").alias("_ScheduledEndUtc"),           # D-193 input
    F.to_timestamp("CancelledUtc").alias("_CancelledUtc"),                 # D-191 input
    F.to_timestamp("ActualStartUtc").alias("_ActualStartUtc"),             # D-191 input
    F.to_timestamp("ActualEndUtc").alias("_ActualEndUtc"),                 # D-191 input
    F.col("PersonCounts").alias("_PersonCounts"),                          # D-201 input
)

n_base = df_base.count()
print(f"Backbone rows (before dedupe): {n_base}")

# Hard identity checks: these columns are NOT NULL in the governed contract.
for c in ["ReservationID", "SnapshotDateTime", "PMSStatusCode"]:
    n_null = df_base.filter(F.col(c).isNull()).count()
    if n_null:
        raise RuntimeError(f"{n_null} raw reservations have NULL {c} — "
                           "cannot build the governed grain. Inspect raw before continuing.")
print("Identity null checks OK.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Grain, duplicates, and collision quarantine (D-189)
# 
# **Plain words:** the governed grain is **one row per `ReservationID` + `SnapshotDateTime`**.
# Because we read every successful extraction run, the same reservation state can appear
# several times (two runs saw the same unchanged reservation). Those are *exact duplicates*
# and simply collapse to one row — that is the whole point of using Mews's own
# `UpdatedUtc` as the snapshot time.
# 
# The dangerous case is a **collision**: same `ReservationID` + same `SnapshotDateTime`,
# but the governed content differs. D-189 says: never fabricate a timestamp, never overwrite —
# **quarantine** the conflicting rows (set them aside in a separate table) and continue.
# 
# Note on scope: comparing an *incoming* row against an *already stored* version using the
# D-188 trigger-field list is incremental-load logic. This BUILD_DRAFT is an initial full
# build into an empty table, so that comparison is deliberately left to a later section.


# CELL ********************

# ---------------------------------------------------------------
# 7. Collapse exact duplicates; quarantine same-key collisions (D-189).
# ---------------------------------------------------------------
grain = ["ReservationID", "SnapshotDateTime"]

# Governed comparison fields available at this stage (raw-level content check).
compare_cols = ["PMSReservationID", "BookingDateTime", "PMSStatusCode",
                "_ServiceId", "_ScheduledStartUtc", "_ScheduledEndUtc",
                "_CancelledUtc", "_ActualStartUtc", "_ActualEndUtc"]

# 7a. exact duplicates (same grain, same content) -> keep one
df_distinct = df_base.dropDuplicates(grain + compare_cols)
n_exact_dupes = n_base - df_distinct.count()

# 7b. collisions: same grain key still appearing more than once => content differs
key_counts = df_distinct.groupBy(grain).count()
collision_keys = key_counts.filter(F.col("count") > 1).select(grain)

df_collisions = df_distinct.join(collision_keys, grain, "inner")
df_versions   = df_distinct.join(collision_keys, grain, "left_anti")

n_collisions = df_collisions.count()
print(f"Exact duplicate rows collapsed:            {n_exact_dupes}")
print(f"Collision rows quarantined (D-189):        {n_collisions}")
print(f"Reservation versions continuing:           {df_versions.count()}")

quarantine_batches = []   # (reason, dataframe of grain keys + note)
if n_collisions > 0:
    quarantine_batches.append(("SNAPSHOT_COLLISION_D189",
                               df_collisions.select(*grain, "PMSReservationID")))
    df_collisions.select(*grain, "PMSStatusCode", "PMSReservationID").show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Property resolution (D-195 + D-205)
# 
# **Plain words:** Mews reservations do not say which hotel they belong to. The governed path is:
# 
# `reservation.ServiceId` → find that service in the services file → read the service's
# `EnterpriseId` (Mews's word for the hotel) → match it to `PMS_PropertyCode` in the
# `D_Property` seed → take that row's `PropertyKey` and the governed `PropertyID`.
# 
# Rules applied literally:
# - The services **latest snapshot** is used (services are slow-changing configuration).
# - A reservation whose link breaks anywhere (unknown ServiceId, service without EnterpriseId,
#   EnterpriseId not in the seed) is a **data-quality error row** — quarantined, never guessed.
# - `PropertyID` is populated with the resolved enterprise identifier per BND-RES-004 (D-195).


# CELL ********************

# ---------------------------------------------------------------
# 8. Property lookup: ServiceId -> Services.EnterpriseId -> D_Property.
# ---------------------------------------------------------------
df_services = (df_svc_raw
    .select(F.col("Id").cast("string").alias("_SvcId"),
            F.col("EnterpriseId").cast("string").alias("_EnterpriseId"))
    .dropDuplicates(["_SvcId"]))

df_prop = (df_versions
    .join(df_services, df_versions._ServiceId == df_services._SvcId, "left")
    .join(df_seed_property.select(
              F.col("PMS_PropertyCode").alias("_PMS_PropertyCode"),
              F.col("PropertyKey"),
              F.col("PropertyID"),
              F.col("TimeZone").alias("_TimeZone")),
          F.col("_EnterpriseId") == F.col("_PMS_PropertyCode"), "left"))

df_prop_failed = df_prop.filter(F.col("PropertyKey").isNull())
df_resolved    = df_prop.filter(F.col("PropertyKey").isNotNull())                         .drop("_SvcId", "_PMS_PropertyCode")

n_prop_failed = df_prop_failed.count()
print(f"Property lookup failed (quarantined, not guessed): {n_prop_failed}")
print(f"Rows with resolved property:                       {df_resolved.count()}")

if n_prop_failed > 0:
    quarantine_batches.append(("PROPERTY_LOOKUP_FAILED_D195",
                               df_prop_failed.select("ReservationID", "SnapshotDateTime",
                                                     "PMSReservationID")))
    (df_prop_failed
     .groupBy("_ServiceId", "_EnterpriseId").count()
     .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Arrival and departure dates (D-193)
# 
# **Plain words:** Mews stores the planned stay boundaries as UTC timestamps
# (`ScheduledStartUtc`, `ScheduledEndUtc`). A hotel night is defined by the hotel's
# **local** clock, so the governed rule has a fixed order:
# 
# 1. Convert the UTC timestamp to the property's local time zone (from the `D_Property` seed).
# 2. Only then cut off the time part and keep the calendar date.
# 
# Never the other way around — taking the date first and converting afterwards can shift
# a stay by a whole day. These are **scheduled** boundaries, not actual check-in/check-out
# times; those are a different concept and are out of BI v1 scope.
# 
# D-203 allows these two columns only because the property lookup resolved (Section 8)
# and the seed readiness gate passed (Section 5b) — both are already guaranteed here.


# CELL ********************

# ---------------------------------------------------------------
# 9. UTC -> property-local time zone FIRST, then date (D-193).
# ---------------------------------------------------------------
df_dated = (df_resolved
    .withColumn("ArrivalDate",
        F.to_date(F.from_utc_timestamp(F.col("_ScheduledStartUtc"), F.col("_TimeZone"))))
    .withColumn("DepartureDate",
        F.to_date(F.from_utc_timestamp(F.col("_ScheduledEndUtc"), F.col("_TimeZone")))))

# Sanity: governed rule says ArrivalDate must be on or before DepartureDate.
n_bad_order = df_dated.filter(F.col("ArrivalDate") > F.col("DepartureDate")).count()
n_null_arr  = df_dated.filter(F.col("ArrivalDate").isNull()).count()
n_null_dep  = df_dated.filter(F.col("DepartureDate").isNull()).count()
print(f"ArrivalDate > DepartureDate rows: {n_bad_order}  (expected 0)")
print(f"NULL ArrivalDate rows:            {n_null_arr}   (expected 0)")
print(f"NULL DepartureDate rows:          {n_null_dep}   (expected 0)")
if n_bad_order or n_null_arr or n_null_dep:
    raise RuntimeError("Stay-date validation failed — inspect raw scheduled timestamps "
                       "and seed TimeZone values before continuing.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Reservation status mapping (D-190)
# 
# **Plain words:** `PMSStatusCode` keeps the exact Mews wording (e.g. `Canceled` with one L).
# `ReservationStatusKey` is the Menja business status, and it may **only** come from the
# governed `D_ReservationStatus` seed — never written straight from the raw string.
# 
# If Mews sends a state that is not in the seed, the row is routed to the governed
# `UNKNOWN` member. That is not an error stop — it is a visible, counted signal
# that the seed needs a new governed row.


# CELL ********************

# ---------------------------------------------------------------
# 10. Seed lookup on the exact Mews State spelling (D-190).
# ---------------------------------------------------------------
df_status = (df_dated
    .join(df_seed_status, df_dated.PMSStatusCode == df_seed_status._SeedPMSState, "left")
    .withColumn("ReservationStatusKey",
                F.coalesce(F.col("ReservationStatusKey"), F.lit(STATUS_UNKNOWN_KEY)))
    .drop("_SeedPMSState"))

unknown_status = (df_status
    .filter(F.col("ReservationStatusKey") == STATUS_UNKNOWN_KEY)
    .groupBy("PMSStatusCode").count())

n_unknown = df_status.filter(F.col("ReservationStatusKey") == STATUS_UNKNOWN_KEY).count()
print(f"Rows routed to governed UNKNOWN status: {n_unknown}")
if n_unknown > 0:
    print("Unmatched Mews State values (seed likely needs a governed update):")
    unknown_status.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. StatusDateTime and its basis marker (D-191)
# 
# **Plain words:** each snapshot row gets the best available timestamp for its **current**
# status, plus a marker saying how trustworthy that timestamp is:
# 
# | Status | Timestamp used | Basis |
# |---|---|---|
# | Canceled | Mews `CancelledUtc` | EXACT — Mews recorded the exact moment |
# | Started | Mews `ActualStartUtc` | EXACT |
# | Processed | Mews `ActualEndUtc` | EXACT |
# | Confirmed / Optional | the `SnapshotDateTime` of the **first stored version** showing that status | OBSERVED — we infer it from when we first saw it |
# 
# Notes applied literally from D-191:
# - This is the simple first-observed rule; the optional `CreatedUtc`-for-initial-status
#   refinement is **not** implemented.
# - Statuses outside the mapped five (e.g. UNKNOWN-routed ones) get NULL, and the basis
#   stays NULL whenever the timestamp is NULL.
# - OBSERVED must never be presented as an exact PMS event time.


# CELL ********************

# ---------------------------------------------------------------
# 11. Per-status StatusDateTime + EXACT/OBSERVED basis (D-191).
# ---------------------------------------------------------------
from pyspark.sql.window import Window

# First-observed SnapshotDateTime per (ReservationID, PMSStatusCode)
w_status = Window.partitionBy("ReservationID", "PMSStatusCode")
df_sd = df_status.withColumn("_FirstObservedForStatus",
                             F.min("SnapshotDateTime").over(w_status))

df_sd = (df_sd
    .withColumn("StatusDateTime",
        F.when(F.col("PMSStatusCode") == "Canceled",  F.col("_CancelledUtc"))
         .when(F.col("PMSStatusCode") == "Started",   F.col("_ActualStartUtc"))
         .when(F.col("PMSStatusCode") == "Processed", F.col("_ActualEndUtc"))
         .when(F.col("PMSStatusCode").isin("Confirmed", "Optional"),
               F.col("_FirstObservedForStatus"))
         .otherwise(F.lit(None).cast("timestamp")))
    .withColumn("StatusDateTimeBasis",
        F.when(F.col("StatusDateTime").isNull(), F.lit(None).cast("string"))
         .when(F.col("PMSStatusCode").isin("Canceled", "Started", "Processed"),
               F.lit("EXACT"))
         .when(F.col("PMSStatusCode").isin("Confirmed", "Optional"),
               F.lit("OBSERVED"))
         .otherwise(F.lit(None).cast("string")))
    .drop("_FirstObservedForStatus"))

print("StatusDateTime coverage by Mews state:")
(df_sd.groupBy("PMSStatusCode", "StatusDateTimeBasis")
      .agg(F.count("*").alias("Rows"),
           F.sum(F.col("StatusDateTime").isNull().cast("int")).alias("NullStatusDateTime"))
      .orderBy("PMSStatusCode")
      .show(truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Adults and Children (D-201)
# 
# **Plain words:** a Mews reservation carries a list of `PersonCounts` — pairs of
# "age-category ID + how many people". The age categories themselves live in the separate
# `ageCategories/getAll` file, and each one is classified by Mews as `Adult` or `Child`.
# 
# The governed rule, literally:
# - Explode the `PersonCounts` list (one row per pair).
# - Join the category ID to the age-category master.
# - Sum counts where classification = `Adult` into **Adults**, `Child` into **Children**.
# - Never hardcode category IDs, never guess from names or age ranges.
# - A reservation with an unresolved ID, a duplicate ID in its list, or a classification
#   other than Adult/Child is a **data-quality exception**: its Adults/Children stay NULL
#   and it is flagged — it is explicitly forbidden to treat these as zero.
# - A reservation with no `PersonCounts` at all keeps NULL (the contract allows blank
#   when the source gives no party counts).


# CELL ********************

# ---------------------------------------------------------------
# 12. Explode PersonCounts, join age categories, aggregate (D-201).
# ---------------------------------------------------------------
df_agecats = (df_age_raw
    .select(F.col("Id").cast("string").alias("_AgeCatId"),
            F.col("Classification").cast("string").alias("_Classification")))

# Duplicate Ids in the age-category master would make the join ambiguous.
n_dup_master = (df_agecats.groupBy("_AgeCatId").count()
                          .filter(F.col("count") > 1).count())
if n_dup_master:
    raise RuntimeError(f"{n_dup_master} duplicate Id values in the age-category master — "
                       "D-201 forbids guessing; resolve the raw duplication first.")

df_pc = (df_sd
    .select("ReservationID", "SnapshotDateTime",
            F.explode_outer("_PersonCounts").alias("_pc"))
    .select("ReservationID", "SnapshotDateTime",
            F.col("_pc.AgeCategoryId").cast("string").alias("_AgeCatId"),
            F.col("_pc.Count").cast("long").alias("_Count")))

df_pc_joined = df_pc.join(df_agecats, "_AgeCatId", "left")

# Exception conditions per D-201 (only for rows that actually have PersonCounts):
exc = (df_pc_joined
    .filter(F.col("_AgeCatId").isNotNull())
    .withColumn("_bad",
        F.col("_Classification").isNull() |                       # unresolved id
        (~F.col("_Classification").isin("Adult", "Child")) |      # unsupported class
        F.col("_Count").isNull()))                                # blank count

dup_in_res = (exc.groupBy("ReservationID", "SnapshotDateTime", "_AgeCatId")
                 .count().filter(F.col("count") > 1)
                 .select("ReservationID", "SnapshotDateTime").distinct())

bad_res = (exc.filter(F.col("_bad"))
              .select("ReservationID", "SnapshotDateTime").distinct()
              .union(dup_in_res).distinct())

n_agecat_exceptions = bad_res.count()
print(f"Reservations with age-category DQ exceptions (Adults/Children left NULL): "
      f"{n_agecat_exceptions}")
if n_agecat_exceptions > 0:
    quarantine_batches.append(("AGECATEGORY_CLASSIFICATION_D201",
                               bad_res.withColumn("PMSReservationID", F.lit(None).cast("string"))))

# Aggregate clean reservations only.
clean_counts = (exc.join(bad_res, ["ReservationID", "SnapshotDateTime"], "left_anti")
    .groupBy("ReservationID", "SnapshotDateTime")
    .agg(F.sum(F.when(F.col("_Classification") == "Adult", F.col("_Count"))
                .otherwise(F.lit(0))).alias("Adults"),
         F.sum(F.when(F.col("_Classification") == "Child", F.col("_Count"))
                .otherwise(F.lit(0))).alias("Children")))

df_people = (df_sd
    .join(clean_counts, ["ReservationID", "SnapshotDateTime"], "left")
    .withColumn("Adults",   F.col("Adults").cast("long"))
    .withColumn("Children", F.col("Children").cast("long")))

print("Adults/Children profile:")
(df_people.agg(
    F.count("*").alias("Rows"),
    F.sum(F.col("Adults").isNull().cast("int")).alias("NullAdults"),
    F.sum(F.col("Children").isNull().cast("int")).alias("NullChildren"),
    F.max("Adults").alias("MaxAdults"), F.max("Children").alias("MaxChildren"))
 .show(truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Tenant lookup (D-177)
# 
# **Plain words:** every reservation row carries the governed tenant (the operating company
# that owns the property context). The governed rule resolves tenant **from the property**,
# using the manual property → tenant input.
# 
# **Open point you must confirm:** the binding references the `T_MI_Property` manual input,
# but the D-198 seed allow-list names sheets like `D_Property` and `D_Tenant`. This draft
# assumes the property → tenant mapping columns live on the sheet set in `TENANT_SEED_SHEET`
# (Section 1). If the columns are not found, the cell **stops** — it does not silently write
# NULL, because `TenantKey` is a governed not-null column.


# CELL ********************

# ---------------------------------------------------------------
# 13. Tenant from governed property context (D-177). Fails loudly.
# ---------------------------------------------------------------
tenant_pdf = seed.get(TENANT_SEED_SHEET)
if tenant_pdf is None:
    if TENANT_SEED_SHEET.startswith("_"):
        raise RuntimeError("Tenant sheet must not start with '_' (D-198).")
    if TENANT_SEED_SHEET not in xls.sheet_names:
        raise RuntimeError(f"Tenant mapping sheet '{TENANT_SEED_SHEET}' not in seed workbook.")
    tenant_pdf = pd.read_excel(xls, sheet_name=TENANT_SEED_SHEET, dtype=str).dropna(how="all")

needed = [TENANT_PROPERTY_COL, TENANT_KEY_COL, TENANT_ID_COL]
missing = [c for c in needed if c not in tenant_pdf.columns]
if missing:
    raise RuntimeError(
        f"Tenant mapping columns {missing} not found on sheet '{TENANT_SEED_SHEET}'. "
        f"Actual columns: {list(tenant_pdf.columns)}. "
        "Confirm where the governed property -> tenant mapping (T_MI_Property under D-177) "
        "lives in the current seed workbook, set the TENANT_* parameters, and re-run. "
        "Do NOT proceed with an invented mapping."
    )

df_tenant = spark.createDataFrame(
    tenant_pdf[needed].rename(columns={TENANT_PROPERTY_COL: "_TenPropertyID",
                                       TENANT_KEY_COL: "TenantKey",
                                       TENANT_ID_COL: "TenantID"})).dropDuplicates(["_TenPropertyID"])

df_tenanted = (df_people
    .join(df_tenant, df_people.PropertyID == df_tenant._TenPropertyID, "left")
    .drop("_TenPropertyID"))

n_tenant_missing = df_tenanted.filter(F.col("TenantKey").isNull()).count()
print(f"Rows with unresolved tenant: {n_tenant_missing}  (expected 0)")
if n_tenant_missing:
    raise RuntimeError("TenantKey is a governed not-null column; the property -> tenant "
                       "seed mapping does not cover every resolved property. Fix the seed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. IsLatestCurrent (D-143)
# 
# **Plain words:** because `I_Reservations` keeps history (one row per reservation *version*),
# reports need an easy way to grab only the newest version of each reservation.
# `IsLatestCurrent = TRUE` marks exactly one row per `ReservationID` — the one with the
# highest `SnapshotDateTime`. Everything older is FALSE.


# CELL ********************

# ---------------------------------------------------------------
# 14. Flag the latest stored version per reservation (D-143).
# ---------------------------------------------------------------
w_latest = (Window.partitionBy("ReservationID")
                  .orderBy(F.col("SnapshotDateTime").desc()))

df_flagged = (df_tenanted
    .withColumn("_rn", F.row_number().over(w_latest))
    .withColumn("IsLatestCurrent", F.col("_rn") == 1)
    .drop("_rn"))

print("IsLatestCurrent profile:")
(df_flagged.groupBy("IsLatestCurrent").count().show())
n_res  = df_flagged.select("ReservationID").distinct().count()
n_true = df_flagged.filter("IsLatestCurrent").count()
print(f"Distinct reservations: {n_res} | rows flagged TRUE: {n_true} (must be equal)")
if n_res != n_true:
    raise RuntimeError("IsLatestCurrent flag inconsistent with distinct reservation count.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Assemble the full governed column set (D-203 NULL rule)
# 
# **Plain words:** the `I_Reservations` contract has 43 columns. This slice fills the governed
# ones built above; **every other column is written as literal NULL** — no fallback, no
# placeholder, no UNKNOWN key, no inferred value. NULL here means, honestly:
# "not implemented yet in this slice".
# 
# Columns intentionally NULL in this slice (with the reason):
# - `CustomerID`, `AccountID` — customer/account lookup not governed (I-076)
# - `MarketCountryKey` — market-country mapping not governed (I-076)
# - `BlockID`, `IsBlockPickupReservation` — block source deferred (I-143)
# - `BookedRoomRevenue`, `BookedTotalRevenue`, `ReportedBookedRoomRevenue`,
#   `ReportedBookedTotalRevenue`, `CurrencyCode` — revenue source work open (I-179)
# - `RoomTypeKey`, `PMS_RoomTypeCode` — room-type lookup not governed (I-157/I-076)
# - `RatePlanKey`, `PMS_RatePlanCode` — rate lookup not governed (I-157/I-076)
# - `ChannelKey`, `PMS_ChannelCode` — channel mapping parked (I-075)
# - `SegmentKey`, `PMS_SegmentCode` — segment lookup not governed (I-157/I-076)
# - `RevenueStreamKey`, `NormalizedGrossBookedRoomRevenue`,
#   `BookedRevenueNormalizationStatus`, `BookedRevenueNormalizationMethod`,
#   `AppliedCommissionPct` — revenue normalization not implemented (I-102)
# 
# **Transparency note:** four of these (`IsBlockPickupReservation`, `RevenueStreamKey`,
# `BookedRevenueNormalizationStatus`, `BookedRevenueNormalizationMethod`) are marked
# not-null in the full 03_Columns contract. D-203 explicitly overrides that for this slice
# by requiring NULL. The Delta table therefore allows NULLs; the not-null contract applies
# when those columns are actually built later.


# CELL ********************

# ---------------------------------------------------------------
# 15. Final projection: governed values + governed NULLs (D-203).
# ---------------------------------------------------------------
def null_str():  return F.lit(None).cast("string")
def null_dbl():  return F.lit(None).cast("double")
def null_bool(): return F.lit(None).cast("boolean")

df_final = df_flagged.select(
    # --- governed populated columns ---
    F.col("ReservationID"),                                   # D-192
    F.col("PMSReservationID"),                                # D-192
    F.col("PropertyKey"),                                     # D-195/D-205
    F.col("PropertyID"),                                      # D-195
    # --- governed NULLs (D-203) ---
    null_str().alias("CustomerID"),                           # I-076
    null_str().alias("AccountID"),                            # I-076
    null_str().alias("MarketCountryKey"),                     # I-076
    null_str().alias("BlockID"),                              # I-143
    # --- governed populated ---
    F.col("IsGroupReservation"),                              # D-200 constant FALSE
    F.col("BookingDateTime"),                                 # D-192
    F.col("ArrivalDate"),                                     # D-193
    # --- governed NULLs ---
    null_dbl().alias("BookedRoomRevenue"),                    # I-179
    null_dbl().alias("BookedTotalRevenue"),                   # I-179
    # --- governed populated ---
    F.col("DepartureDate"),                                   # D-193
    # --- governed NULLs ---
    null_str().alias("RoomTypeKey"),                          # I-157
    null_str().alias("PMS_RoomTypeCode"),                     # I-076
    null_str().alias("RatePlanKey"),                          # I-157
    null_str().alias("PMS_RatePlanCode"),                     # I-076
    null_str().alias("ChannelKey"),                           # I-075
    null_str().alias("PMS_ChannelCode"),                      # I-075
    null_str().alias("SegmentKey"),                           # I-157
    null_str().alias("PMS_SegmentCode"),                      # I-076
    # --- governed populated ---
    F.col("ReservationStatusKey"),                            # D-190
    F.col("PMSStatusCode"),                                   # D-190
    F.col("StatusDateTime"),                                  # D-191
    F.col("Adults"),                                          # D-201
    F.col("Children"),                                        # D-201
    F.col("BookedRooms"),                                     # D-199
    # --- governed NULLs ---
    null_dbl().alias("ReportedBookedRoomRevenue"),            # I-179
    null_dbl().alias("ReportedBookedTotalRevenue"),           # I-179
    null_str().alias("CurrencyCode"),                         # I-179
    # --- governed populated ---
    F.col("SourceSystem"),                                    # D-122
    F.col("SnapshotDateTime"),                                # D-189
    # --- governed NULLs (D-203 overrides not-null for this slice) ---
    null_bool().alias("IsBlockPickupReservation"),            # I-143
    null_str().alias("RevenueStreamKey"),                     # I-102
    null_dbl().alias("NormalizedGrossBookedRoomRevenue"),     # I-102
    null_str().alias("BookedRevenueNormalizationStatus"),     # I-102
    null_str().alias("BookedRevenueNormalizationMethod"),     # I-102
    null_dbl().alias("AppliedCommissionPct"),                 # I-102
    # --- governed populated ---
    F.col("IsLatestCurrent"),                                 # D-143
    F.col("TenantKey"),                                       # D-177
    F.col("TenantID"),                                        # D-177
    F.col("StatusDateTimeBasis"),                             # D-191
)

print(f"Final I_Reservations rows: {df_final.count()}")
print(f"Final column count:        {len(df_final.columns)}  (expected 43)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 16. Write `I_Reservations` and the quarantine table
# 
# **Plain words:** the finished dataframe is saved as a **Delta table** — the lakehouse
# table format Power BI and later notebooks can read. This BUILD_DRAFT uses `overwrite`
# (full rebuild), which is safe because all raw snapshots are retained and the table can
# always be rebuilt. Incremental change-aware appends come later, in a separate governed
# section, once the D-188 trigger config is wired in.
# 
# Quarantined rows (snapshot collisions, failed property lookups, age-category exceptions)
# go into one small side table with a reason code, so nothing disappears silently.


# CELL ********************

# ---------------------------------------------------------------
# 16. Write target + quarantine as Delta.
# ---------------------------------------------------------------
(df_final.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE))
print(f"Wrote {TARGET_TABLE} ({WRITE_MODE}).")

if quarantine_batches:
    from functools import reduce
    parts = []
    for reason, dfq in quarantine_batches:
        parts.append(dfq.select(
            F.lit(reason).alias("ExceptionType"),
            F.col("ReservationID"),
            F.col("SnapshotDateTime"),
            F.col("PMSReservationID"),
            F.current_timestamp().alias("QuarantinedUtc")))
    df_quarantine = reduce(lambda a, b: a.unionByName(b), parts)
    (df_quarantine.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE))
    print(f"Wrote {QUARANTINE_TABLE}: {df_quarantine.count()} rows.")
else:
    print("No quarantined rows in this build — quarantine table not written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 17. Validation — read the written table back and prove it
# 
# **Plain words:** everything below reads the **written** table (not the in-memory dataframe),
# so what you check is what actually landed. Five checks:
# 
# 1. **Row counts** — full funnel from raw to written.
# 2. **Grain uniqueness** — no duplicate `ReservationID` + `SnapshotDateTime`.
# 3. **NULL profile** — intentionally-NULL columns must be 100% NULL; governed not-null
#    columns must be 0% NULL.
# 4. **Lookup outcomes** — property failures, UNKNOWN statuses, age-category exceptions,
#    missing StatusDateTime.
# 5. **SnapshotDateTime sanity** — no NULLs, plausible min/max, and `IsLatestCurrent` correct.
# 
# Run all of them. If any check prints a FAIL, do not build anything on top of this table.


# CELL ********************

# ---------------------------------------------------------------
# 17a. Row-count funnel.
# ---------------------------------------------------------------
t = spark.read.table(TARGET_TABLE)
n_written = t.count()

print("Row-count funnel")
print(f"  Raw reservation records parsed (all runs): {len(raw_reservations)}")
print(f"  Exact duplicates collapsed:                {n_exact_dupes}")
print(f"  Snapshot collisions quarantined (D-189):   {n_collisions}")
print(f"  Property-lookup failures quarantined:      {n_prop_failed}")
print(f"  Rows written to {TARGET_TABLE}:            {n_written}")

expected = len(raw_reservations) - n_exact_dupes - n_collisions - n_prop_failed
status = "OK" if n_written == expected else "FAIL"
print(f"  Funnel arithmetic: expected {expected} -> {status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 17b. Grain uniqueness: one row per ReservationID + SnapshotDateTime.
# ---------------------------------------------------------------
n_dup_grain = (t.groupBy("ReservationID", "SnapshotDateTime")
                .count().filter("count > 1").count())
print(f"Duplicate grain keys in written table: {n_dup_grain} "
      f"-> {'OK' if n_dup_grain == 0 else 'FAIL'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 17c. NULL profile: intentional NULLs = 100%, governed not-null = 0%.
# ---------------------------------------------------------------
INTENTIONALLY_NULL = [
    "CustomerID", "AccountID", "MarketCountryKey", "BlockID",
    "BookedRoomRevenue", "BookedTotalRevenue",
    "RoomTypeKey", "PMS_RoomTypeCode", "RatePlanKey", "PMS_RatePlanCode",
    "ChannelKey", "PMS_ChannelCode", "SegmentKey", "PMS_SegmentCode",
    "ReportedBookedRoomRevenue", "ReportedBookedTotalRevenue", "CurrencyCode",
    "IsBlockPickupReservation", "RevenueStreamKey",
    "NormalizedGrossBookedRoomRevenue", "BookedRevenueNormalizationStatus",
    "BookedRevenueNormalizationMethod", "AppliedCommissionPct",
]
MUST_BE_POPULATED = [
    "ReservationID", "PMSReservationID", "PropertyKey", "PropertyID",
    "IsGroupReservation", "BookingDateTime", "ArrivalDate", "DepartureDate",
    "ReservationStatusKey", "PMSStatusCode", "BookedRooms",
    "SourceSystem", "SnapshotDateTime", "IsLatestCurrent",
    "TenantKey", "TenantID",
]

nulls = t.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                  for c in INTENTIONALLY_NULL + MUST_BE_POPULATED]).first()

print("Intentionally NULL columns (must equal row count", n_written, "):")
ok = True
for c in INTENTIONALLY_NULL:
    good = nulls[c] == n_written
    ok = ok and good
    print(f"  {c:<38} nulls={nulls[c]:>6}  {'OK' if good else 'FAIL'}")

print("\nGoverned populated columns (must have 0 NULLs):")
for c in MUST_BE_POPULATED:
    good = nulls[c] == 0
    ok = ok and good
    print(f"  {c:<38} nulls={nulls[c]:>6}  {'OK' if good else 'FAIL'}")

print("\nOverall NULL profile:", "OK" if ok else "FAIL")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 17d. Lookup outcomes: failures stay visible, never hidden.
# ---------------------------------------------------------------
n_unknown_written = t.filter(F.col("ReservationStatusKey") == STATUS_UNKNOWN_KEY).count()
n_null_statusdt   = t.filter(F.col("StatusDateTime").isNull()).count()
n_null_adults     = t.filter(F.col("Adults").isNull()).count()
n_null_children   = t.filter(F.col("Children").isNull()).count()

print(f"Property lookup failures (quarantined):        {n_prop_failed}")
print(f"UNKNOWN reservation statuses written (D-190):  {n_unknown_written}")
print(f"Age-category DQ exceptions (D-201):            {n_agecat_exceptions}")
print(f"Rows with NULL StatusDateTime (D-191):         {n_null_statusdt}")
print(f"Rows with NULL Adults / Children:              {n_null_adults} / {n_null_children}")
print("\nStatusDateTimeBasis distribution:")
t.groupBy("ReservationStatusKey", "StatusDateTimeBasis").count() \
 .orderBy("ReservationStatusKey").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------
# 17e. SnapshotDateTime sanity + IsLatestCurrent consistency.
# ---------------------------------------------------------------
snap = t.agg(
    F.sum(F.col("SnapshotDateTime").isNull().cast("int")).alias("NullSnapshots"),
    F.min("SnapshotDateTime").alias("MinSnapshot"),
    F.max("SnapshotDateTime").alias("MaxSnapshot")).first()
print(f"NULL SnapshotDateTime: {snap['NullSnapshots']} "
      f"-> {'OK' if snap['NullSnapshots'] == 0 else 'FAIL'}")
print(f"SnapshotDateTime range: {snap['MinSnapshot']}  ..  {snap['MaxSnapshot']}")
print("(Sanity: must look like real Mews update times, not extraction-run times — D-189.)")

n_res_t  = t.select("ReservationID").distinct().count()
n_true_t = t.filter("IsLatestCurrent").count()
print(f"\nDistinct reservations: {n_res_t} | IsLatestCurrent=TRUE rows: {n_true_t} "
      f"-> {'OK' if n_res_t == n_true_t else 'FAIL'}")

print("\nSample of written rows (governed columns):")
t.select("ReservationID", "PMSReservationID", "PropertyKey", "ArrivalDate",
         "DepartureDate", "ReservationStatusKey", "StatusDateTime",
         "StatusDateTimeBasis", "Adults", "Children", "BookedRooms",
         "SnapshotDateTime", "IsLatestCurrent") \
 .orderBy("ReservationID", "SnapshotDateTime").show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 18. What this notebook did NOT do — and what comes next
# 
# Not done here, on purpose:
# - No `I_RoomNights`, no D tables, no F tables, no gold layer.
# - No rate, segment, channel, room-type, account, customer, revenue, market-country,
#   block-pickup, or positive-group logic (D-203 exclusions).
# - No incremental change-aware append — the D-188 trigger-field comparison is the next
#   build section once initial-load results are confirmed.
# - No governance workbook change. This notebook implements FINAL decisions; it decides nothing.
# 
# After a successful run, confirm the results back in chat so the working context and
# governance documentation can be updated through the normal Copilot handoff.
# 
# **Reminder: pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working,
# to avoid unnecessary cost.**


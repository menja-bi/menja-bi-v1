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

# # NB_Menja_Phase1_00_Raw_Extract_And_Verify_DEV
# 
# **Purpose:** land and verify ALL Phase-1 raw Mews inputs in one governed notebook.
# 
# Endpoints landed by this notebook:
# 
# 1. `reservations/getAll/2023-06-06` (current versioned endpoint; old version deprecated 10 Jan 2026)
# 2. `services/getAll`
# 3. `ageCategories/getAll`
# 
# **Standard raw root:** `Files/Raw/Mews/...` (capital R, capital M).
# Section 1 WARNS if a lowercase `Files/raw` (or other case variant) exists.
# This notebook never moves, renames, or deletes anything.
# 
# **Scope boundaries (raw landing only):**
# - No I-layer logic. No joins. No mappings. No fallback values. No business logic.
# - I_Reservations transformation logic does NOT belong here.
#   It belongs in `NB_Menja_Phase1_10_I_Reservations_BUILD_DRAFT`.
# 
# **Governing FINAL decisions:**
# 
# | ID | What it governs here |
# |---|---|
# | D-148 | Raw lands as JSON, source-shaped, unchanged |
# | D-149 | Extractor is raw-only, no modeled tables or business logic |
# | D-151 | Stable root folder, endpoint subfolder, timestamp in filename |
# | D-153 | Bounded date windows, chunking, page caps for heavy endpoints |
# | D-186 | ExtractionRunLog + ExtractionFileLog as Delta tables |
# 
# **Before running:**
# 1. Attach lakehouse `LH_Menja_BI_v1_Mews_DEV` to this notebook FIRST.
#    (Attaching restarts the session, so attach before running anything.)
# 2. Then use Run all, or run cells top to bottom.


# MARKDOWN ********************

# ## Section 1 - Lakehouse and path-standard check
# 
# What this cell does, in plain words:
# - Confirms the default lakehouse Files area is mounted (stops loudly if not).
# - Looks for folders whose name is a case variant of the standard
#   (`Files/raw` instead of `Files/Raw`, `Files/Raw/mews` instead of `Files/Raw/Mews`).
# - If it finds one, it prints a WARNING and continues.
# - It never moves, renames, or deletes anything. Cleanup is a manual user decision.


# CELL ********************

# Section 1 - Lakehouse and path-standard check (warn-only)

import os

FILES_ROOT = "/lakehouse/default/Files"
RAW_ROOT = FILES_ROOT + "/Raw/Mews"   # standard raw root (D-151)

if not os.path.exists(FILES_ROOT):
    raise RuntimeError(
        "Default lakehouse is not attached. "
        "Attach LH_Menja_BI_v1_Mews_DEV to this notebook, then re-run from the top."
    )

print("Default lakehouse Files area found:", FILES_ROOT)

# --- Case-variant warnings (warn-only, nothing is touched) ---
path_warnings = []

for name in os.listdir(FILES_ROOT):
    if name.lower() == "raw" and name != "Raw":
        path_warnings.append(
            "Case-variant folder found: Files/" + name
            + "  (standard is Files/Raw). NOT touched. Review manually."
        )

raw_std = FILES_ROOT + "/Raw"
if os.path.isdir(raw_std):
    for name in os.listdir(raw_std):
        if name.lower() == "mews" and name != "Mews":
            path_warnings.append(
                "Case-variant folder found: Files/Raw/" + name
                + "  (standard is Files/Raw/Mews). NOT touched. Review manually."
            )

if path_warnings:
    print("")
    print("WARNING - non-standard raw folder casing detected:")
    for w in path_warnings:
        print(" -", w)
    print("This notebook only WARNS. It never moves or deletes files.")
else:
    print("No case-variant raw folders found. Path standard is clean.")

print("")
print("Standard raw root for this run:", RAW_ROOT)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 - Key Vault secrets
# 
# Reads the two Mews API tokens from Azure Key Vault `kv-menja-biv1`.
# Secret values are never printed - only a yes/no that they loaded.


# CELL ********************

# Section 2 - Key Vault secrets (never print secret values)

vault_url = "https://kv-menja-biv1.vault.azure.net/"

mews_access_token = notebookutils.credentials.getSecret(
    vault_url,
    "mews-access-token"
)

mews_client_token = notebookutils.credentials.getSecret(
    vault_url,
    "mews-client-token"
)

print("Mews Key Vault secrets loaded.")
print("Access token loaded:", len(mews_access_token) > 0)
print("Client token loaded:", len(mews_client_token) > 0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 3 - Configuration
# 
# All parameters live here. No hidden defaults elsewhere.
# 
# Notes:
# - Endpoint subfolders are kept EXACTLY where earlier committed runs landed files
#   (`reservations`, `services/getAll`, `ageCategories/getAll`). Unifying the
#   subfolder naming later is a manual cleanup decision, not something this
#   notebook does silently.
# - The reservations window is the same bounded window the proven landing run
#   used (D-153). Widen it deliberately when you decide to - keep it bounded.
# - `RES_SERVICE_IDS` is the Mews demo service used by the committed landing
#   notebook. Widening to more services is a user decision, not a default.


# CELL ********************

# Section 3 - Configuration (single place for all parameters)

from datetime import datetime, timezone

# --- Mews API ---
BASE_URL = "https://api.mews-demo.com/api/connector/v1"
CLIENT_NAME = "Menja BI v1/1.0"
PMS_NAME = "Mews"

RESERVATIONS_ENDPOINT = "reservations/getAll/2023-06-06"

# --- Landing folders under the standard root (D-151) ---
# Kept identical to where the committed notebooks already land files.
RES_FOLDER = RAW_ROOT + "/reservations"

RAW_SIMPLE_ENDPOINTS = [
    {
        "raw_object_id": "RAW_MEWS_SERVICES",
        "endpoint": "services/getAll",
        "folder": RAW_ROOT + "/services/getAll",
        "record_keys": ["Services", "services"]
    },
    {
        "raw_object_id": "RAW_MEWS_AGE_CATEGORIES",
        "endpoint": "ageCategories/getAll",
        "folder": RAW_ROOT + "/ageCategories/getAll",
        "record_keys": ["AgeCategories", "ageCategories"]
    }
]

# --- Reservations scope (committed pattern) ---
# Mews demo service ID published in Mews docs. Proper lookup added later.
RES_SERVICE_IDS = ["bd26d8db-86da-4f96-9efc-e5a4654a4a94"]

# --- Bounded date window for reservations (D-153) ---
WINDOW_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_END_UTC   = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

# --- Chunking and page caps for reservations (D-153) ---
RES_CHUNK_DAYS = 7
RES_MAX_PAGES_PER_CHUNK = 5
RES_PAGE_SIZE = 1000

# --- Network safety ---
TIMEOUT_SEC = 60
RETRIES = 3
RETRY_SLEEP_SEC = 3

print("Config loaded.")
print("Raw root:", RAW_ROOT)
print("Reservations endpoint:", RESERVATIONS_ENDPOINT)
print("Reservations window:", WINDOW_START_UTC.date(), "to", WINDOW_END_UTC.date())
print("Simple endpoints:")
for e in RAW_SIMPLE_ENDPOINTS:
    print(" -", e["raw_object_id"], "|", e["endpoint"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 - Imports, D-186 log schemas, helper functions
# 
# Plain words:
# - The two log tables (`ExtractionRunLog`, `ExtractionFileLog`) are the D-186
#   audit trail: one row per run, one row per file written.
# - `ensure_log_tables_exist` creates them with an explicit schema if they are
#   missing. This removes the earlier first-append quirk, where the very first
#   log write could fail because the table did not exist yet with a matching schema.
# - `align_to_existing_table_schema` makes every append match whatever schema the
#   existing table already has, so older string-typed log tables still work.
# - `mews_post` is one safe HTTP call with retries and rate-limit handling.
#   It only adds a paging block (`Limitation`) when a page size is given, because
#   the simple endpoints were proven to work without one.


# CELL ********************

# Section 4 - Imports, D-186 log schemas, helpers

import json
import time
import uuid
import traceback
import requests

from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# Results of this notebook run, used by the verification section.
landing_results = {}

run_log_schema = StructType([
    StructField("RunID", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("Endpoint", StringType(), False),
    StructField("WindowStartUtc", TimestampType(), True),
    StructField("WindowEndUtc", TimestampType(), True),
    StructField("RunStartUtc", TimestampType(), False),
    StructField("RunEndUtc", TimestampType(), True),
    StructField("Status", StringType(), False),
    StructField("PagesWritten", IntegerType(), False),
    StructField("RecordCount", IntegerType(), False),
    StructField("ErrorMessage", StringType(), True)
])

file_log_schema = StructType([
    StructField("FileID", StringType(), False),
    StructField("RunID", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("Endpoint", StringType(), False),
    StructField("PageOrChunkIndex", IntegerType(), False),
    StructField("FileName", StringType(), False),
    StructField("FilePath", StringType(), False),
    StructField("RecordCount", IntegerType(), False),
    StructField("WrittenUtc", TimestampType(), False)
])


def utc_now():
    return datetime.now(timezone.utc)


def fmt_utc(dt):
    # Mews wants ISO 8601 with milliseconds and Z
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def count_records_best_effort(payload, record_keys):
    # Raw sanity count only. No transformation, no business logic.
    if isinstance(payload, dict):
        for key in record_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
    if isinstance(payload, list):
        return len(payload)
    return 0


def mews_post(endpoint, extra_body=None, cursor=None, page_size=None):
    # One POST to a Mews endpoint with retries and 429 handling.
    # Adds a Limitation block only when page_size is given.
    # Never prints secrets.
    url = BASE_URL + "/" + endpoint

    body = {
        "ClientToken": mews_client_token,
        "AccessToken": mews_access_token,
        "Client": CLIENT_NAME,
    }
    if extra_body:
        body.update(extra_body)

    if page_size is not None:
        if cursor:
            body["Limitation"] = {"Cursor": cursor, "Count": page_size}
        else:
            body["Limitation"] = {"Count": page_size}

    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(url, json=body, timeout=TIMEOUT_SEC)

            if resp.status_code == 429:
                wait = 5 * attempt
                print("Rate limited. Waiting", wait, "s (attempt", attempt, ").")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as ex:
            last_error = ex
            wait = RETRY_SLEEP_SEC * attempt
            print("Request error:", str(ex), "- retry in", wait, "s (attempt", attempt, ").")
            time.sleep(wait)

    raise RuntimeError(
        "Mews request failed after " + str(RETRIES) + " attempts: " + str(last_error)
    )


def write_json_payload(output_folder, file_name, payload):
    # Writes the raw response exactly as JSON (D-148).
    # No flattening. No mapping. No business logic.
    os.makedirs(output_folder, exist_ok=True)
    file_path = output_folder + "/" + file_name
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return file_path


def ensure_log_tables_exist():
    # Creates the D-186 log tables with an explicit schema if missing.
    # Explicit schema avoids Spark failing to infer types from NULL-only columns.
    for table_name, schema in [
        ("ExtractionRunLog", run_log_schema),
        ("ExtractionFileLog", file_log_schema),
    ]:
        if spark.catalog.tableExists(table_name):
            print("Log table exists:", table_name)
        else:
            empty_df = spark.createDataFrame([], schema=schema)
            empty_df.write.format("delta").saveAsTable(table_name)
            print("Log table created:", table_name)


def align_to_existing_table_schema(df, table_name):
    # Aligns a DataFrame to the existing Delta table schema before append.
    # Prevents type conflicts if an older table version used different types.
    target_schema = spark.table(table_name).schema
    aligned_columns = []
    for field in target_schema.fields:
        if field.name in df.columns:
            aligned_columns.append(
                F.col(field.name).cast(field.dataType).alias(field.name)
            )
        else:
            aligned_columns.append(
                F.lit(None).cast(field.dataType).alias(field.name)
            )
    return df.select(aligned_columns)


def append_run_log(row):
    df = spark.createDataFrame([row], schema=run_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionRunLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionRunLog")


def append_file_log(row):
    df = spark.createDataFrame([row], schema=file_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionFileLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionFileLog")


ensure_log_tables_exist()
print("Imports, schemas, and helpers ready.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5 - Extract reservations/getAll/2023-06-06
# 
# Plain words:
# - Splits the bounded window into 7-day chunks (D-153).
# - Pulls pages of up to 1000 reservations per chunk, capped at 5 pages per chunk.
# - Writes each page as one raw JSON file (D-148, D-151), with the run timestamp
#   in the filename.
# - Logs one run row and one file row per page (D-186).
# - If a chunk hits the page cap with more data remaining, the run is marked
#   `Partial` so nothing is silently missing.
# 
# The run ID and timestamp are generated INSIDE this cell so every run
# self-stamps (known Fabric lesson: do not depend on a separate config cell).


# CELL ********************

# Section 5 - Reservations raw landing (raw only, D-148/149/151/153/186)
# No I-layer logic. No joins. No mappings. No business logic.

endpoint = RESERVATIONS_ENDPOINT
folder = RES_FOLDER

run_id = str(uuid.uuid4())
run_stamp = run_id[:8]
run_ts = utc_now().strftime("%Y-%m-%d_%H%M%S")
run_start_utc = utc_now()

status = "Success"
error_message = None
pages_written = 0
record_count = 0
files_written = []
hit_cap_with_more = False

print("=======================================================")
print("Raw landing: ", endpoint)
print("Folder:      ", folder)
print("Window:      ", WINDOW_START_UTC.date(), "to", WINDOW_END_UTC.date())
print("RunID:       ", run_id)
print("=======================================================")


def daterange_chunks(start, end, chunk_days):
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=chunk_days), end)
        yield cur, chunk_end
        cur = chunk_end


try:
    for chunk_start, chunk_end in daterange_chunks(
        WINDOW_START_UTC, WINDOW_END_UTC, RES_CHUNK_DAYS
    ):
        print("")
        print("Chunk:", chunk_start.date(), "to", chunk_end.date())
        cursor = None
        page_index = 0

        while page_index < RES_MAX_PAGES_PER_CHUNK:
            extra_body = {
                "ServiceIds": RES_SERVICE_IDS,
                "CollidingUtc": {
                    "StartUtc": fmt_utc(chunk_start),
                    "EndUtc": fmt_utc(chunk_end),
                },
            }
            payload = mews_post(
                endpoint, extra_body, cursor=cursor, page_size=RES_PAGE_SIZE
            )
            page = payload.get("Reservations", [])
            if not page:
                print("  No more reservations in this chunk.")
                break

            page_index += 1
            pages_written += 1
            record_count += len(page)

            file_name = (
                "reservations_" + run_ts + "_" + run_stamp + "_"
                + chunk_start.strftime("%Y%m%d") + "_"
                + chunk_end.strftime("%Y%m%d")
                + "_page_" + str(page_index).zfill(3) + ".json"
            )
            file_path = write_json_payload(folder, file_name, payload)
            files_written.append(file_name)

            append_file_log({
                "FileID": str(uuid.uuid4()),
                "RunID": run_id,
                "PMS": PMS_NAME,
                "Endpoint": endpoint,
                "PageOrChunkIndex": pages_written,
                "FileName": file_name,
                "FilePath": file_path,
                "RecordCount": len(page),
                "WrittenUtc": utc_now()
            })
            print("  Page", page_index, ":", len(page), "reservations ->", file_name)

            cursor = payload.get("Cursor")
            if not cursor:
                break

        if page_index >= RES_MAX_PAGES_PER_CHUNK and cursor:
            hit_cap_with_more = True
            print("  Page cap reached (", RES_MAX_PAGES_PER_CHUNK, ") with more data remaining.")

    if hit_cap_with_more:
        status = "Partial"

except Exception as ex:
    status = "Failed"
    error_message = str(ex)
    print("")
    print("LANDING FAILED:", error_message)
    print(traceback.format_exc())

finally:
    run_end_utc = utc_now()
    append_run_log({
        "RunID": run_id,
        "PMS": PMS_NAME,
        "Endpoint": endpoint,
        "WindowStartUtc": WINDOW_START_UTC,
        "WindowEndUtc": WINDOW_END_UTC,
        "RunStartUtc": run_start_utc,
        "RunEndUtc": run_end_utc,
        "Status": status,
        "PagesWritten": pages_written,
        "RecordCount": record_count,
        "ErrorMessage": error_message
    })
    landing_results[endpoint] = {
        "run_id": run_id,
        "folder": folder,
        "files": files_written,
        "pages_written": pages_written,
        "record_count": record_count,
        "status": status,
        "record_keys": ["Reservations"]
    }
    print("")
    print("Finished:", endpoint)
    print("Status:", status, "| Files:", pages_written, "| Records:", record_count)
    if status == "Partial":
        print("NOTE: Partial run. Raise the page cap or narrow the window, then re-run.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6 - Extract services/getAll and ageCategories/getAll
# 
# Plain words:
# - Each of these small reference endpoints is one POST and one JSON file.
# - This mirrors the committed pattern that already landed 495 services and
#   333 age categories in DEV.
# - New safety check: if Mews returns a `Cursor` (meaning more pages exist),
#   the run is marked `Partial` and a warning is printed. Nothing is guessed.


# CELL ********************

# Section 6 - Simple raw inputs landing (raw only, D-148/149/151/186)
# No I-layer logic. No joins. No mappings. No business logic.

for endpoint_config in RAW_SIMPLE_ENDPOINTS:
    raw_object_id = endpoint_config["raw_object_id"]
    endpoint = endpoint_config["endpoint"]
    folder = endpoint_config["folder"]
    record_keys = endpoint_config["record_keys"]

    run_id = str(uuid.uuid4())
    run_stamp = run_id[:8]
    run_ts = utc_now().strftime("%Y-%m-%d_%H%M%S")
    run_start_utc = utc_now()

    status = "Success"
    error_message = None
    pages_written = 0
    record_count = 0
    files_written = []

    print("")
    print("=======================================================")
    print("Raw landing: ", raw_object_id)
    print("Endpoint:    ", endpoint)
    print("Folder:      ", folder)
    print("RunID:       ", run_id)
    print("=======================================================")

    try:
        payload = mews_post(endpoint)

        record_count = count_records_best_effort(payload, record_keys)

        safe_endpoint_name = endpoint.replace("/", "_")
        file_name = (
            safe_endpoint_name + "_" + run_ts + "_" + run_stamp + ".json"
        )
        file_path = write_json_payload(folder, file_name, payload)
        files_written.append(file_name)
        pages_written = 1

        append_file_log({
            "FileID": str(uuid.uuid4()),
            "RunID": run_id,
            "PMS": PMS_NAME,
            "Endpoint": endpoint,
            "PageOrChunkIndex": 1,
            "FileName": file_name,
            "FilePath": file_path,
            "RecordCount": record_count,
            "WrittenUtc": utc_now()
        })

        if isinstance(payload, dict) and payload.get("Cursor"):
            status = "Partial"
            print("WARNING: response contains a Cursor - more pages may exist.")
            print("This notebook does not page this endpoint. Review before relying on counts.")

    except Exception as ex:
        status = "Failed"
        error_message = str(ex)
        print("Landing failed.")
        print("Endpoint:", endpoint)
        print("Error:", error_message)
        print(traceback.format_exc())

    finally:
        run_end_utc = utc_now()
        append_run_log({
            "RunID": run_id,
            "PMS": PMS_NAME,
            "Endpoint": endpoint,
            "WindowStartUtc": None,
            "WindowEndUtc": None,
            "RunStartUtc": run_start_utc,
            "RunEndUtc": run_end_utc,
            "Status": status,
            "PagesWritten": pages_written,
            "RecordCount": record_count,
            "ErrorMessage": error_message
        })
        landing_results[endpoint] = {
            "run_id": run_id,
            "folder": folder,
            "files": files_written,
            "pages_written": pages_written,
            "record_count": record_count,
            "status": status,
            "record_keys": record_keys
        }
        print("Finished:", endpoint)
        print("Status:", status, "| Files:", pages_written, "| Records:", record_count)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 7 - Verify this run
# 
# Plain words, what "verified" means here:
# 1. Every file this run says it wrote actually exists on disk.
# 2. Re-opening those files and re-counting records matches the logged counts.
# 3. `ExtractionRunLog` has exactly one row for this run per endpoint.
# 4. `ExtractionFileLog` rows match the number of files written.
# 
# Each endpoint gets a PASS / CHECK / FAIL verdict.
# - PASS  = safe to move on.
# - CHECK = landed, but with a note (for example a Partial run) - read the note.
# - FAIL  = do not run the build notebook until this is fixed.


# CELL ********************

# Section 7 - Verification (files vs logs vs re-counted records)

if not landing_results:
    raise RuntimeError(
        "No landing results in memory. Run Sections 5 and 6 first, "
        "in this same session."
    )

overall_ok = True
summary_lines = []

for endpoint, res in landing_results.items():
    problems = []
    notes = []

    folder = res["folder"]
    run_id = res["run_id"]

    # 1. Files on disk
    missing_files = [
        f for f in res["files"]
        if not os.path.exists(folder + "/" + f)
    ]
    if missing_files:
        problems.append("missing files on disk: " + str(missing_files))

    # 2. Re-count records straight from the landed JSON
    recount = 0
    for f in res["files"]:
        fp = folder + "/" + f
        if os.path.exists(fp):
            with open(fp, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            recount += count_records_best_effort(payload, res["record_keys"])
    if recount != res["record_count"]:
        problems.append(
            "re-counted records (" + str(recount) + ") "
            + "do not match logged count (" + str(res["record_count"]) + ")"
        )

    # 3. Run log row for this RunID
    run_rows = spark.sql(
        "SELECT Status, PagesWritten, RecordCount, ErrorMessage "
        "FROM ExtractionRunLog WHERE RunID = '" + run_id + "'"
    ).collect()
    if len(run_rows) != 1:
        problems.append(
            "expected 1 ExtractionRunLog row for this RunID, found "
            + str(len(run_rows))
        )
    else:
        row = run_rows[0]
        if row["Status"] == "Failed":
            problems.append("run status is Failed: " + str(row["ErrorMessage"]))
        elif row["Status"] == "Partial":
            notes.append("run status is Partial - more data may remain at source")
        if row["PagesWritten"] != res["pages_written"]:
            problems.append("PagesWritten in log does not match this session")
        if row["RecordCount"] != res["record_count"]:
            problems.append("RecordCount in log does not match this session")

    # 4. File log rows for this RunID
    file_row_count = spark.sql(
        "SELECT COUNT(*) AS n FROM ExtractionFileLog WHERE RunID = '"
        + run_id + "'"
    ).collect()[0]["n"]
    if file_row_count != len(res["files"]):
        problems.append(
            "ExtractionFileLog rows (" + str(file_row_count) + ") "
            + "do not match files written (" + str(len(res["files"])) + ")"
        )

    if problems:
        verdict = "FAIL"
        overall_ok = False
    elif notes:
        verdict = "CHECK"
    else:
        verdict = "PASS"

    summary_lines.append(
        verdict + " | " + endpoint
        + " | files=" + str(res["pages_written"])
        + " | records=" + str(res["record_count"])
    )
    for p in problems:
        summary_lines.append("       problem: " + p)
    for n in notes:
        summary_lines.append("       note: " + n)

print("=======================================================")
print("VERIFICATION SUMMARY - this notebook run")
print("=======================================================")
for line in summary_lines:
    print(line)

print("")
if overall_ok:
    print("All Phase-1 raw inputs landed and verified for this run.")
    print("Next: run NB_Menja_Phase1_10_I_Reservations_BUILD_DRAFT (Section 2")
    print("checks this same ExtractionRunLog before building).")
else:
    print("STOP: at least one endpoint FAILED verification.")
    print("Fix and re-run this notebook before running the build notebook.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 8 - Log views (read-only)
# 
# Shows the D-186 log rows for this run, plus every distinct `Endpoint` value in
# `ExtractionRunLog`.
# 
# Why the distinct-endpoint list matters: the reservations endpoint is logged as
# the full versioned string `reservations/getAll/2023-06-06`. If the build
# notebook's readiness check filters on the shorter string `reservations/getAll`,
# it will not find these rows. This view makes any mismatch visible immediately.


# CELL ********************

# Section 8 - Read-only log views for this run

this_run_ids = [res["run_id"] for res in landing_results.values()]
run_ids_sql = ",".join(["'" + r + "'" for r in this_run_ids])

print("ExtractionRunLog rows for this run:")
display(
    spark.sql(
        "SELECT RunID, PMS, Endpoint, Status, PagesWritten, RecordCount, "
        "RunStartUtc, RunEndUtc, ErrorMessage "
        "FROM ExtractionRunLog "
        "WHERE RunID IN (" + run_ids_sql + ") "
        "ORDER BY RunStartUtc ASC"
    )
)

print("ExtractionFileLog rows for this run:")
display(
    spark.sql(
        "SELECT RunID, Endpoint, PageOrChunkIndex, FileName, RecordCount, WrittenUtc "
        "FROM ExtractionFileLog "
        "WHERE RunID IN (" + run_ids_sql + ") "
        "ORDER BY WrittenUtc ASC"
    )
)

print("Distinct Endpoint values in ExtractionRunLog (all history):")
display(
    spark.sql(
        "SELECT Endpoint, COUNT(*) AS Runs, MAX(RunStartUtc) AS LastRunStartUtc "
        "FROM ExtractionRunLog GROUP BY Endpoint ORDER BY Endpoint"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 9 - Wrap-up
# 
# If Section 7 shows PASS for all three endpoints:
# 
# 1. Confirm the results yourself (files under `Files/Raw/Mews/...`, log rows above).
# 2. Commit this notebook to GitHub from workspace Source control,
#    and confirm GitHub actually updated.
# 3. Continue with `NB_Menja_Phase1_10_I_Reservations_BUILD_DRAFT`.
# 
# This notebook never deletes or moves files. If Section 1 warned about a
# lowercase `Files/raw` folder, decide manually what to do with it.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working,
# to avoid unnecessary cost.**


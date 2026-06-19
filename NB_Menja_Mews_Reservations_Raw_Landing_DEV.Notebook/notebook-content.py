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

# =======================================================
# CONFIG CELL — Menja Mews Reservations Raw Landing
# Secrets are already loaded in the previous cell.
# No tokens here.
# =======================================================

from datetime import datetime, timezone

# --- Mews API ---
BASE_URL = "https://api.mews-demo.com/api/connector/v1"
CLIENT_NAME = "Menja BI v1/1.0"
RESERVATIONS_ENDPOINT = "reservations/getAll/2023-06-06"

# --- Raw landing location (D-151: stable root, endpoint subfolder) ---
RAW_ROOT = "/lakehouse/default/Files/Raw/Mews"
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")

# --- Date window for this run (D-153: bounded window) ---
# Small window for the first test. Widen later.
WINDOW_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_END_UTC   = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

# --- Chunking and page caps (D-153) ---
RES_CHUNK_DAYS = 7
RES_MAX_PAGES_PER_CHUNK = 5
RES_PAGE_SIZE = 1000

# --- Network safety ---
TIMEOUT_SEC = 60
RETRIES = 3

print("Config loaded.")
print("Endpoint:", RESERVATIONS_ENDPOINT)
print("Window:", WINDOW_START_UTC.date(), "to", WINDOW_END_UTC.date())
print("Raw root:", RAW_ROOT)
print("Run timestamp:", RUN_TS)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# HTTP HELPER — one safe Mews POST with retry + rate-limit
# Reads tokens from memory. No tokens written in this cell.
# =======================================================

import time
import json
import requests

# Demo service ID (published in Mews docs). Proper lookup added later.
SERVICE_ID = "bd26d8db-86da-4f96-9efc-e5a4654a4a94"

def fmt_utc(dt):
    # Mews wants ISO 8601 with milliseconds and Z
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def mews_post(endpoint, extra_body, cursor=None):
    """
    Sends one POST to a Mews endpoint and returns parsed JSON.
    Handles 429 rate limits and transient errors with retries.
    Does NOT page or write files.
    """
    url = f"{BASE_URL}/{endpoint}"

    body = {
        "ClientToken": mews_client_token,
        "AccessToken": mews_access_token,
        "Client": CLIENT_NAME,
    }
    body.update(extra_body)

    if cursor:
        body["Limitation"] = {"Cursor": cursor, "Count": RES_PAGE_SIZE}
    else:
        body["Limitation"] = {"Count": RES_PAGE_SIZE}

    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(url, json=body, timeout=TIMEOUT_SEC)

            if resp.status_code == 429:
                wait = 5 * attempt
                print(f"Rate limited. Waiting {wait}s (attempt {attempt}).")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_error = e
            wait = 3 * attempt
            print(f"Request error: {e}. Retry in {wait}s (attempt {attempt}).")
            time.sleep(wait)

    raise RuntimeError(f"Mews request failed after {RETRIES} attempts: {last_error}")

print("HTTP helper ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# TEST CALL — one page of reservations, no paging, no files
# Proves the request works and data comes back.
# =======================================================

extra_body = {
    "ServiceIds": [SERVICE_ID],
    "CollidingUtc": {
        "StartUtc": fmt_utc(WINDOW_START_UTC),
        "EndUtc": fmt_utc(WINDOW_END_UTC),
    },
}

result = mews_post(RESERVATIONS_ENDPOINT, extra_body)

# Look at what came back, without dumping everything
reservations = result.get("Reservations", [])
cursor = result.get("Cursor")

print("Reservations returned this page:", len(reservations))
print("Cursor present (more pages?):", bool(cursor))

if reservations:
    first = reservations[0]
    print("\nFirst reservation — sample fields:")
    print("  Id:", first.get("Id"))
    print("  State:", first.get("State"))
    print("  StartUtc:", first.get("StartUtc"))
    print("  ScheduledEndUtc:", first.get("ScheduledEndUtc"))
    print("  Number:", first.get("Number"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# EXTRACTION + D-186 LOGGING — reservations raw landing
# Writes raw JSON (D-148, D-151) AND logs to two Delta
# tables (D-186): ExtractionRunLog, ExtractionFileLog.
# =======================================================

import os
import uuid
from datetime import timedelta, datetime, timezone

RES_DIR = f"{RAW_ROOT}/reservations"
os.makedirs(RES_DIR, exist_ok=True)

PMS = "MEWS"
# Generate a fresh, unique RunID every time this cell runs (D-186).
RUN_ID = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S_%f") + "_" + uuid.uuid4().hex[:6]
RUN_TS = RUN_ID  # filenames use the same unique stamp

# Collect log rows in memory, write to Delta at the end
file_log_rows = []

def daterange_chunks(start, end, chunk_days):
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=chunk_days), end)
        yield cur, chunk_end
        cur = chunk_end

def write_page(chunk_start, chunk_end, page_index, page_json):
    start_str = chunk_start.strftime("%Y%m%d")
    end_str = chunk_end.strftime("%Y%m%d")
    filename = (
        f"reservations_{RUN_TS}_{start_str}_{end_str}"
        f"_page_{page_index:03d}.json"
    )
    filepath = f"{RES_DIR}/{filename}"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(page_json, f, ensure_ascii=False)
    return filename, filepath

# --- Run-level tracking ---
run_start = datetime.now(timezone.utc)
status = "Success"
error_message = None
total_reservations = 0
total_files = 0
hit_cap_with_more = False

try:
    for chunk_start, chunk_end in daterange_chunks(
        WINDOW_START_UTC, WINDOW_END_UTC, RES_CHUNK_DAYS
    ):
        print(f"\nChunk: {chunk_start.date()} to {chunk_end.date()}")
        cursor = None
        page_index = 0

        while page_index < RES_MAX_PAGES_PER_CHUNK:
            extra_body = {
                "ServiceIds": [SERVICE_ID],
                "CollidingUtc": {
                    "StartUtc": fmt_utc(chunk_start),
                    "EndUtc": fmt_utc(chunk_end),
                },
            }
            result = mews_post(RESERVATIONS_ENDPOINT, extra_body, cursor=cursor)
            page = result.get("Reservations", [])
            if not page:
                print("  No more reservations in this chunk.")
                break

            page_index += 1
            fname, fpath = write_page(chunk_start, chunk_end, page_index, result)
            total_reservations += len(page)
            total_files += 1

            # D-186 file log row — one per file
            file_log_rows.append({
                "FileID": str(uuid.uuid4()),
                "RunID": RUN_ID,
                "PMS": PMS,
                "Endpoint": RESERVATIONS_ENDPOINT,
                "PageOrChunkIndex": page_index,
                "FileName": fname,
                "FilePath": fpath,
                "RecordCount": len(page),
                "WrittenUtc": datetime.now(timezone.utc).isoformat(),
            })
            print(f"  Page {page_index}: {len(page)} reservations -> {fname}")

            cursor = result.get("Cursor")
            if not cursor:
                break

        if page_index >= RES_MAX_PAGES_PER_CHUNK and cursor:
            hit_cap_with_more = True
            print(f"  Hit page cap ({RES_MAX_PAGES_PER_CHUNK}). More data may remain.")

    if hit_cap_with_more:
        status = "Partial"

except Exception as e:
    status = "Failed"
    error_message = str(e)
    print(f"\nRUN FAILED: {e}")

run_end = datetime.now(timezone.utc)

# --- Build the two log tables as Delta (D-186) ---
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

run_log_schema = StructType([
    StructField("RunID", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("Endpoint", StringType(), False),
    StructField("WindowStartUtc", StringType(), True),
    StructField("WindowEndUtc", StringType(), True),
    StructField("RunStartUtc", StringType(), True),
    StructField("RunEndUtc", StringType(), True),
    StructField("Status", StringType(), False),
    StructField("PagesWritten", IntegerType(), True),
    StructField("RecordCount", IntegerType(), True),
    StructField("ErrorMessage", StringType(), True),
])

file_log_schema = StructType([
    StructField("FileID", StringType(), False),
    StructField("RunID", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("Endpoint", StringType(), False),
    StructField("PageOrChunkIndex", IntegerType(), True),
    StructField("FileName", StringType(), True),
    StructField("FilePath", StringType(), True),
    StructField("RecordCount", IntegerType(), True),
    StructField("WrittenUtc", StringType(), True),
])

run_log_df = spark.createDataFrame(run_log_rows, schema=run_log_schema)
run_log_df.write.format("delta").mode("append").saveAsTable("ExtractionRunLog")

if file_log_rows:
    file_log_df = spark.createDataFrame(file_log_rows, schema=file_log_schema)
    file_log_df.write.format("delta").mode("append").saveAsTable("ExtractionFileLog")

print(f"\nDONE. Status: {status}")
print(f"Reservations: {total_reservations}, files: {total_files}")
print(f"Logged to ExtractionRunLog and ExtractionFileLog (RunID: {RUN_ID})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick check — list what landed in the reservations folder
import os
files = sorted(os.listdir(RES_DIR))
print(f"Files in {RES_DIR}:")
for f in files:
    size = os.path.getsize(f"{RES_DIR}/{f}")
    print(f"  {f}  ({size:,} bytes)")
print(f"\nTotal files: {len(files)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the two D-186 log tables
print("=== ExtractionRunLog ===")
spark.sql("SELECT * FROM ExtractionRunLog").show(truncate=False)

print("=== ExtractionFileLog ===")
spark.sql("SELECT FileID, RunID, PageOrChunkIndex, FileName, RecordCount FROM ExtractionFileLog").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS ExtractionRunLog")
spark.sql("DROP TABLE IF EXISTS ExtractionFileLog")
print("Log tables cleared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os, glob
old = glob.glob(f"{RAW_ROOT}/reservations/*.json")
for f in old:
    os.remove(f)
print(f"Removed {len(old)} old files. Clean slate.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Force-clear both log tables by deleting all rows
spark.sql("DELETE FROM ExtractionRunLog")
spark.sql("DELETE FROM ExtractionFileLog")

# Confirm they're empty
print("RunLog rows:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionRunLog").collect()[0]["n"])
print("FileLog rows:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionFileLog").collect()[0]["n"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnosis — what's actually in each table and where
print("RUN LOG count:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionRunLog").collect()[0]["n"])
print("FILE LOG count:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionFileLog").collect()[0]["n"])
print()
print("Distinct RunIDs in RUN log:")
spark.sql("SELECT DISTINCT RunID FROM ExtractionRunLog").show(truncate=False)
print("Distinct RunIDs in FILE log:")
spark.sql("SELECT DISTINCT RunID FROM ExtractionFileLog").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DELETE FROM ExtractionRunLog")
spark.sql("DELETE FROM ExtractionFileLog")
print("RunLog:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionRunLog").collect()[0]["n"])
print("FileLog:", spark.sql("SELECT COUNT(*) AS n FROM ExtractionFileLog").collect()[0]["n"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os, glob
old = glob.glob(f"{RAW_ROOT}/reservations/*.json")
for f in old:
    os.remove(f)
print(f"Removed {len(old)} files.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Distinct RunIDs in RUN log:")
spark.sql("SELECT DISTINCT RunID FROM ExtractionRunLog").show(truncate=False)
print("Distinct RunIDs in FILE log:")
spark.sql("SELECT DISTINCT RunID FROM ExtractionFileLog").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

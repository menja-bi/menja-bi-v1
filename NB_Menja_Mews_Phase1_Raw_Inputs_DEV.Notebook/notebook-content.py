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

# =======================================================
# SECRET CELL — Menja Mews Raw Landing
# Reads secrets from Key Vault.
# Do not print secret values.
# =======================================================

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

# CELL ********************

# =======================================================
# CONFIG CELL — Menja Mews Phase-1 Raw Inputs Landing
#
# Stage 1 scope:
# - services/getAll
# - ageCategories/getAll
#
# Raw landing only.
# No I-layer logic.
# No joins.
# No mappings.
# No fallback values.
# No business logic.
# =======================================================

from datetime import datetime, timezone

# --- Mews API ---
BASE_URL = "https://api.mews-demo.com/api/connector/v1"
CLIENT_NAME = "Menja BI v1/1.0"

# --- Raw landing location ---
# D-151 pattern: stable raw root with endpoint-specific subfolders.
RAW_ROOT = "/lakehouse/default/Files/Raw/Mews"

# One timestamp for this notebook run.
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")

# --- Endpoints required before Phase-1 I_Reservations build ---
# services/getAll supports D-195 property resolution.
# ageCategories/getAll supports D-201 Adults/Children classification.
RAW_INPUT_ENDPOINTS = [
    {
        "raw_object_id": "RAW_MEWS_SERVICES",
        "endpoint": "services/getAll",
        "folder": "services/getAll",
        "record_keys": ["Services", "services"]
    },
    {
        "raw_object_id": "RAW_MEWS_AGE_CATEGORIES",
        "endpoint": "ageCategories/getAll",
        "folder": "ageCategories/getAll",
        "record_keys": ["AgeCategories", "ageCategories"]
    }
]

# --- Network safety ---
TIMEOUT_SEC = 60
RETRIES = 3
RETRY_SLEEP_SEC = 2

print("Config loaded.")
print("Raw root:", RAW_ROOT)
print("Run timestamp:", RUN_TS)
print("Endpoints to land:")
for e in RAW_INPUT_ENDPOINTS:
    print("-", e["raw_object_id"], "|", e["endpoint"])
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# IMPORTS + D-186 LOG SCHEMAS
# =======================================================

import os
import json
import uuid
import time
import requests
import traceback

from datetime import datetime, timezone

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

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

print("Imports and log schemas loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# HELPER FUNCTIONS
# Raw landing only.
# =======================================================

def utc_now():
    return datetime.now(timezone.utc)


def count_records_best_effort(payload, record_keys):
    """
    Raw sanity count only.
    This does not transform data or apply business logic.
    It only counts the likely top-level array returned by Mews.
    """
    if isinstance(payload, dict):
        for key in record_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)

    if isinstance(payload, list):
        return len(payload)

    return 0


def post_mews_with_retries(endpoint, body):
    """
    Simple POST helper with retry.
    Does not print secrets.
    """
    url = f"{BASE_URL}/{endpoint}"
    last_error = None

    for attempt in range(1, RETRIES + 1):
        try:
            response = requests.post(
                url,
                json=body,
                timeout=TIMEOUT_SEC
            )

            response.raise_for_status()
            return response.json()

        except Exception as ex:
            last_error = ex
            print(f"Attempt {attempt} failed for {endpoint}: {str(ex)}")

            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_SEC)

    raise last_error


def write_json_payload(output_folder, file_name, payload):
    """
    Writes the raw response exactly as JSON.
    No flattening.
    No mapping.
    No business logic.
    """
    os.makedirs(output_folder, exist_ok=True)

    file_path = f"{output_folder}/{file_name}"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return file_path


from pyspark.sql import functions as F

def align_to_existing_table_schema(df, table_name):
    """
    Aligns a new DataFrame to the existing Delta table schema before append.
    This prevents Delta merge/type conflicts such as WindowStartUtc mismatch.
    """
    target_schema = spark.table(table_name).schema

    aligned_columns = []

    for field in target_schema.fields:
        col_name = field.name

        if col_name in df.columns:
            aligned_columns.append(
                F.col(col_name).cast(field.dataType).alias(col_name)
            )
        else:
            aligned_columns.append(
                F.lit(None).cast(field.dataType).alias(col_name)
            )

    return df.select(aligned_columns)


from pyspark.sql import functions as F

def align_to_existing_table_schema(df, table_name):
    """
    Aligns a new DataFrame to the existing Delta table schema before append.
    This prevents Delta merge/type conflicts such as WindowStartUtc mismatch.
    """
    target_schema = spark.table(table_name).schema

    aligned_columns = []

    for field in target_schema.fields:
        col_name = field.name

        if col_name in df.columns:
            aligned_columns.append(
                F.col(col_name).cast(field.dataType).alias(col_name)
            )
        else:
            aligned_columns.append(
                F.lit(None).cast(field.dataType).alias(col_name)
            )

    return df.select(aligned_columns)


def append_file_log(row):
    df = spark.createDataFrame([row], schema=file_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionFileLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionFileLog")


def append_run_log(row):
    df = spark.createDataFrame([row], schema=run_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionRunLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionRunLog")


print("Helper functions loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# RAW LANDING CELL — Stage 1
#
# Lands:
# - services/getAll
# - ageCategories/getAll
#
# Raw landing only.
# No I-layer logic.
# No joins.
# No mappings.
# No fallback values.
# No business logic.
# =======================================================

for endpoint_config in RAW_INPUT_ENDPOINTS:
    raw_object_id = endpoint_config["raw_object_id"]
    endpoint = endpoint_config["endpoint"]
    folder = endpoint_config["folder"]
    record_keys = endpoint_config["record_keys"]

    run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    run_start_utc = utc_now()

    status = "Success"
    error_message = None
    pages_written = 0
    record_count = 0
    file_name = None
    file_path = None

    print("")
    print("=======================================================")
    print("Starting raw landing")
    print("Raw object:", raw_object_id)
    print("Endpoint:", endpoint)
    print("=======================================================")

    try:
        body = {
            "ClientToken": mews_client_token,
            "AccessToken": mews_access_token,
            "Client": CLIENT_NAME
        }

        payload = post_mews_with_retries(endpoint, body)

        record_count = count_records_best_effort(
            payload=payload,
            record_keys=record_keys
        )

        output_folder = f"{RAW_ROOT}/{folder}"

        safe_endpoint_name = endpoint.replace("/", "_")
        file_name = f"{safe_endpoint_name}_{RUN_TS}_{run_id}.json"
        file_path = write_json_payload(
            output_folder=output_folder,
            file_name=file_name,
            payload=payload
        )

        pages_written = 1
        written_utc = utc_now()

        append_file_log({
            "FileID": file_id,
            "RunID": run_id,
            "PMS": "Mews",
            "Endpoint": endpoint,
            "PageOrChunkIndex": 1,
            "FileName": file_name,
            "FilePath": file_path,
            "RecordCount": record_count,
            "WrittenUtc": written_utc
        })

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
            "PMS": "Mews",
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

        print("Finished:", endpoint)
        print("Status:", status)
        print("Files written:", pages_written)
        print("Record count:", record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# CHECK CELL — Raw files landed
# =======================================================

for endpoint_config in RAW_INPUT_ENDPOINTS:
    raw_object_id = endpoint_config["raw_object_id"]
    folder = endpoint_config["folder"]
    folder_path = f"{RAW_ROOT}/{folder}"

    print("")
    print("=======================================================")
    print("Raw object:", raw_object_id)
    print("Folder:", folder_path)
    print("=======================================================")

    if os.path.exists(folder_path):
        files = [
            f for f in os.listdir(folder_path)
            if f.lower().endswith(".json")
        ]

        files = sorted(files)

        print("JSON files found:", len(files))

        for file_name in files[-5:]:
            print("-", file_name)
    else:
        print("Folder not found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# CHECK CELL — ExtractionRunLog
# =======================================================

display(
    spark.sql("""
        SELECT
            PMS,
            Endpoint,
            Status,
            PagesWritten,
            RecordCount,
            RunStartUtc,
            RunEndUtc,
            ErrorMessage
        FROM ExtractionRunLog
        WHERE Endpoint IN ('services/getAll', 'ageCategories/getAll')
        ORDER BY RunStartUtc DESC
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# CHECK CELL — ExtractionFileLog
# =======================================================

display(
    spark.sql("""
        SELECT
            PMS,
            Endpoint,
            FileName,
            FilePath,
            RecordCount,
            WrittenUtc
        FROM ExtractionFileLog
        WHERE Endpoint IN ('services/getAll', 'ageCategories/getAll')
        ORDER BY WrittenUtc DESC
        LIMIT 20
    """)
)

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

# CELL ********************

display(
    spark.sql("""
        SELECT
            Endpoint,
            Status,
            PagesWritten,
            RecordCount,
            RunStartUtc,
            RunEndUtc,
            ErrorMessage
        FROM ExtractionRunLog
        WHERE Endpoint IN ('services/getAll', 'ageCategories/getAll')
        ORDER BY RunStartUtc DESC
        LIMIT 10
    """)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# BROADER LOG CHECK — no RUN_TS filter
# =======================================================

print("Recent ExtractionRunLog rows for Stage-1 endpoints")

display(
    spark.sql("""
        SELECT
            RunID,
            PMS,
            Endpoint,
            Status,
            PagesWritten,
            RecordCount,
            RunStartUtc,
            RunEndUtc,
            ErrorMessage
        FROM ExtractionRunLog
        WHERE Endpoint IN ('services/getAll', 'ageCategories/getAll')
        ORDER BY RunStartUtc DESC
        LIMIT 20
    """)
)

print("Recent ExtractionFileLog rows for Stage-1 endpoints")

display(
    spark.sql("""
        SELECT
            FileID,
            RunID,
            PMS,
            Endpoint,
            FileName,
            FilePath,
            RecordCount,
            WrittenUtc
        FROM ExtractionFileLog
        WHERE Endpoint IN ('services/getAll', 'ageCategories/getAll')
        ORDER BY WrittenUtc DESC
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================
# SELF-CONTAINED DEV REPAIR + VERIFY CELL
#
# Purpose:
# Write missing D-186 log rows for already-landed Stage-1 raw files.
#
# Use when:
# - raw files exist
# - payload shape is verified
# - ExtractionRunLog / ExtractionFileLog are missing rows
#
# Scope:
# - services/getAll
# - ageCategories/getAll
#
# No business logic.
# No I-layer logic.
# No joins.
# No mappings.
# =======================================================

import os
import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

# -------------------------------------------------------
# Local schemas
# -------------------------------------------------------

repair_run_log_schema = StructType([
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

repair_file_log_schema = StructType([
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

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc)


def align_to_existing_table_schema(df, table_name):
    """
    Align a DataFrame to an existing Delta table before append.
    This avoids type conflicts with existing log-table schema.
    """
    target_schema = spark.table(table_name).schema
    aligned_columns = []

    for field in target_schema.fields:
        col_name = field.name

        if col_name in df.columns:
            aligned_columns.append(
                F.col(col_name).cast(field.dataType).alias(col_name)
            )
        else:
            aligned_columns.append(
                F.lit(None).cast(field.dataType).alias(col_name)
            )

    return df.select(aligned_columns)


def append_repair_file_log(row):
    df = spark.createDataFrame([row], schema=repair_file_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionFileLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionFileLog")


def append_repair_run_log(row):
    df = spark.createDataFrame([row], schema=repair_run_log_schema)
    df = align_to_existing_table_schema(df, "ExtractionRunLog")
    df.write.format("delta").mode("append").saveAsTable("ExtractionRunLog")


# -------------------------------------------------------
# Stage-1 files to repair
# -------------------------------------------------------

stage1_files = [
    {
        "endpoint": "services/getAll",
        "folder": f"{RAW_ROOT}/services/getAll",
        "expected_list_key": "Services"
    },
    {
        "endpoint": "ageCategories/getAll",
        "folder": f"{RAW_ROOT}/ageCategories/getAll",
        "expected_list_key": "AgeCategories"
    }
]

print("=======================================================")
print("MENJA STAGE-1 D-186 LOG REPAIR")
print("=======================================================")
print("RUN_TS:", RUN_TS)
print("RAW_ROOT:", RAW_ROOT)

repaired_run_ids = []

# -------------------------------------------------------
# Repair loop
# -------------------------------------------------------

for item in stage1_files:
    endpoint = item["endpoint"]
    folder = item["folder"]
    expected_list_key = item["expected_list_key"]

    print("")
    print("-------------------------------------------------------")
    print("Endpoint:", endpoint)
    print("Folder:", folder)

    if not os.path.exists(folder):
        raise Exception(f"Folder does not exist: {folder}")

    files = sorted([
        f for f in os.listdir(folder)
        if f.endswith(".json") and RUN_TS in f
    ])

    if not files:
        raise Exception(f"No JSON file found for {endpoint} and RUN_TS={RUN_TS}")

    file_name = files[-1]
    file_path = f"{folder}/{file_name}"

    print("Using file:", file_name)

    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise Exception(f"Unexpected payload shape for {endpoint}: payload is not a dict")

    if expected_list_key not in payload:
        raise Exception(f"Expected key '{expected_list_key}' not found in payload for {endpoint}")

    if not isinstance(payload[expected_list_key], list):
        raise Exception(f"Expected key '{expected_list_key}' is not a list for {endpoint}")

    record_count = len(payload[expected_list_key])

    run_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    now_utc = utc_now()

    append_repair_file_log({
        "FileID": file_id,
        "RunID": run_id,
        "PMS": "Mews",
        "Endpoint": endpoint,
        "PageOrChunkIndex": 1,
        "FileName": file_name,
        "FilePath": file_path,
        "RecordCount": record_count,
        "WrittenUtc": now_utc
    })

    append_repair_run_log({
        "RunID": run_id,
        "PMS": "Mews",
        "Endpoint": endpoint,
        "WindowStartUtc": None,
        "WindowEndUtc": None,
        "RunStartUtc": now_utc,
        "RunEndUtc": now_utc,
        "Status": "Success",
        "PagesWritten": 1,
        "RecordCount": record_count,
        "ErrorMessage": None
    })

    repaired_run_ids.append(run_id)

    print(
        "Repair log rows written |",
        "Endpoint:", endpoint,
        "| RecordCount:", record_count,
        "| RunID:", run_id
    )

# -------------------------------------------------------
# Verification
# -------------------------------------------------------

print("")
print("=======================================================")
print("VERIFY REPAIRED RUN LOG ROWS")
print("=======================================================")

run_ids_sql = ",".join([f"'{x}'" for x in repaired_run_ids])

display(
    spark.sql(f"""
        SELECT
            RunID,
            PMS,
            Endpoint,
            Status,
            PagesWritten,
            RecordCount,
            RunStartUtc,
            RunEndUtc,
            ErrorMessage
        FROM ExtractionRunLog
        WHERE RunID IN ({run_ids_sql})
        ORDER BY RunStartUtc ASC
    """)
)

print("")
print("=======================================================")
print("VERIFY REPAIRED FILE LOG ROWS")
print("=======================================================")

display(
    spark.sql(f"""
        SELECT
            FileID,
            RunID,
            PMS,
            Endpoint,
            FileName,
            FilePath,
            RecordCount,
            WrittenUtc
        FROM ExtractionFileLog
        WHERE RunID IN ({run_ids_sql})
        ORDER BY WrittenUtc ASC
    """)
)

print("")
print("=======================================================")
print("FINAL REPAIR RESULT")
print("=======================================================")

for run_id in repaired_run_ids:
    rows = spark.sql(f"""
        SELECT
            Endpoint,
            Status,
            PagesWritten,
            RecordCount,
            ErrorMessage
        FROM ExtractionRunLog
        WHERE RunID = '{run_id}'
    """).collect()

    if not rows:
        print(run_id, "| FAIL | No run-log row found")
    else:
        row = rows[0]
        if row["Status"] == "Success" and row["PagesWritten"] == 1 and row["ErrorMessage"] is None:
            print(
                row["Endpoint"],
                "| PASS |",
                "RecordCount=" + str(row["RecordCount"])
            )
        else:
            print(
                row["Endpoint"],
                "| CHECK |",
                "Status=" + str(row["Status"]),
                "| Error=" + str(row["ErrorMessage"])
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

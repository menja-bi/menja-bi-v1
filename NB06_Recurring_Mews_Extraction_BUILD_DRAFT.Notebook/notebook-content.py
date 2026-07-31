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

# # NB06 — Recurring LIVE Mews Extraction — BUILD_DRAFT
# 
# **Plain words:** this notebook is the recurring, watermark-based puller for LIVE Mews
# properties. Unlike `NB00` (a one-time demo historical pull), NB06 pulls only what changed
# since the last successful run for each enabled LIVE property, using Mews `UpdatedUtc` as
# the filter — not the demo `CollidingUtc` stay-window filter NB00 uses.
# 
# **Status:** DRAFT for user review. Nothing here is run, committed, or done until the user
# runs it in Fabric and confirms the results.
# 
# **Governance basis (all FINAL):**
# 
# | Rule | Decision |
# |---|---|
# | Observation cadence / business-day cut-off (context only, not implemented here) | D-210 |
# | Incremental pull, per-property watermark, overlap margin, de-duplication, missed-run handling | D-213 |
# | Cold-start `UpdatedUtc` lower boundary comes only from the seed's `ColdStartUpdatedUtcFrom` | D-214 |
# | PropertyKey-to-request-scope binding (`MewsScopeType` / `MewsScopeIds`), no inference | D-215 |
# | A fully successful zero-row interval still advances the watermark | D-216 |
# | Raw retry / idempotency: every attempt lands new immutable raw evidence | D-217 |
# | Fixed `WindowEndUtc` captured once, after config validation, before the first request | D-218 |
# | `PropertyExtractionConfig` structure; only `IsLiveExtractionEnabled = TRUE` rows are eligible | D-219 |
# | Run/file logging grain and the `PropertyKey`/`SourceType`/`SourcePropertyCode`/`MewsScopeType`/`MewsScopeIds` provenance columns | D-186 (revised) |
# 
# **Explicitly NOT in scope here (per governance, out of scope, or genuinely open):**
# Fabric pipeline/schedule/trigger setup (a separate Fabric object the user configures);
# Mews hard-delete handling (D-213: "an open assumption to validate against real data, not
# handled by invented logic in v1"); backfill logic; `F_OTBPosition` grain/build; any
# revenue, channel, segment, rate-plan, or market-country logic; forecasting, optimization,
# recommendations, automation, RMS logic.
# 
# **Two things this DRAFT cannot supply, and does not guess:**
# 1. **`BASE_URL` for the production Mews API.** NB00's `https://api.mews-demo.com/...` is
#    the demo host. No FINAL decision or repository file states the production host. This
#    is left as an explicit placeholder below — fill it in and confirm it before any live run.
# 2. **Today's actual data has zero eligible properties.** The current
#    `PropertyExtractionConfig` (per the last confirmed DEV read-back) has one row for OSL
#    with `IsLiveExtractionEnabled = FALSE` and no LIVE `SourcePropertyCode` resolved yet.
#    That means a correct run of this notebook right now will find **zero** enabled
#    properties and stop cleanly with nothing to do — which is the *correct*, governed
#    behavior (D-219: never initiate a Mews request for a disabled configuration), not a bug.
# 
# **Reused, not reinvented:** the Key Vault pattern, `mews_post` retry/rate-limit handling,
# `write_json_payload`, `daterange_chunks`, and the D-186 log-table helpers are carried over
# from `NB00` and `NB_Menja_ExtractionControl_Setup` essentially unchanged, so this notebook
# does not introduce a second way of doing the same thing.


# CELL ********************

# ---------------------------------------------------------------
# Section 1 - Lakehouse and path-standard check (warn-only)
# Identical pattern to NB00; not re-explained here.
# ---------------------------------------------------------------
import os

FILES_ROOT = "/lakehouse/default/Files"
RAW_ROOT = FILES_ROOT + "/Raw/Mews"   # standard raw root (D-151)

if not os.path.exists(FILES_ROOT):
    raise RuntimeError(
        "Default lakehouse is not attached. "
        "Attach LH_Menja_BI_v1_Mews_DEV to this notebook, then re-run from the top."
    )
print("Default lakehouse Files area found:", FILES_ROOT)
print("Standard raw root for this run:", RAW_ROOT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 2 - Key Vault secrets
# 
# Same secrets, same pattern as NB00. Values are never printed.

# CELL ********************

# ---------------------------------------------------------------
# Section 2 - Key Vault secrets (never print secret values)
# ---------------------------------------------------------------
vault_url = "https://kv-menja-biv1.vault.azure.net/"

mews_access_token = notebookutils.credentials.getSecret(vault_url, "mews-access-token")
mews_client_token = notebookutils.credentials.getSecret(vault_url, "mews-client-token")

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
# **STOP before a live run:** `BASE_URL` below is a placeholder. No governed decision or
# repository file states the production Mews Connector host — confirm the real one before
# this notebook ever calls it. Chunking/page/retry values are carried over unchanged from
# NB00's proven D-153 bounded-window discipline, not re-derived.

# CELL ********************

# ---------------------------------------------------------------
# Section 3 - Configuration (single place for all parameters)
# ---------------------------------------------------------------
from datetime import datetime, timezone, timedelta

# --- Mews API ---
# TODO (user, before first live run): confirm the real production Connector host.
# NB00's host below is the DEMO host only - do not run this notebook live against it.
BASE_URL = "REPLACE_ME_PRODUCTION_MEWS_CONNECTOR_BASE_URL"
CLIENT_NAME = "Menja BI v1/1.0"

# D-224/D-228 governed vocabulary uses "MEWS" (uppercase). NB00 itself still writes
# PMS_NAME = "Mews" - a pre-existing, separate drift this notebook does not fix.
PMS_NAME = "MEWS"

# Same endpoint path as NB00's proven demo pull; only BASE_URL/tokens/filter differ
# between demo and live in the Mews Connector API.
RESERVATIONS_ENDPOINT = "reservations/getAll/2023-06-06"

RES_FOLDER = RAW_ROOT + "/reservations"   # same standard root as NB00 (D-151)

# --- Recurring-extraction specific (D-213) ---
OVERLAP_MINUTES = 15   # D-213: each run pulls from watermark minus this margin

# --- Chunking and page caps (D-153) - identical values to NB00, not re-derived ---
RES_CHUNK_DAYS = 7
RES_MAX_PAGES_PER_CHUNK = 5
RES_PAGE_SIZE = 1000

# --- Network safety - identical to NB00 ---
TIMEOUT_SEC = 60
RETRIES = 3
RETRY_SLEEP_SEC = 3

if BASE_URL.startswith("REPLACE_ME"):
    print("WARNING: BASE_URL is still a placeholder. Any live Mews call below will fail "
          "fast on a bad URL - fill in the real production host before relying on a run.")

print("Config loaded.")
print("Raw root:", RAW_ROOT)
print("Reservations endpoint:", RESERVATIONS_ENDPOINT)
print("Overlap margin (D-213):", OVERLAP_MINUTES, "minutes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 4 - Imports, D-186 (revised) log schemas, helper functions
# 
# **Plain words:** the log schemas here already include the current governed columns
# (`PropertyKey`, `SourceType`, `SourcePropertyCode`, `MewsScopeType`, `MewsScopeIds`) —
# unlike NB00's own local schema, which still reflects the pre-revision D-186 shape.
# `align_to_existing_table_schema` (reused unchanged) means this notebook writes correctly
# against the real table either way, but this notebook's *own* schema constants are written
# fresh against the current FINAL D-186 shape rather than copying NB00's stale one.
# 
# `mews_post` is reused from NB00 unchanged except for one thing: the scope-filter body key
# is no longer hardcoded to `ServiceIds` - it is taken literally from the governed
# `MewsScopeType` value for each property (D-215: NB06 must use the supplied values
# "exactly as supplied," not assume `ServiceIds` is the production scope type).

# CELL ********************

# ---------------------------------------------------------------
# Section 4 - Imports, D-186 (revised) log schemas, helpers
# ---------------------------------------------------------------
import json
import time
import uuid
import traceback
import requests

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
)

run_log_schema = StructType([
    StructField("RunID", StringType(), False),
    StructField("PMS", StringType(), False),
    StructField("Endpoint", StringType(), False),
    StructField("PropertyKey", StringType(), True),
    StructField("SourceType", StringType(), True),
    StructField("SourcePropertyCode", StringType(), True),
    StructField("MewsScopeType", StringType(), True),
    StructField("MewsScopeIds", StringType(), True),
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
    StructField("PropertyKey", StringType(), True),
    StructField("SourceType", StringType(), True),
    StructField("SourcePropertyCode", StringType(), True),
    StructField("MewsScopeType", StringType(), True),
    StructField("MewsScopeIds", StringType(), True),
    StructField("PageOrChunkIndex", IntegerType(), False),
    StructField("FileName", StringType(), False),
    StructField("FilePath", StringType(), False),
    StructField("RecordCount", IntegerType(), False),
    StructField("WrittenUtc", TimestampType(), False)
])


def utc_now():
    return datetime.now(timezone.utc)


def fmt_utc(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def daterange_chunks(start, end, chunk_days):
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=chunk_days), end)
        yield cur, chunk_end
        cur = chunk_end


def mews_post(endpoint, extra_body=None, cursor=None, page_size=None):
    # Reused from NB00 unchanged. Never prints secrets.
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
    # Reused from NB00 unchanged (D-148: raw exactly as received).
    os.makedirs(output_folder, exist_ok=True)
    file_path = output_folder + "/" + file_name
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return file_path


def ensure_log_tables_exist():
    # Guard only - both tables already exist with the wide D-186 shape from
    # NB_Menja_ExtractionControl_Setup. This just protects a from-scratch environment.
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
    # Reused from NB00 unchanged - aligns to whatever the REAL table looks like today.
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
print("Imports, D-186 (revised) schemas, and helpers ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 5 - Read `PropertyExtractionConfig`; find eligible LIVE properties (D-219)
# 
# Only rows with `IsLiveExtractionEnabled = TRUE` are eligible. Zero eligible rows is a
# valid, expected outcome right now (see the notebook header) - the notebook stops here
# cleanly, not with an error.

# CELL ********************

# ---------------------------------------------------------------
# Section 5 - Eligible LIVE properties (D-219)
# ---------------------------------------------------------------
cfg_all = spark.read.table("PropertyExtractionConfig")
cfg_enabled = cfg_all.filter(F.col("IsLiveExtractionEnabled") == True)

n_enabled = cfg_enabled.count()
print(f"PropertyExtractionConfig rows total: {cfg_all.count()}")
print(f"Rows with IsLiveExtractionEnabled = TRUE: {n_enabled}")

if n_enabled == 0:
    print("")
    print("No properties are enabled for live extraction. Nothing to do this run.")
    print("This is expected and correct per D-219 - not a failure.")

enabled_rows = cfg_enabled.collect() if n_enabled > 0 else []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 6 - Per-property recurring extraction (D-213 / D-214 / D-215 / D-216 / D-218)
# 
# **Plain words, one property at a time:**
# 1. Validate the config row has everything D-215/D-219 require. Missing/blank -> stop
#    *this property's* run (logged Failed), do not guess, do not touch other properties.
# 2. Look up this property's watermark. None yet -> cold start from the seed's
#    `ColdStartUpdatedUtcFrom` (D-214), no overlap subtraction (there is nothing to
#    overlap with). Found -> `WindowStartUtc = CapturedThroughUtc - 15 min` (D-213).
# 3. Capture `WindowEndUtc = now()` once, after validation, before the first request (D-218).
# 4. Chunk the window (D-153, reused), page through Mews using an `UpdatedUtc` filter
#    (not NB00's `CollidingUtc` stay-window filter), scoped by the property's own
#    `MewsScopeType` / `MewsScopeIds`, used exactly as supplied (D-215).
# 5. Land every response as a new immutable raw file (D-217 - retries are not suppressed).
# 6. Advance the watermark to `WindowEndUtc` only if the whole property run finished
#    Success - including a fully successful zero-row run (D-216). Failed/Partial never
#    advances it.
# 
# **Flagged assumption (not found in any governed source - verify before a live run):**
# the Mews `UpdatedUtc` range filter is assumed to take the same
# `{"StartUtc": ..., "EndUtc": ...}` shape as the `CollidingUtc` filter NB00 already uses
# successfully. `MewsScopeIds` is assumed to be a comma-separated list of IDs. Both are
# reasonable readings of the Mews Connector API's own conventions, but neither is stated
# in this project's governance or codebase - confirm against the real API before trusting
# a live result.


# CELL ********************

# ---------------------------------------------------------------
# Section 6 - Per-property recurring extraction
# ---------------------------------------------------------------
REQUIRED_FIELDS = ["PropertyKey", "SourcePropertyCode", "MewsScopeType",
                   "MewsScopeIds", "ColdStartUpdatedUtcFrom"]

for cfg_row in enabled_rows:
    cfg = cfg_row.asDict()
    property_key = cfg.get("PropertyKey")

    print("")
    print("=======================================================")
    print("Property:", property_key)
    print("=======================================================")

    run_id = str(uuid.uuid4())
    run_stamp = run_id[:8]
    run_start_utc = utc_now()
    status = "Success"
    error_message = None
    pages_written = 0
    record_count = 0
    window_start_utc = None
    window_end_utc = None
    source_property_code = cfg.get("SourcePropertyCode")
    mews_scope_type = cfg.get("MewsScopeType")
    mews_scope_ids_raw = cfg.get("MewsScopeIds")

    try:
        # --- D-215 / D-219: validate before doing anything else ---
        missing = [f for f in REQUIRED_FIELDS if not cfg.get(f)]
        if missing:
            raise RuntimeError(
                f"Enabled PropertyExtractionConfig row for {property_key} is missing "
                f"required field(s) {missing}. Per D-215/D-219 this must stop, not be "
                f"guessed or defaulted."
            )

        scope_ids = [s.strip() for s in mews_scope_ids_raw.split(",") if s.strip()]
        if not scope_ids:
            raise RuntimeError(
                f"MewsScopeIds for {property_key} did not yield any usable ID after "
                f"parsing '{mews_scope_ids_raw}'. Stop - do not guess a scope."
            )

        # --- D-213 / D-214: resolve WindowStartUtc from the watermark, or cold start ---
        wm = (spark.read.table("ExtractionWatermark")
              .filter((F.col("PropertyKey") == property_key)
                      & (F.col("PMS") == PMS_NAME)
                      & (F.col("SourceType") == "LIVE")
                      & (F.col("SourcePropertyCode") == source_property_code))
              .collect())

        if wm:
            captured_through = wm[0]["CapturedThroughUtc"]
            window_start_utc = captured_through - timedelta(minutes=OVERLAP_MINUTES)
            print(f"Existing watermark found. CapturedThroughUtc={captured_through} "
                  f"-> WindowStartUtc (minus {OVERLAP_MINUTES}-min overlap)={window_start_utc}")
        else:
            cold_start = cfg.get("ColdStartUpdatedUtcFrom")
            window_start_utc = cold_start
            print(f"No watermark row found - cold start. "
                  f"WindowStartUtc = ColdStartUpdatedUtcFrom = {window_start_utc} (D-214)")

        # --- D-218: fixed WindowEndUtc, captured once, before the first request ---
        window_end_utc = utc_now().replace(tzinfo=None)
        if window_start_utc.tzinfo is not None:
            window_start_utc = window_start_utc.replace(tzinfo=None)

        if window_start_utc >= window_end_utc:
            raise RuntimeError(
                f"WindowStartUtc ({window_start_utc}) is not earlier than WindowEndUtc "
                f"({window_end_utc}) for {property_key}. Stop - do not run an empty or "
                f"inverted window."
            )

        print(f"Fixed WindowEndUtc for this RunID: {window_end_utc}")

        # --- D-153 chunking (reused), D-215 scope, UpdatedUtc filter ---
        for chunk_start, chunk_end in daterange_chunks(
            window_start_utc, window_end_utc, RES_CHUNK_DAYS
        ):
            print("")
            print("Chunk:", chunk_start, "to", chunk_end)
            cursor = None
            page_index = 0
            hit_cap_with_more = False

            while page_index < RES_MAX_PAGES_PER_CHUNK:
                extra_body = {
                    mews_scope_type: scope_ids,
                    "UpdatedUtc": {
                        "StartUtc": fmt_utc(chunk_start),
                        "EndUtc": fmt_utc(chunk_end),
                    },
                }
                payload = mews_post(
                    RESERVATIONS_ENDPOINT, extra_body, cursor=cursor,
                    page_size=RES_PAGE_SIZE
                )
                page = payload.get("Reservations", [])
                if not page:
                    print("  No more reservations in this chunk.")
                    break

                page_index += 1
                pages_written += 1
                record_count += len(page)

                file_name = (
                    "reservations_live_" + property_key + "_"
                    + run_start_utc.strftime("%Y%m%d_%H%M%S") + "_" + run_stamp + "_"
                    + chunk_start.strftime("%Y%m%dT%H%M%S") + "_"
                    + chunk_end.strftime("%Y%m%dT%H%M%S")
                    + "_page_" + str(page_index).zfill(3) + ".json"
                )
                file_path = write_json_payload(RES_FOLDER, file_name, payload)

                append_file_log({
                    "FileID": str(uuid.uuid4()),
                    "RunID": run_id,
                    "PMS": PMS_NAME,
                    "Endpoint": RESERVATIONS_ENDPOINT,
                    "PropertyKey": property_key,
                    "SourceType": "LIVE",
                    "SourcePropertyCode": source_property_code,
                    "MewsScopeType": mews_scope_type,
                    "MewsScopeIds": mews_scope_ids_raw,
                    "PageOrChunkIndex": pages_written,
                    "FileName": file_name,
                    "FilePath": file_path,
                    "RecordCount": len(page),
                    "WrittenUtc": utc_now().replace(tzinfo=None)
                })
                print("  Page", page_index, ":", len(page), "reservations ->", file_name)

                cursor = payload.get("Cursor")
                if not cursor:
                    break

            if page_index >= RES_MAX_PAGES_PER_CHUNK and cursor:
                hit_cap_with_more = True
                print("  Page cap reached (", RES_MAX_PAGES_PER_CHUNK,
                      ") with more data remaining.")
                status = "Partial"

    except Exception as ex:
        status = "Failed"
        error_message = str(ex)
        print("")
        print("PROPERTY RUN FAILED:", error_message)
        print(traceback.format_exc())

    finally:
        run_end_utc = utc_now().replace(tzinfo=None)
        append_run_log({
            "RunID": run_id,
            "PMS": PMS_NAME,
            "Endpoint": RESERVATIONS_ENDPOINT,
            "PropertyKey": property_key,
            "SourceType": "LIVE",
            "SourcePropertyCode": source_property_code,
            "MewsScopeType": mews_scope_type,
            "MewsScopeIds": mews_scope_ids_raw,
            "WindowStartUtc": window_start_utc,
            "WindowEndUtc": window_end_utc,
            "RunStartUtc": run_start_utc.replace(tzinfo=None),
            "RunEndUtc": run_end_utc,
            "Status": status,
            "PagesWritten": pages_written,
            "RecordCount": record_count,
            "ErrorMessage": error_message
        })

        # --- D-216: advance watermark ONLY on a fully successful run (zero rows included) ---
        if status == "Success" and window_end_utc is not None:
            wm_row = spark.createDataFrame(
                [(property_key, PMS_NAME, "LIVE", source_property_code, window_end_utc)],
                schema=["PropertyKey", "PMS", "SourceType", "SourcePropertyCode",
                        "CapturedThroughUtc"]
            )
            (spark.read.table("ExtractionWatermark")
                  .filter(~((F.col("PropertyKey") == property_key)
                            & (F.col("PMS") == PMS_NAME)
                            & (F.col("SourceType") == "LIVE")
                            & (F.col("SourcePropertyCode") == source_property_code)))
                  .union(wm_row)
                  .write.format("delta").mode("overwrite").saveAsTable("ExtractionWatermark"))
            print(f"Watermark advanced for {property_key} -> CapturedThroughUtc = "
                  f"{window_end_utc} (D-216).")
        else:
            print(f"Watermark NOT advanced for {property_key} (Status={status}).")

        print(f"Finished {property_key}: Status={status} | Pages={pages_written} "
              f"| Records={record_count}")

if not enabled_rows:
    print("Section 6 skipped - no enabled properties (see Section 5).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Section 7 - Wrap-up — honest status
# 
# **Plain words:** when this notebook runs with zero enabled properties, it does nothing
# and that is correct. When it runs with an enabled property, `ExtractionRunLog` /
# `ExtractionFileLog` / raw JSON files / `ExtractionWatermark` reflect exactly what
# happened - nothing is "done" until:
# 
# 1. You confirm the run and inspect the Section 5/6 output.
# 2. You commit this notebook to GitHub if you want it versioned.
# 3. Any follow-up documentation is handed to Copilot.
# 
# **Not created here, by design:** the Fabric pipeline/schedule that would actually call
# this notebook on a cadence (D-213 requirement, D-210 target) - that is a separate Fabric
# object the user sets up. Hard-delete handling, backfill, `F_OTBPosition`, and all
# revenue/channel/segment/rate-plan/market-country logic remain untouched and out of scope.
# 
# **Before this can do anything on real data:** (a) confirm the real production `BASE_URL`,
# (b) add and verify a LIVE row in `B_PropertySourceIdentity`, (c) enable the matching
# `PropertyExtractionConfig` row with a real, approved `MewsScopeType`/`MewsScopeIds`, then
# re-run `NB_Menja_ExtractionControl_Setup` so `PropertyExtractionConfig.SourcePropertyCode`
# resolves before this notebook can find an eligible property.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working, to avoid
# unnecessary cost.**


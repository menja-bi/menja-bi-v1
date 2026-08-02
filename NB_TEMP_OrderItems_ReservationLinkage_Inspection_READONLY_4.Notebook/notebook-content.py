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

# # NB_TEMP_OrderItems_ReservationLinkage_Inspection_READONLY
# 
# **Purpose:** Bounded, read-only Mews source inspection to gather evidence for governance issue I-215 (order-item-to-reservation linkage semantics).
# 
# **This notebook is:**
# - Temporary. Not part of the governed demo chain (`NB00 -> NB25 -> NB10 -> NB20 -> NB30 -> NB06`).
# - Read-only against the Mews DEMO API. No Delta writes. No Lakehouse table creation, overwrite, append, merge, update, or delete.
# - Not attached to a Lakehouse by default. It does not need to be.
# - Not a governance decision. It produces evidence only. I-215, I-216, and I-196 remain OPEN until the user reviews the evidence and a FINAL decision is recorded in the workbook.
# - Not to be run against LIVE. `SOURCE_TYPE` below must remain `"DEMO"`.
# 
# **What it does NOT do:**
# - Does not decide room-night allocation, booked-vs-realized recognition, currency placement or conversion, rounding, or no-show policy.
# - Does not invent Mews API request shapes beyond what is already used in the governed extraction notebooks. Cells marked `# CONFIRM` must be checked by the user against the existing NB00 / extraction-control configuration before running.
# - Does not print or persist secret values.
# 
# Run this notebook top to bottom in the Fabric notebook environment. Paste the final evidence report (last cell's output) back into the chat for the I-215 review.


# CELL ********************

# ============================================================
# CONFIGURATION -- review every value in this cell before running
# ============================================================

# Governed source context (per D-223..D-234 property-source identity contract).
# Must remain DEMO for this inspection. Never LIVE.
PMS = "MEWS"
SOURCE_TYPE = "DEMO"   # DO NOT CHANGE to "LIVE" in this notebook.

# CONFIRM: reuse the exact DEMO connector base URL already configured in
# NB00 / NB_Menja_ExtractionControl_Setup. Do not invent a new one.
# This is a placeholder default for the public Mews demo connector; verify
# against the governed extraction-control configuration before running.
MEWS_BASE_URL = "https://api.mews-demo.com/api/connector/v1"  # CONFIRM

# Bounded scope for this inspection. Fill in a real DEMO EnterpriseId
# (property scope) if the governed extraction config already resolves one;
# leave as None to skip property-side filtering and rely on the
# reservation/services join instead.
DEMO_ENTERPRISE_ID = None  # CONFIRM -- optional DEMO property scope filter

# Bounded time windows. Keep these narrow -- this is a bounded evidence
# pull, not a full extraction. Adjust dates to a period known to contain
# DEMO reservation activity (see NB00 / prior inspection notes).
HISTORICAL_WINDOW = {"start": "2015-01-01T00:00:00Z", "end": "2019-12-31T23:59:59Z"}
CURRENT_WINDOW    = {"start": "2026-01-01T00:00:00Z", "end": "2026-07-31T23:59:59Z"}
FUTURE_WINDOW     = {"start": "2026-08-01T00:00:00Z", "end": "2026-12-31T23:59:59Z"}

# Separate page budgets per pull, to avoid one pull starving another
# (lesson from the earlier order-item inspection: a combined past/future
# pull produced a lopsided 156-future vs 8-past result).
PAGE_SIZE = 200
# Mews time-interval filters (UpdatedUtc, CreatedUtc, ConsumedUtc, etc.) cap at
# 3 months per call -- verified against docs.mews.com on 2026-08-01. The fetch
# helpers below automatically split any wider window into <=3-month chunks,
# so HISTORICAL_WINDOW / CURRENT_WINDOW / FUTURE_WINDOW above can stay as wide
# as you like; you do not need to narrow them yourself.
MAX_PAGES = {
    "reservations_current":   15,
    "reservations_historical": 10,
    "reservations_future":    10,
    "services":                5,
    "orderitems_unfiltered":  20,   # the decisive unfiltered pull
    "orderitems_filtered":    10,   # reservation-filtered comparison pull
}

# Sample size for the filtered comparison pull (T3). Chosen from the
# tagged reservation sample, capped to keep the pull bounded.
FILTERED_SAMPLE_SIZE = 40

# Optional: persist a plain (non-Delta) JSON scratch snapshot outside the
# governed Files/Raw/Mews path, purely to allow a *second run* of this
# notebook to test T7 (stability). Off by default. This is a plain file
# write for evidence-gathering only -- it is not a Delta table and is not
# part of the governed demo chain. Leave False unless you intend to re-run
# this notebook later to compare against a prior snapshot.
PERSIST_SCRATCH_COPY = False
SCRATCH_PATH = "Files/Scratch/menja_i215_inspection_snapshot.json"  # outside governed Files/Raw/Mews

print("Config loaded. SOURCE_TYPE =", SOURCE_TYPE, "| PMS =", PMS)
assert SOURCE_TYPE == "DEMO", "This notebook must only run against DEMO. Stopping."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CREDENTIALS -- retrieved from Key Vault only. Never printed.
# ============================================================
# Fabric's notebookutils.credentials.getSecret requires the FULL Key Vault
# URI, not just the vault name. Confirm this URI against the Key Vault
# resource in Azure (kv-menja-biv1) if it differs from the standard
# "<name>.vault.azure.net" pattern.
KEY_VAULT_URI = "https://kv-menja-biv1.vault.azure.net/"  # CONFIRM

try:
    import notebookutils  # available in Fabric runtime
    mews_access_token = notebookutils.credentials.getSecret(KEY_VAULT_URI, "mews-access-token")
    mews_client_token  = notebookutils.credentials.getSecret(KEY_VAULT_URI, "mews-client-token")
    print("Credentials retrieved from Key Vault. (Values not printed.)")
except Exception as e:
    raise RuntimeError(
        "Could not retrieve secrets from Key Vault '%s'. "
        "Confirm this notebook is running inside Fabric with access to the "
        "existing extraction-control Key Vault connection, and that the URI "
        "above matches the Key Vault resource exactly. Original error: %r"
        % (KEY_VAULT_URI, e)
    )

# Safety check: never allow secret values into any print/log statement below.
assert isinstance(mews_access_token, str) and len(mews_access_token) > 0
assert isinstance(mews_client_token, str) and len(mews_client_token) > 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# MEWS API HELPER -- read-only POST calls with cursor-based paging.
# Verified against the published Mews Connector API documentation
# (docs.mews.com) on 2026-08-01:
#   - Cursor is nested inside Limitation, not a top-level request field.
#   - Time-interval filters (UpdatedUtc etc.) cap at 3 months per call;
#     chunk_time_window() / mews_get_all_paged_over_window() below handle
#     splitting a wider window into safe chunks automatically.
#   - orderItems/getAll requires at least one of a fixed set of filters
#     (OrderItemIds, AccountIds, ServiceOrderIds, ServiceIds, BillIds,
#     CreatedUtc, UpdatedUtc, ConsumedUtc, CanceledUtc, ClosedUtc) --
#     UpdatedUtc alone satisfies this for the unfiltered pull.
# Do not modify this to perform any write/update Mews operation.
# ============================================================
import requests
import json               # used directly by the T1-T8 and evidence-report cells below
import json as _json      # internal alias used only inside this cell's HTTP helpers
import time
import datetime

MEWS_HEADERS = {"Content-Type": "application/json"}

def _mews_post(endpoint: str, body: dict, timeout: int = 30) -> dict:
    """Single read-only POST call to a Mews getAll-style endpoint."""
    payload = dict(body)
    payload["ClientToken"] = mews_client_token
    payload["AccessToken"] = mews_access_token
    payload["Client"] = "Menja BI v1 I-215 Inspection 1.0"
    url = f"{MEWS_BASE_URL}/{endpoint}"
    resp = requests.post(url, headers=MEWS_HEADERS, data=_json.dumps(payload), timeout=timeout)
    if not resp.ok:
        # Surface the actual Mews error body instead of a bare status code,
        # so any future failure is diagnosable without another round trip.
        # Mews error responses do not echo back credentials.
        raise requests.HTTPError(
            f"{resp.status_code} error calling {endpoint}: {resp.text[:2000]}",
            response=resp,
        )
    return resp.json()

def mews_get_all_paged(endpoint: str, body: dict, result_key: str, max_pages: int,
                        page_size: int = PAGE_SIZE, sleep_seconds: float = 0.2) -> tuple:
    """
    Read-only paged fetch for a Mews *_/getAll endpoint using cursor paging.
    Cursor is correctly nested inside the Limitation object, per the
    published Mews pagination contract. Returns (records, meta) where meta
    records whether the page cap was hit.
    """
    records = []
    cursor = None
    pages_fetched = 0
    cap_hit = False
    for _ in range(max_pages):
        req_body = dict(body)
        limitation = {"Count": page_size}
        if cursor:
            limitation["Cursor"] = cursor
        req_body["Limitation"] = limitation
        data = _mews_post(endpoint, req_body)
        page_records = data.get(result_key, [])
        records.extend(page_records)
        pages_fetched += 1
        cursor = data.get("Cursor")
        if not cursor or len(page_records) < page_size:
            break
        time.sleep(sleep_seconds)
    else:
        cap_hit = True
    meta = {"endpoint": endpoint, "pages_fetched": pages_fetched,
            "page_cap": max_pages, "cap_hit": cap_hit, "record_count": len(records)}
    return records, meta

def chunk_time_window(start_iso: str, end_iso: str, max_days: int = 89) -> list:
    """Split a UTC ISO8601 window into consecutive chunks of at most
    max_days each. Mews time-interval filters cap at 3 months; 89 days is
    used as a safe margin under that cap."""
    start = datetime.datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
    end = datetime.datetime.strptime(end_iso, "%Y-%m-%dT%H:%M:%SZ")
    chunks = []
    cur = start
    step = datetime.timedelta(days=max_days)
    while cur < end:
        nxt = min(cur + step, end)
        chunks.append((cur.strftime("%Y-%m-%dT%H:%M:%SZ"), nxt.strftime("%Y-%m-%dT%H:%M:%SZ")))
        cur = nxt
    return chunks

def mews_get_all_paged_over_window(endpoint: str, base_body: dict, time_field: str,
                                    window: dict, result_key: str, max_pages: int,
                                    page_size: int = PAGE_SIZE) -> tuple:
    """
    Pages through `endpoint` across one or more <=3-month sub-windows of
    `window`, applying `time_field` (e.g. 'UpdatedUtc') as the Mews time
    filter for each chunk. The page budget (max_pages) is shared across
    ALL chunks, not reset per chunk, so one wide window cannot silently
    consume more pages than the caller asked for.
    """
    chunks = chunk_time_window(window["start"], window["end"])
    all_records = []
    pages_used = 0
    cap_hit = False
    chunk_metas = []
    for (c_start, c_end) in chunks:
        remaining = max_pages - pages_used
        if remaining <= 0:
            cap_hit = True
            break
        body = dict(base_body)
        body[time_field] = {"StartUtc": c_start, "EndUtc": c_end}
        records, meta = mews_get_all_paged(endpoint, body, result_key, remaining, page_size)
        all_records.extend(records)
        pages_used += meta["pages_fetched"]
        chunk_metas.append({"window": [c_start, c_end], **meta})
        if meta["cap_hit"]:
            cap_hit = True
    meta_out = {
        "endpoint": endpoint,
        "pages_fetched": pages_used,
        "page_cap": max_pages,
        "cap_hit": cap_hit,
        "record_count": len(all_records),
        "chunks": chunk_metas,
    }
    return all_records, meta_out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GOVERNED-SCOPE FETCH FUNCTIONS
# One function per raw object needed for this inspection. Each is a plain
# read-only getAll call scoped to DEMO. No write parameters are ever sent.
# Request shapes verified against docs.mews.com on 2026-08-01.
# ============================================================

def fetch_reservations(window: dict, max_pages: int) -> tuple:
    """reservations/getAll/2023-06-06, bounded by an UpdatedUtc window.
    The plain reservations/getAll (no version suffix) is DEPRECATED and
    uses an incompatible body shape (ServiceIds required, Extent required)
    -- do not use it. Automatically chunked into <=3-month windows."""
    base_body = {}
    if DEMO_ENTERPRISE_ID:
        base_body["EnterpriseIds"] = [DEMO_ENTERPRISE_ID]
    return mews_get_all_paged_over_window(
        "reservations/getAll/2023-06-06", base_body, "UpdatedUtc", window, "Reservations", max_pages
    )

def fetch_services(max_pages: int) -> tuple:
    """services/getAll -- used to resolve ServiceId -> EnterpriseId (T6).
    No time filter is applied here, so no 3-month chunking is needed."""
    body = {}
    if DEMO_ENTERPRISE_ID:
        body["EnterpriseIds"] = [DEMO_ENTERPRISE_ID]
    return mews_get_all_paged("services/getAll", body, "Services", max_pages)

def fetch_orderitems_unfiltered(window: dict, max_pages: int) -> tuple:
    """orderItems/getAll bounded ONLY by property scope and time window --
    the decisive unfiltered pull. No ReservationIds / ServiceOrderIds are
    passed in the request. UpdatedUtc alone satisfies orderItems/getAll's
    'at least one filter' requirement. Automatically chunked into
    <=3-month windows."""
    base_body = {}
    if DEMO_ENTERPRISE_ID:
        base_body["EnterpriseIds"] = [DEMO_ENTERPRISE_ID]
    return mews_get_all_paged_over_window(
        "orderItems/getAll", base_body, "UpdatedUtc", window, "OrderItems", max_pages
    )

def fetch_orderitems_filtered_by_reservations(reservation_ids: list, max_pages: int) -> tuple:
    """orderItems/getAll filtered by ServiceOrderIds = reservation IDs.
    This mirrors the earlier reservation-filtered inspection pattern and
    exists ONLY to support the T3 echo-control comparison against the
    unfiltered pull -- it must not be treated as linkage evidence on its
    own (that is exactly the ambiguity T2/T3 are designed to resolve).
    No time filter is used, so no chunking is needed; ServiceOrderIds
    alone satisfies the 'at least one filter' requirement."""
    body = {"ServiceOrderIds": reservation_ids}
    return mews_get_all_paged("orderItems/getAll", body, "OrderItems", max_pages)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pulls
# 
# Five bounded pulls. Each has its own page budget from the config cell so that one pull cannot starve another.
# 
# 1. Reservations -- current, historical, future windows (separate budgets).
# 2. Services -- for the ServiceId -> EnterpriseId property lookup.
# 3. Order items, **unfiltered** -- the decisive pull for T1, T2, T4, T5, T6, T8.
# 4. A tagged sample of reservations across case categories (multi-night, day-use, cancelled, group/block, etc.), drawn from pull 1.
# 5. Order items, **filtered** by that sample's reservation IDs -- used only for the T3 echo-control comparison.

# CELL ********************

# ============================================================
# PULL A -- Reservations (current, historical, future; separate budgets)
# ============================================================
reservations_current, meta_res_current = fetch_reservations(CURRENT_WINDOW, MAX_PAGES["reservations_current"])
reservations_historical, meta_res_hist = fetch_reservations(HISTORICAL_WINDOW, MAX_PAGES["reservations_historical"])
reservations_future, meta_res_future = fetch_reservations(FUTURE_WINDOW, MAX_PAGES["reservations_future"])

all_reservations = reservations_current + reservations_historical + reservations_future
reservation_ids_governed = {r["Id"] for r in all_reservations if r.get("Id")}

print("Reservations current:", meta_res_current)
print("Reservations historical:", meta_res_hist)
print("Reservations future:", meta_res_future)
print("Total distinct reservation IDs pulled:", len(reservation_ids_governed))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PULL B -- Services (for ServiceId -> EnterpriseId lookup, T6)
# ============================================================
services, meta_services = fetch_services(MAX_PAGES["services"])
service_id_to_enterprise = {s["Id"]: s.get("EnterpriseId") for s in services if s.get("Id")}

print("Services pulled:", meta_services)
print("Distinct services with EnterpriseId:", sum(1 for v in service_id_to_enterprise.values() if v))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PULL C -- Order items, UNFILTERED (decisive pull)
# Bounded only by property scope (if set) and time window. No
# ReservationIds / ServiceOrderIds are passed into this request.
# ============================================================
orderitems_unfiltered_current, meta_oi_current = fetch_orderitems_unfiltered(CURRENT_WINDOW, MAX_PAGES["orderitems_unfiltered"])
orderitems_unfiltered_historical, meta_oi_hist = fetch_orderitems_unfiltered(HISTORICAL_WINDOW, MAX_PAGES["orderitems_unfiltered"])
orderitems_unfiltered_future, meta_oi_future = fetch_orderitems_unfiltered(FUTURE_WINDOW, MAX_PAGES["orderitems_unfiltered"])

orderitems_unfiltered = orderitems_unfiltered_current + orderitems_unfiltered_historical + orderitems_unfiltered_future

print("Order items unfiltered (current):", meta_oi_current)
print("Order items unfiltered (historical):", meta_oi_hist)
print("Order items unfiltered (future):", meta_oi_future)
print("Total unfiltered order items:", len(orderitems_unfiltered))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SAMPLE SELECTION -- tag reservations by case category from the governed
# pull. Categories are observational tags, not governed classifications.
# Report what was and was not found; do not fabricate missing cases.
# ============================================================
from collections import defaultdict, Counter

# Verified against docs.mews.com and consistent with governed issue I-181:
# Mews assigns a GroupId to every reservation, including "groups of one".
# So GroupId presence alone cannot identify a genuine multi-reservation
# group -- a reservation is tagged group_or_block only when its GroupId is
# shared by more than one reservation in the pulled sample.
group_id_counts = Counter(r.get("GroupId") for r in all_reservations if r.get("GroupId"))

def classify_reservation(r: dict) -> set:
    tags = set()
    arr = r.get("ScheduledStartUtc") or r.get("StartUtc")
    dep = r.get("ScheduledEndUtc") or r.get("EndUtc")
    state = (r.get("State") or "").lower()
    cancel_reason = (r.get("CancellationReason") or "").lower()

    if arr and dep:
        tags.add("multi_night" if arr != dep else "day_use")
    if state == "canceled" and cancel_reason == "noshow":
        tags.add("no_show")
    elif state == "canceled":
        tags.add("cancelled")
    gid = r.get("GroupId")
    if gid and group_id_counts.get(gid, 0) > 1:
        tags.add("group_or_block")
    return tags

reservation_tags = {r["Id"]: classify_reservation(r) for r in all_reservations if r.get("Id")}
tag_buckets = defaultdict(list)
for rid, tags in reservation_tags.items():
    for t in tags:
        tag_buckets[t].append(rid)

sample_ids = []
for cat in ["multi_night", "day_use", "cancelled", "no_show", "group_or_block"]:
    sample_ids.extend(tag_buckets.get(cat, [])[:8])
sample_ids = list(dict.fromkeys(sample_ids))[:FILTERED_SAMPLE_SIZE]

print("Case categories observed (count of reservations tagged):")
for cat in ["multi_night", "day_use", "cancelled", "no_show", "group_or_block"]:
    print(f"  {cat:16s}: {len(tag_buckets.get(cat, []))}")
print("Sample reservation IDs selected for T3 filtered pull:", len(sample_ids))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PULL D -- Order items, FILTERED by the sample reservation IDs.
# Used only for the T3 echo-control comparison against the unfiltered pull.
# ============================================================
if sample_ids:
    orderitems_filtered, meta_oi_filtered = fetch_orderitems_filtered_by_reservations(
        sample_ids, MAX_PAGES["orderitems_filtered"]
    )
else:
    orderitems_filtered, meta_oi_filtered = [], {"note": "no sample reservation IDs available"}

print("Order items filtered pull:", meta_oi_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tests T1 – T8

# CELL ********************

# ============================================================
# T1 -- ServiceOrderId population and null rate by item Type
# ============================================================
from collections import Counter

def t1_population_by_type(items: list) -> dict:
    by_type_total = Counter()
    by_type_null = Counter()
    for it in items:
        t = it.get("Type", "UNKNOWN")
        by_type_total[t] += 1
        if not it.get("ServiceOrderId"):
            by_type_null[t] += 1
    return {
        t: {
            "total": by_type_total[t],
            "null_service_order_id": by_type_null.get(t, 0),
            "null_rate_pct": round(100 * by_type_null.get(t, 0) / by_type_total[t], 1) if by_type_total[t] else None,
        }
        for t in by_type_total
    }

t1_result = t1_population_by_type(orderitems_unfiltered)
print(json.dumps(t1_result, indent=2))

accommodation_types = {"SpaceOrder"}  # CONFIRM against observed Type values before drawing conclusions
t1_accommodation_null_rate = t1_result.get("SpaceOrder", {}).get("null_rate_pct")
t1_status = "FAIL" if (t1_accommodation_null_rate is None) else (
    "PASS" if t1_accommodation_null_rate <= 5 else "FAIL"
)
print("T1 status:", t1_status, "(SpaceOrder null rate:", t1_accommodation_null_rate, "%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T2 -- DECISIVE TEST. ServiceOrderId membership in governed Reservations[].Id
# ============================================================
oi_with_soid = [it for it in orderitems_unfiltered if it.get("ServiceOrderId")]
matched = [it for it in oi_with_soid if it["ServiceOrderId"] in reservation_ids_governed]
unmatched = [it for it in oi_with_soid if it["ServiceOrderId"] not in reservation_ids_governed]

t2_result = {
    "order_items_with_service_order_id": len(oi_with_soid),
    "matched_to_governed_reservation": len(matched),
    "unmatched": len(unmatched),
    "match_rate_pct": round(100 * len(matched) / len(oi_with_soid), 1) if oi_with_soid else None,
}
print(json.dumps(t2_result, indent=2))

# Matched rate among SpaceOrder (accommodation) items specifically -- this is
# the number that actually answers I-215.
oi_spaceorder_with_soid = [it for it in oi_with_soid if it.get("Type") == "SpaceOrder"]
matched_spaceorder = [it for it in oi_spaceorder_with_soid if it["ServiceOrderId"] in reservation_ids_governed]
t2_spaceorder_match_rate = (
    round(100 * len(matched_spaceorder) / len(oi_spaceorder_with_soid), 1)
    if oi_spaceorder_with_soid else None
)
print("SpaceOrder-specific match rate:", t2_spaceorder_match_rate, "%",
      f"({len(matched_spaceorder)}/{len(oi_spaceorder_with_soid)})")

t2_status = "FAIL" if t2_spaceorder_match_rate is None else (
    "PASS" if t2_spaceorder_match_rate >= 95 else "FAIL"
)
print("T2 status:", t2_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T3 -- Echo control. Compare filtered pull vs unfiltered pull for the
# same sample reservations.
# ============================================================
unfiltered_ids = {it["Id"] for it in orderitems_unfiltered if it.get("Id")}
filtered_ids = {it["Id"] for it in orderitems_filtered if it.get("Id")}

only_in_filtered = filtered_ids - unfiltered_ids
only_in_unfiltered_for_sample = set()  # items in unfiltered pull tied to sample reservations, not in filtered pull
sample_set = set(sample_ids)
for it in orderitems_unfiltered:
    if it.get("ServiceOrderId") in sample_set and it.get("Id") not in filtered_ids:
        only_in_unfiltered_for_sample.add(it["Id"])

t3_result = {
    "filtered_item_count": len(filtered_ids),
    "unfiltered_item_count_total": len(unfiltered_ids),
    "items_only_in_filtered_pull": len(only_in_filtered),
    "items_only_in_unfiltered_pull_for_sample_reservations": len(only_in_unfiltered_for_sample),
}
print(json.dumps(t3_result, indent=2))

t3_status = "INCONCLUSIVE" if not sample_ids else (
    "PASS" if (len(only_in_filtered) == 0 and len(only_in_unfiltered_for_sample) == 0) else "FAIL"
)
print("T3 status:", t3_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T4 -- Cardinality and ambiguity
# ============================================================
soid_counts = Counter(it["ServiceOrderId"] for it in orderitems_unfiltered if it.get("ServiceOrderId"))
# Reservation matches per item is always <=1 by construction (ServiceOrderId is a single value per item);
# the real ambiguity question is whether one ServiceOrderId maps to more than one governed reservation,
# which cannot happen if Reservations[].Id is unique -- verify that assumption too.
reservation_id_counts = Counter(r["Id"] for r in all_reservations if r.get("Id"))
duplicate_reservation_ids = {rid: c for rid, c in reservation_id_counts.items() if c > 1}

items_per_soid_distribution = Counter(soid_counts.values())

t4_result = {
    "distinct_service_order_ids_in_orderitems": len(soid_counts),
    "distinct_reservation_ids_pulled": len(reservation_id_counts),
    "duplicate_reservation_ids_in_pull": len(duplicate_reservation_ids),
    "items_per_service_order_id_distribution": dict(items_per_soid_distribution),
}
print(json.dumps(t4_result, indent=2))

t4_status = "PASS" if len(duplicate_reservation_ids) == 0 else "FAIL"
print("T4 status:", t4_status)
if duplicate_reservation_ids:
    print("Example duplicate reservation IDs (ambiguity):", list(duplicate_reservation_ids.items())[:5])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T5 -- Profile unmatched ServiceOrderId values
# ============================================================
def profile(items, field):
    return dict(Counter(it.get(field, "UNKNOWN") for it in items))

t5_result = {
    "unmatched_count": len(unmatched),
    "by_type": profile(unmatched, "Type"),
    "by_revenue_type": profile(unmatched, "RevenueType"),
    "by_data_discriminator": Counter(
        (it.get("Data") or {}).get("Discriminator", "UNKNOWN") for it in unmatched
    ),
}
t5_result["by_data_discriminator"] = dict(t5_result["by_data_discriminator"])
print(json.dumps(t5_result, indent=2))

unmatched_spaceorder = [it for it in unmatched if it.get("Type") == "SpaceOrder"]
t5_status = "PASS" if len(unmatched_spaceorder) == 0 else "FAIL"
print("T5 status:", t5_status, "| unmatched SpaceOrder (accommodation) items:", len(unmatched_spaceorder))
if unmatched_spaceorder:
    print("Example unmatched accommodation item IDs:", [it.get("Id") for it in unmatched_spaceorder[:5]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T6 -- Compare OrderItems[].EnterpriseId (direct) vs the property reached
# by traversing ServiceId -> Services[].EnterpriseId
# ============================================================
t6_rows = []
for it in matched:  # only meaningful for items already tied to a governed reservation
    direct = it.get("EnterpriseId")
    via_service = service_id_to_enterprise.get(it.get("ServiceId"))
    t6_rows.append({
        "item_id": it.get("Id"),
        "direct_enterprise_id": direct,
        "via_service_enterprise_id": via_service,
        "equal": (direct == via_service) if (direct and via_service) else None,
    })

both_present = [r for r in t6_rows if r["equal"] is not None]
agree = [r for r in both_present if r["equal"]]
disagree = [r for r in both_present if not r["equal"]]

t6_result = {
    "items_with_both_fields_present": len(both_present),
    "agree": len(agree),
    "disagree": len(disagree),
    "direct_field_missing_count": sum(1 for r in t6_rows if not r["direct_enterprise_id"]),
}
print(json.dumps(t6_result, indent=2))

t6_status = "INCONCLUSIVE" if not both_present else ("PASS" if len(disagree) == 0 else "FAIL")
print("T6 status:", t6_status)
if disagree:
    print("Example disagreements:", disagree[:5])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T7 -- Stability comparison. Requires a prior comparable snapshot.
# Do NOT manufacture a result if no baseline exists.
# ============================================================
import os

t7_status = "INCONCLUSIVE"
t7_result = {"note": "No earlier comparable pull available in this run."}

if PERSIST_SCRATCH_COPY:
    try:
        current_snapshot = {
            it["Id"]: it.get("ServiceOrderId")
            for it in orderitems_unfiltered if it.get("Id")
        }
        prior_snapshot = None
        try:
            with open("/lakehouse/default/" + SCRATCH_PATH, "r") as f:
                prior_snapshot = _json.load(f)
        except Exception:
            prior_snapshot = None

        if prior_snapshot:
            common_ids = set(current_snapshot) & set(prior_snapshot)
            changed = [i for i in common_ids if current_snapshot[i] != prior_snapshot[i]]
            t7_result = {
                "prior_snapshot_items": len(prior_snapshot),
                "current_snapshot_items": len(current_snapshot),
                "common_items_compared": len(common_ids),
                "changed_service_order_id": len(changed),
            }
            t7_status = "PASS" if len(changed) == 0 else "FAIL"
        else:
            t7_result = {"note": "PERSIST_SCRATCH_COPY is on but no prior snapshot file was found. "
                                  "This run will write one; re-run later to complete T7."}

        with open("/lakehouse/default/" + SCRATCH_PATH, "w") as f:
            _json.dump(current_snapshot, f)
    except Exception as e:
        t7_result = {"note": f"Scratch snapshot write/read failed: {e!r}. T7 remains inconclusive."}
else:
    t7_result = {"note": "PERSIST_SCRATCH_COPY is off. Stability cannot be concluded from a single run."}

print(json.dumps(t7_result, indent=2))
print("T7 status:", t7_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# T8 -- Null and edge inventory by Type, for the four candidate fields
# ============================================================
candidate_fields = ["ServiceOrderId", "ServiceId", "EnterpriseId", "BillId"]

t8_result = {}
for field in candidate_fields:
    by_type = defaultdict(lambda: {"total": 0, "null": 0})
    for it in orderitems_unfiltered:
        t = it.get("Type", "UNKNOWN")
        by_type[t]["total"] += 1
        if not it.get(field):
            by_type[t]["null"] += 1
    t8_result[field] = {t: v for t, v in by_type.items()}

print(json.dumps(t8_result, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample coverage and evidence report

# CELL ********************

# ============================================================
# SAMPLE COVERAGE -- report which requested case types were actually
# observed. Do not fabricate coverage that was not found.
# ============================================================
requested_cases = [
    "multi_night_overnight_stay", "day_use_reservation", "cancelled_reservation",
    "no_show", "group_or_block_linked_reservation", "rebate_item",
    "product_only_order", "historical_records", "current_records", "future_dated_reservations",
]

observed_cases = {}
observed_cases["multi_night_overnight_stay"] = len(tag_buckets.get("multi_night", [])) > 0
observed_cases["day_use_reservation"] = len(tag_buckets.get("day_use", [])) > 0
observed_cases["cancelled_reservation"] = len(tag_buckets.get("cancelled", [])) > 0
observed_cases["no_show"] = len(tag_buckets.get("no_show", [])) > 0
observed_cases["group_or_block_linked_reservation"] = len(tag_buckets.get("group_or_block", [])) > 0
observed_cases["rebate_item"] = any(
    "rebate" in (it.get("Type") or "").lower() for it in orderitems_unfiltered
)
observed_cases["product_only_order"] = any(
    (it.get("Type") == "ProductOrder") and not it.get("ServiceOrderId") for it in orderitems_unfiltered
)
observed_cases["historical_records"] = len(reservations_historical) > 0
observed_cases["current_records"] = len(reservations_current) > 0
observed_cases["future_dated_reservations"] = len(reservations_future) > 0

print("Sample coverage (observed = True/False, not fabricated):")
for case in requested_cases:
    print(f"  {case:38s}: {observed_cases.get(case)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# COMPACT EVIDENCE REPORT -- copy this cell's printed output into the
# I-215 governance review chat.
# ============================================================
import datetime

page_caps_hit = [
    m["endpoint"] + f" ({m['pages_fetched']}/{m['page_cap']} pages)"
    for m in [meta_res_current, meta_res_hist, meta_res_future, meta_services,
              meta_oi_current, meta_oi_hist, meta_oi_future]
    if m.get("cap_hit")
]
if meta_oi_filtered.get("cap_hit"):
    page_caps_hit.append("orderItems/getAll filtered pull")

evidence_report = {
    "generated_utc": datetime.datetime.utcnow().isoformat() + "Z",
    "notebook": "NB_TEMP_OrderItems_ReservationLinkage_Inspection_READONLY",
    "governance_issue": "I-215 (order-item-to-reservation linkage semantics)",
    "source_type": SOURCE_TYPE,
    "scope": {
        "enterprise_id_filter": DEMO_ENTERPRISE_ID,
        "windows": {
            "historical": HISTORICAL_WINDOW,
            "current": CURRENT_WINDOW,
            "future": FUTURE_WINDOW,
        },
    },
    "record_counts": {
        "reservations_pulled": len(all_reservations),
        "services_pulled": len(services),
        "orderitems_unfiltered_pulled": len(orderitems_unfiltered),
        "orderitems_filtered_pulled": len(orderitems_filtered),
        "sample_reservations_for_filtered_pull": len(sample_ids),
    },
    "page_caps_hit": page_caps_hit if page_caps_hit else "none",
    "tests": {
        "T1_population_and_null_rate_by_type": {"status": t1_status, "result": t1_result},
        "T2_service_order_id_membership_in_reservations": {"status": t2_status, "result": t2_result,
            "spaceorder_specific_match_rate_pct": t2_spaceorder_match_rate},
        "T3_filtered_vs_unfiltered_echo_control": {"status": t3_status, "result": t3_result},
        "T4_cardinality_and_ambiguity": {"status": t4_status, "result": t4_result},
        "T5_unmatched_profile": {"status": t5_status, "result": t5_result},
        "T6_enterprise_id_direct_vs_traversal": {"status": t6_status, "result": t6_result},
        "T7_stability": {"status": t7_status, "result": t7_result},
        "T8_null_edge_inventory": {"result": t8_result},
    },
    "sample_coverage_observed": observed_cases,
    "example_identifiers": {
        "matched_spaceorder_item_ids_sample": [it.get("Id") for it in matched_spaceorder][:5] if unmatched_spaceorder else [it.get("Id") for it in matched][:5],
        "unmatched_spaceorder_item_ids_sample": [it.get("Id") for it in unmatched_spaceorder][:5],
        "duplicate_reservation_ids_sample": list(duplicate_reservation_ids.keys())[:5],
    },
    "limitations": [
        "DEMO data is structurally valid but demand-unrealistic (I-154); rates observed here support structural conclusions, not production volumes.",
        "Page caps were assigned per pull to avoid starvation; see page_caps_hit above for any pull that hit its cap.",
        "T7 stability requires a second run with PERSIST_SCRATCH_COPY enabled; a single run cannot conclude stability.",
        "This report is evidence only. It does not constitute a governance decision and assigns no DecisionID.",
    ],
    "signal_for_candidate_conclusions": {
        "ServiceOrderId_as_reservation_link":
            "SUPPORTED" if (t2_status == "PASS" and t3_status in ("PASS", "INCONCLUSIVE") and t4_status == "PASS")
            else "NOT SUPPORTED" if t2_status == "FAIL"
            else "INCONCLUSIVE",
        "direct_EnterpriseId_as_property_scope":
            "SUPPORTED" if t6_status == "PASS" else ("NOT SUPPORTED" if t6_status == "FAIL" else "INCONCLUSIVE"),
        "mandatory_ServiceId_to_Service_traversal":
            "REQUIRED" if t6_status == "FAIL" else ("NOT REQUIRED" if t6_status == "PASS" else "INCONCLUSIVE"),
    },
}

print(json.dumps(evidence_report, indent=2, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# **End of inspection.** Copy the JSON output of the cell above and paste it back into the I-215 governance review chat. Do not edit the values. If any test shows `INCONCLUSIVE`, say so explicitly rather than rounding it to PASS or FAIL.
# 
# **Reminder:** this notebook made live read-only calls to the Mews DEMO API. No governance conclusion is final until the user reviews this evidence and a decision is recorded in the workbook. Pause Fabric capacity `fabaurorabiv1devf2` in Azure when done.

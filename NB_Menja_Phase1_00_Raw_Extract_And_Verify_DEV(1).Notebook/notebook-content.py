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
# **Scope:** the eleven raw source objects marked `ActiveForBIv1 = True` in
# `07_Raw_Source_Objects` of `Menja_Schema_Governance_0626.xlsx`.
# 
# | # | RawObjectID | Endpoint called by this notebook | Landed by |
# |---|---|---|---|
# | 1 | RAW_MEWS_RESERVATIONS | `reservations/getAll/2023-06-06` | Section 5 |
# | 2 | RAW_MEWS_SERVICES | `services/getAll` | Section 6 |
# | 3 | RAW_MEWS_AGE_CATEGORIES | `ageCategories/getAll` | Section 6 |
# | 4 | RAW_MEWS_AVAILABILITY_BLOCKS | `availabilityBlocks/getAll` | Section 6B |
# | 5 | RAW_MEWS_BUSINESS_SEGMENTS | `businessSegments/getAll` | Section 6B |
# | 6 | RAW_MEWS_COMPANIES | `companies/getAll` | Section 6B |
# | 7 | RAW_MEWS_ORDER_ITEMS | `orderItems/getAll` | Section 6B |
# | 8 | RAW_MEWS_RATES | `rates/getAll` | Section 6B |
# | 9 | RAW_MEWS_RESOURCE_BLOCKS | `resourceBlocks/getAll` | Section 6B |
# | 10 | RAW_MEWS_RESOURCE_CATEGORIES | `resourceCategories/getAll` | Section 6B |
# | 11 | RAW_MEWS_RESOURCES | `resources/getAll` | Section 6B |
# 
# Row 1 calls the current versioned reservations endpoint. `reservations/getAll`
# without a version suffix is deprecated and takes an incompatible body, so the
# workbook value `EndpointOrEntity = reservations/getAll` is read as the object
# name, not as the literal URL.
# 
# **Deliberately NOT landed here:**
# 
# - `reservationGroups/getAll` - `ActiveForBIv1 = Review` under D-200, pending I-193.
# - `bills/getAll` and `configuration/get` - parked in `09_ObjectDictionary` only (D-159).
# 
# Section 3 stops the run if the configured object set is not exactly these eleven.
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
# | D-159 | The governed KEEP NOW raw object set for BI v1 ETL scope |
# | D-186 | ExtractionRunLog + ExtractionFileLog as Delta tables |
# | D-200 | reservationGroups is review-only, not an active raw object |
# | D-201 | ageCategories is a governed active raw object |
# 
# **Known governance gap - NOT closed by this notebook.**
# D-186 was revised on 2026-07-25 and now requires `PropertyKey`, `SourceType`,
# `SourcePropertyCode`, `MewsScopeType`, and `MewsScopeIds` on both log tables.
# This notebook still writes the original D-186 field set. Closing that gap needs
# the governed property/source resolution in D-215 / D-223 / D-228 and is a
# separate governed change. See `NB00_11Objects_AMENDMENT_REVIEW.md`.
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
# 
# Key Vault is addressed by its **full URI**. The bare vault name does not work
# from Fabric and was one of the four Mews/Fabric bugs already fixed in this
# project.


# CELL ********************

# Section 2 - Mews API tokens from Azure Key Vault
# Secret VALUES are never printed, logged, or written to any file.

from notebookutils import mssparkutils

KEY_VAULT_URI = "https://kv-menja-biv1.vault.azure.net/"
CLIENT_TOKEN_SECRET_NAME = "mews-client-token"
ACCESS_TOKEN_SECRET_NAME = "mews-access-token"

mews_client_token = mssparkutils.credentials.getSecret(
    KEY_VAULT_URI, CLIENT_TOKEN_SECRET_NAME
)
mews_access_token = mssparkutils.credentials.getSecret(
    KEY_VAULT_URI, ACCESS_TOKEN_SECRET_NAME
)

if not mews_client_token or not mews_access_token:
    raise RuntimeError(
        "One or both Mews tokens are empty. Check the secret names in "
        + KEY_VAULT_URI
        + " and that this notebook's identity has Get permission on secrets."
    )

print("Key Vault URI:      ", KEY_VAULT_URI)
print("Client token loaded:", bool(mews_client_token))
print("Access token loaded:", bool(mews_access_token))
print("Secret values are never printed by this notebook.")


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
# - Endpoint subfolders for reservations, services, and ageCategories are kept
#   EXACTLY where earlier committed runs landed files (`reservations`,
#   `services/getAll`, `ageCategories/getAll`). Unifying the subfolder naming
#   later is a manual cleanup decision, not something this notebook does silently.
#   The eight objects added by this amendment use `<endpoint>` as their subfolder,
#   which matches the `services/getAll` / `ageCategories/getAll` pattern.
# - The reservations window is the same bounded window the proven landing run
#   used (D-153). Widen it deliberately when you decide to - keep it bounded.
# - `RES_SERVICE_IDS` is the Mews demo service used by the committed landing
#   notebook. Widening to more services is a user decision, not a default.
# 
# **Two config lists, on purpose:**
# 
# - `RAW_SIMPLE_ENDPOINTS` - services and ageCategories. One POST, one file, no
#   `Limitation` block. This is the proven committed pattern and is unchanged.
# - `RAW_PAGED_ENDPOINTS` - the eight objects added by this amendment. Cursor
#   paging, and bounded chunking plus a page cap where a Mews time filter is used.
# 
# **`request_contract` on each paged object:**
# 
# - `VERIFIED_IN_PROJECT` - the required request shape is recorded as a verified
#   Mews API fact in `Menja_BI_v1_AI_Working_Context.md`.
# - `UNVERIFIED` - the body below is the **minimum** request this notebook can
#   send without inventing a filter. It has not been checked against
#   docs.mews.com inside this repo. If Mews rejects it, Section 4 now prints the
#   Mews error message. Read that message and set the body here explicitly.
#   Do not guess.
# 
# The scope guard at the end of this cell stops the run before any API call if
# the configured set is not exactly the eleven `ActiveForBIv1 = True` objects.


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
RES_SERVICE_IDS_I215 = [
    "bd26d8db-86da-4f96-9efc-e5a4654a4a94",   # already pulled
    "f38cac87-196b-4a5a-9c45-b046006ba01b",   # 2079 order items
    "66867ec0-62dc-4937-b04b-b37100ab60c1",   # 537 order items
    "a04a7571-7225-4bf2-8ddc-b36e016074b6",   # 175 order items
    "bcc5ce3a-0b73-4746-802d-b30100b05bf0",   # 129 order items
    "a804d717-8cf7-47a3-9535-af6a009d869c",   # 127 order items
    "bd9aedba-3ad5-4367-bb13-b1d900c33e77",   # 90 order items
    "d9b3720a-faba-4292-95fe-b2e900cbe332",   # 76 order items
    "7d35e0b2-9739-411e-9078-b3b7013dc9a3",   # 69 order items
    "539e0194-3e0f-4457-b480-b1e900ba96c6",   # 66 order items
    "da46c24a-e867-4b70-aa43-b28a000547c8",   # 32 order items
    "0503c3bd-11ae-497a-a97d-b19600a43124",   # 32 order items
    "a6aee71c-40e4-4f88-9e33-b18c013858d6",   # 30 order items
    "12b0e869-d697-4f59-8fe0-b38b00ea83fd",   # 29 order items
    "9c4f488b-367c-423d-916c-af8e00e4e888",   # 28 order items
    "63c61063-4a47-4566-825c-b2bf00b905af",   # 19 order items
    "ec9d261c-1ef1-4a6e-8565-ad7200d77411",   # 17 order items
    "b88188db-ba23-4bbb-bdfd-b14f00a637f4",   # 12 order items
    "6dc3e202-0192-43c4-bf6a-b191010fdf18",   # 8 order items
    "a47dce07-dfc0-42ac-9db1-afbd00964cc0",   # 7 order items
    "c5432c47-ae24-4e69-a779-b05100b8284c",   # 6 order items
    "15c4328d-d488-422b-8ed0-b046008bedc3",   # 4 order items
    "4ffcafa0-fe14-46e6-ad12-b24a00d80cd8",   # 2 order items
    "c22a16ba-f017-4b93-be76-b15d00c4fe61",   # 2 order items
    "35b42568-b7f3-4fb4-bca8-b06e007fa9d8",   # 2 order items
    "58332a85-89a1-4fb7-b896-b0a00115744d",   # 1 order items
    "8b1fe3bb-d69f-4ac6-9fbf-b05100b75adb",   # 1 order items
    "c66ddb5d-73cd-433d-b83d-b1d100cfeea6",   # 1 order items
]

# --- Bounded date window for reservations (D-153) ---
WINDOW_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_END_UTC   = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

# --- Chunking and page caps for reservations (D-153) ---
RES_CHUNK_DAYS = 7
RES_MAX_PAGES_PER_CHUNK = 20
RES_PAGE_SIZE = 1000

# --- Network safety ---
TIMEOUT_SEC = 60
RETRIES = 3
RETRY_SLEEP_SEC = 3

# =====================================================================
# Amendment: the remaining eight governed ActiveForBIv1 raw objects
# =====================================================================

# Verified Mews API fact: every Mews time-interval filter (UpdatedUtc,
# CreatedUtc, ConsumedUtc, ClosedUtc, CanceledUtc) caps at 3 months per call.
# The chunk sizes below stay far inside that; this constant makes the limit
# checkable instead of implicit.
MEWS_MAX_INTERVAL_DAYS = 90

# Page caps per object class (D-153). A cap that is hit with more data
# remaining marks the run Partial - it never silently truncates.
REF_MAX_PAGES = 20              # reference objects, no time filter
BLOCK_MAX_PAGES_PER_CHUNK = 5   # block objects, same density class as reservations
ORDER_ITEMS_MAX_PAGES_PER_CHUNK = 20  # order items are dense: a read-only
                                      # inspection on 2026-08-01 saw 7,615 items

# orderItems is filtered on UpdatedUtc, which is RECORD-CHANGE time, not stay
# time. It does NOT correspond to the reservations CollidingUtc stay window,
# and no alignment between the two is implied or asserted here. Widening or
# realigning this window is a deliberate user decision.
ORDER_ITEMS_WINDOW_START_UTC = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
ORDER_ITEMS_WINDOW_END_UTC   = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

RAW_PAGED_ENDPOINTS = [
    {
        "raw_object_id": "RAW_MEWS_AVAILABILITY_BLOCKS",
        "endpoint": "availabilityBlocks/getAll",
        "folder": RAW_ROOT + "/availabilityBlocks/getAll",
        "record_keys": ["AvailabilityBlocks"],
        "static_body": {"ServiceIds": RES_SERVICE_IDS},
        "time_filter": "CollidingUtc",
        "window_start_utc": WINDOW_START_UTC,
        "window_end_utc": WINDOW_END_UTC,
        "chunk_days": RES_CHUNK_DAYS,
        "max_pages_per_chunk": BLOCK_MAX_PAGES_PER_CHUNK,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "ServiceIds + CollidingUtc mirrors the proven reservations call. "
            "09_ObjectDictionary also records Adjustments, ServiceOrders, and "
            "Rates response roots for this endpoint, which suggests an Extent "
            "block exists. No Extent is sent because its exact field names are "
            "not verified in this repo."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_BUSINESS_SEGMENTS",
        "endpoint": "businessSegments/getAll",
        "folder": RAW_ROOT + "/businessSegments/getAll",
        "record_keys": ["BusinessSegments"],
        "static_body": {},
        "time_filter": None,
        "window_start_utc": None,
        "window_end_utc": None,
        "chunk_days": None,
        "max_pages_per_chunk": REF_MAX_PAGES,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "Sent with paging only. No filter is asserted. "
            "09_ObjectDictionary records a Cursor root for this endpoint."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_COMPANIES",
        "endpoint": "companies/getAll",
        "folder": RAW_ROOT + "/companies/getAll",
        "record_keys": ["Companies"],
        "static_body": {},
        "time_filter": None,
        "window_start_utc": None,
        "window_end_utc": None,
        "chunk_days": None,
        "max_pages_per_chunk": REF_MAX_PAGES,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": "Sent with paging only. No filter is asserted.",
    },
    {
        "raw_object_id": "RAW_MEWS_ORDER_ITEMS",
        "endpoint": "orderItems/getAll",
        "folder": RAW_ROOT + "/orderItems/getAll",
        "record_keys": ["OrderItems"],
        "static_body": {},
        "time_filter": "UpdatedUtc",
        "window_start_utc": ORDER_ITEMS_WINDOW_START_UTC,
        "window_end_utc": ORDER_ITEMS_WINDOW_END_UTC,
        "chunk_days": RES_CHUNK_DAYS,
        "max_pages_per_chunk": ORDER_ITEMS_MAX_PAGES_PER_CHUNK,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "VERIFIED_IN_PROJECT",
        "contract_note": (
            "Mews requires at least one of OrderItemIds, AccountIds, "
            "ServiceOrderIds, ServiceIds, BillIds, CreatedUtc, UpdatedUtc, "
            "ConsumedUtc, CanceledUtc, ClosedUtc. UpdatedUtc alone satisfies "
            "that. ServiceIds is deliberately NOT added: whether the Mews "
            "filter can drop items that still carry a payload link is the open "
            "echo-control question in I-215."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_RATES",
        "endpoint": "rates/getAll",
        "folder": RAW_ROOT + "/rates/getAll",
        "record_keys": ["Rates"],
        "static_body": {"ServiceIds": RES_SERVICE_IDS},
        "time_filter": None,
        "window_start_utc": None,
        "window_end_utc": None,
        "chunk_days": None,
        "max_pages_per_chunk": REF_MAX_PAGES,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "Rates are service-scoped in the governed dictionary "
            "(Rates[].ServiceId), so the same RES_SERVICE_IDS scope as "
            "reservations is used. No time filter is asserted."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_RESOURCE_BLOCKS",
        "endpoint": "resourceBlocks/getAll",
        "folder": RAW_ROOT + "/resourceBlocks/getAll",
        "record_keys": ["ResourceBlocks"],
        "static_body": {},
        "time_filter": "CollidingUtc",
        "window_start_utc": WINDOW_START_UTC,
        "window_end_utc": WINDOW_END_UTC,
        "chunk_days": RES_CHUNK_DAYS,
        "max_pages_per_chunk": BLOCK_MAX_PAGES_PER_CHUNK,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "ResourceBlocks carry StartUtc/EndUtc and no ServiceId in the "
            "governed dictionary, so the object is enterprise-scoped and only "
            "a colliding time window is sent."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_RESOURCE_CATEGORIES",
        "endpoint": "resourceCategories/getAll",
        "folder": RAW_ROOT + "/resourceCategories/getAll",
        "record_keys": ["ResourceCategories"],
        "static_body": {},
        "time_filter": None,
        "window_start_utc": None,
        "window_end_utc": None,
        "chunk_days": None,
        "max_pages_per_chunk": REF_MAX_PAGES,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "07_Raw_Source_Objects governs this object as FULL_SNAPSHOT_APPEND "
            "with IncrementalDriver = NONE, so no time filter is sent."
        ),
    },
    {
        "raw_object_id": "RAW_MEWS_RESOURCES",
        "endpoint": "resources/getAll",
        "folder": RAW_ROOT + "/resources/getAll",
        "record_keys": ["Resources"],
        "static_body": {},
        "time_filter": None,
        "window_start_utc": None,
        "window_end_utc": None,
        "chunk_days": None,
        "max_pages_per_chunk": REF_MAX_PAGES,
        "page_size": RES_PAGE_SIZE,
        "request_contract": "UNVERIFIED",
        "contract_note": (
            "Sent with paging only. 09_ObjectDictionary records "
            "ResourceCategories, ResourceCategoryAssignments, "
            "ResourceCategoryImageAssignments, ResourceFeatures, and "
            "ResourceFeatureAssignments response roots, which suggests an "
            "Extent block exists. No Extent is sent because its exact field "
            "names are not verified in this repo."
        ),
    },
]

# =====================================================================
# Governed scope guard (D-159 + D-201, with D-200 exclusion)
# =====================================================================
# The eleven ActiveForBIv1 = True rows in 07_Raw_Source_Objects of
# Menja_Schema_Governance_0626.xlsx. This is a scope assertion, not model
# logic: it stops the run before any API call if the configured set drifts.

GOVERNED_ACTIVE_RAW_OBJECT_IDS = {
    "RAW_MEWS_AVAILABILITY_BLOCKS",
    "RAW_MEWS_BUSINESS_SEGMENTS",
    "RAW_MEWS_COMPANIES",
    "RAW_MEWS_ORDER_ITEMS",
    "RAW_MEWS_RATES",
    "RAW_MEWS_RESERVATIONS",
    "RAW_MEWS_RESOURCE_BLOCKS",
    "RAW_MEWS_RESOURCE_CATEGORIES",
    "RAW_MEWS_RESOURCES",
    "RAW_MEWS_SERVICES",
    "RAW_MEWS_AGE_CATEGORIES",
}

# Endpoints that must NOT be landed by this notebook.
NOT_ACTIVE_ENDPOINTS = {
    "reservationGroups/getAll",  # ActiveForBIv1 = Review (D-200, open I-193)
    "bills/getAll",              # parked in 09_ObjectDictionary only (D-159)
    "configuration/get",         # parked in 09_ObjectDictionary only (D-159)
}

# Endpoint strings this run is expected to key its results on.
GOVERNED_ACTIVE_ENDPOINTS = (
    {RESERVATIONS_ENDPOINT}
    | {e["endpoint"] for e in RAW_SIMPLE_ENDPOINTS}
    | {e["endpoint"] for e in RAW_PAGED_ENDPOINTS}
)

_configured_object_ids = (
    {"RAW_MEWS_RESERVATIONS"}
    | {e["raw_object_id"] for e in RAW_SIMPLE_ENDPOINTS}
    | {e["raw_object_id"] for e in RAW_PAGED_ENDPOINTS}
)

_scope_problems = []

_missing = sorted(GOVERNED_ACTIVE_RAW_OBJECT_IDS - _configured_object_ids)
if _missing:
    _scope_problems.append("governed active objects not configured: " + str(_missing))

_extra = sorted(_configured_object_ids - GOVERNED_ACTIVE_RAW_OBJECT_IDS)
if _extra:
    _scope_problems.append("configured objects that are not governed active: " + str(_extra))

_blocked = sorted(GOVERNED_ACTIVE_ENDPOINTS & NOT_ACTIVE_ENDPOINTS)
if _blocked:
    _scope_problems.append("endpoints that must not be landed here: " + str(_blocked))

for _e in RAW_PAGED_ENDPOINTS:
    if _e["time_filter"] and _e["chunk_days"] > MEWS_MAX_INTERVAL_DAYS:
        _scope_problems.append(
            _e["endpoint"] + " chunk_days exceeds the Mews "
            + str(MEWS_MAX_INTERVAL_DAYS) + "-day interval cap"
        )

if _scope_problems:
    raise RuntimeError(
        "Governed raw scope check failed (D-159 / D-200 / D-201). "
        "Nothing was requested from Mews. Problems: " + " | ".join(_scope_problems)
    )

print("Config loaded.")
print("Raw root:", RAW_ROOT)
print("Reservations endpoint:", RESERVATIONS_ENDPOINT)
print("Reservations window:", WINDOW_START_UTC.date(), "to", WINDOW_END_UTC.date())
print("Simple endpoints (one POST, no paging):")
for e in RAW_SIMPLE_ENDPOINTS:
    print(" -", e["raw_object_id"], "|", e["endpoint"])
print("Paged endpoints:")
for e in RAW_PAGED_ENDPOINTS:
    _scope = e["time_filter"] if e["time_filter"] else "no time filter"
    print(
        " -", e["raw_object_id"], "|", e["endpoint"],
        "|", _scope, "|", e["request_contract"]
    )
print("")
print("Governed active raw objects configured:", len(_configured_object_ids), "of 11")
print("Excluded by governance:", sorted(NOT_ACTIVE_ENDPOINTS))


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
# - `mews_post` treats a 4xx other than 429 as a request-shape or auth problem:
#   it does not retry, and it surfaces the Mews explanation instead of a bare
#   `400 Client Error`. A Mews error body carries a message, not source records.
#   The request body - which holds the tokens - is never printed.
# - `daterange_chunks` is defined here so Section 6B does not depend on Section 5
#   having been run first. Section 5 redefines it identically and is unchanged.
# 
# **Schema note (open governance gap):** these schemas are the ORIGINAL D-186
# field set. The 2026-07-25 D-186 revision additionally requires `PropertyKey`,
# `SourceType`, `SourcePropertyCode`, `MewsScopeType`, and `MewsScopeIds` on both
# tables. That change needs governed property/source resolution (D-215, D-223,
# D-228) and is deliberately not made here.


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

class MewsRequestRejected(Exception):
    """Mews returned a 4xx that retrying cannot fix (request shape or auth)."""


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


def daterange_chunks(start, end, chunk_days):
    # Bounded window splitter (D-153). Defined here so Section 6B does not
    # depend on Section 5 having been run first. Section 5 redefines it
    # identically and is unchanged.
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=chunk_days), end)
        yield cur, chunk_end
        cur = chunk_end


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

            # A 4xx other than 429 is a request-shape or auth problem.
            # Retrying cannot fix it, and Mews explains the problem in the
            # response body. Surface that message instead of a bare
            # "400 Client Error". A Mews error body carries a message, not
            # source records. The request body - which holds the tokens -
            # is never printed.
            if 400 <= resp.status_code < 500:
                detail = (resp.text or "")[:500]
                raise MewsRequestRejected(
                    "Mews rejected the request for " + endpoint
                    + " with HTTP " + str(resp.status_code)
                    + ". Mews message: " + detail
                )

            resp.raise_for_status()
            return resp.json()

        except MewsRequestRejected:
            raise

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
                "ServiceIds": RES_SERVICE_IDS_I215,
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

# ## Section 6B - Extract the remaining eight governed active raw objects
# 
# Plain words:
# - This section lands the eight `ActiveForBIv1 = True` objects that the earlier
#   version of this notebook did not cover: availabilityBlocks, businessSegments,
#   companies, orderItems, rates, resourceBlocks, resourceCategories, resources.
# - Every object is paged with a `Limitation` cursor, matching
#   `LoadMethod = API_PAGED` in `07_Raw_Source_Objects`.
# - Objects with a Mews time filter are split into bounded chunks with a page cap
#   per chunk (D-153). Objects with no time filter are pulled as one full paged
#   sweep, also under a page cap.
# - Hitting a page cap with more data remaining marks the run `Partial`. Nothing
#   is silently truncated and nothing is guessed.
# - Each object gets its own `RunID`, so D-186 stays at one endpoint and one
#   interval per run.
# - If Mews rejects a request, the failure is caught per object: that object is
#   logged `Failed` with the Mews message, and the remaining objects still run.
#   Section 7 then reports FAIL for it.
# 
# **Read the `request_contract` line printed for each object.** `UNVERIFIED` means
# the request body is the minimum this notebook can send without inventing a
# filter, and has not been checked against docs.mews.com inside this repo.


# CELL ********************

# Section 6B - Remaining governed active raw objects
# (raw only, D-148/149/151/153/159/186)
# No I-layer logic. No joins. No mappings. No business logic.

for endpoint_config in RAW_PAGED_ENDPOINTS:
    raw_object_id = endpoint_config["raw_object_id"]
    endpoint = endpoint_config["endpoint"]
    folder = endpoint_config["folder"]
    record_keys = endpoint_config["record_keys"]
    static_body = endpoint_config["static_body"]
    time_filter = endpoint_config["time_filter"]
    window_start = endpoint_config["window_start_utc"]
    window_end = endpoint_config["window_end_utc"]
    chunk_days = endpoint_config["chunk_days"]
    max_pages = endpoint_config["max_pages_per_chunk"]
    page_size = endpoint_config["page_size"]

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

    print("")
    print("=======================================================")
    print("Raw landing: ", raw_object_id)
    print("Endpoint:    ", endpoint)
    print("Folder:      ", folder)
    if time_filter:
        print("Filter:      ", time_filter)
        print("Window:      ", window_start.date(), "to", window_end.date())
        print("Chunking:    ", chunk_days, "days | page cap", max_pages, "per chunk")
    else:
        print("Filter:       none - one full paged sweep")
        print("Page cap:    ", max_pages)
    print("Contract:    ", endpoint_config["request_contract"])
    print("RunID:       ", run_id)
    print("=======================================================")
    if endpoint_config["request_contract"] != "VERIFIED_IN_PROJECT":
        print("NOTE: request shape not verified against docs.mews.com in this repo.")
    print("Note:", endpoint_config["contract_note"])

    try:
        if time_filter:
            chunks = list(daterange_chunks(window_start, window_end, chunk_days))
        else:
            chunks = [(None, None)]

        for chunk_start, chunk_end in chunks:
            if time_filter:
                print("")
                print("Chunk:", chunk_start.date(), "to", chunk_end.date())

            cursor = None
            page_index = 0

            while page_index < max_pages:
                extra_body = dict(static_body)
                if time_filter:
                    extra_body[time_filter] = {
                        "StartUtc": fmt_utc(chunk_start),
                        "EndUtc": fmt_utc(chunk_end),
                    }

                payload = mews_post(
                    endpoint, extra_body, cursor=cursor, page_size=page_size
                )
                page_len = count_records_best_effort(payload, record_keys)
                if page_len == 0:
                    print("  No more records.")
                    break

                page_index += 1
                pages_written += 1
                record_count += page_len

                name_parts = [endpoint.replace("/", "_"), run_ts, run_stamp]
                if time_filter:
                    name_parts.append(chunk_start.strftime("%Y%m%d"))
                    name_parts.append(chunk_end.strftime("%Y%m%d"))
                name_parts.append("page_" + str(page_index).zfill(3))
                file_name = "_".join(name_parts) + ".json"

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
                    "RecordCount": page_len,
                    "WrittenUtc": utc_now()
                })
                print("  Page", page_index, ":", page_len, "records ->", file_name)

                cursor = payload.get("Cursor") if isinstance(payload, dict) else None
                if not cursor:
                    break

            if page_index >= max_pages and cursor:
                hit_cap_with_more = True
                print("  Page cap reached (", max_pages, ") with more data remaining.")

        if hit_cap_with_more:
            status = "Partial"

    except Exception as ex:
        status = "Failed"
        error_message = str(ex)
        print("")
        print("LANDING FAILED:", endpoint)
        print("Error:", error_message)
        print(traceback.format_exc())

    finally:
        run_end_utc = utc_now()
        append_run_log({
            "RunID": run_id,
            "PMS": PMS_NAME,
            "Endpoint": endpoint,
            "WindowStartUtc": window_start,
            "WindowEndUtc": window_end,
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
        if status == "Partial":
            print("NOTE: Partial run. Raise the page cap or narrow the window, then re-run.")
        if status == "Success" and record_count == 0:
            print("NOTE: zero records returned. Confirm the request shape before")
            print("      treating this object as landed.")


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
# 5. All eleven governed `ActiveForBIv1 = True` objects were landed in this
#    session, and nothing outside that governed set was landed.
# 
# Each endpoint gets a PASS / CHECK / FAIL verdict.
# - PASS  = safe to move on.
# - CHECK = landed, but with a note (for example a Partial run) - read the note.
# - FAIL  = do not run the build notebook until this is fixed.
# 
# An object that returns zero records is reported as a note, not a failure: zero
# can be a true source state. Confirm the request shape before relying on it.


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

# --- Governed scope completeness for this run (D-159 + D-201) ---
print("")
print("=======================================================")
print("GOVERNED SCOPE COMPLETENESS")
print("=======================================================")

landed_endpoints = set(landing_results.keys())
missing_from_run = sorted(GOVERNED_ACTIVE_ENDPOINTS - landed_endpoints)
unexpected_in_run = sorted(landed_endpoints - GOVERNED_ACTIVE_ENDPOINTS)
zero_record_objects = sorted(
    ep for ep, r in landing_results.items() if r["record_count"] == 0
)

print("Governed active raw objects expected:", len(GOVERNED_ACTIVE_ENDPOINTS))
print("Objects landed in this session:      ", len(landed_endpoints))

if missing_from_run:
    overall_ok = False
    print("MISSING - governed active objects not landed in this session:")
    for ep in missing_from_run:
        print(" -", ep)

if unexpected_in_run:
    overall_ok = False
    print("UNEXPECTED - landed but not in the governed active set:")
    for ep in unexpected_in_run:
        print(" -", ep)

if zero_record_objects:
    print("NOTE - landed with zero records. Confirm the request shape")
    print("       before relying on these:")
    for ep in zero_record_objects:
        print(" -", ep)

if not missing_from_run and not unexpected_in_run:
    print("Scope matches the eleven ActiveForBIv1 objects in")
    print("07_Raw_Source_Objects.")

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
# If Section 7 shows PASS for all eleven governed active raw objects, and the
# scope-completeness block reports no missing and no unexpected objects:
# 
# 1. Confirm the results yourself (files under `Files/Raw/Mews/...`, log rows above).
# 2. Commit this notebook to GitHub from workspace Source control,
#    and confirm GitHub actually updated.
# 3. Continue with `NB_Menja_Phase1_10_I_Reservations_BUILD_DRAFT`.
# 
# If any of the eight objects added by the amendment failed with a Mews 4xx, read
# the Mews message printed by Section 6B, verify the endpoint's request contract
# against docs.mews.com, then correct that object's entry in `RAW_PAGED_ENDPOINTS`
# in Section 3. Do not guess a filter.
# 
# This notebook never deletes or moves files. If Section 1 warned about a
# lowercase `Files/raw` folder, decide manually what to do with it.
# 
# **Pause Fabric capacity `fabaurorabiv1devf2` in Azure if you are done working,
# to avoid unnecessary cost.**


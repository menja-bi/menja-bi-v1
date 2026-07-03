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

# # Menja BI v1 — Phase-1 Mews I_Reservations Notebook Plan
# 
# This notebook is a planning skeleton for the governed Phase-1 Fabric implementation slice for Mews I_Reservations.
# 
# It does not implement production transformation code yet.
# 
# Governance rules:
# - Menja_Schema_Governance_0626.xlsx is authority.
# - Only FINAL workbook decisions govern.
# - Generated Markdown files are helper context only.
# - D-203 authorizes the Phase-1 Fabric build slice for Mews I_Reservations.
# - Do not invent business logic.
# - If anything is not governed, keep it NULL and classify it as a governance gap.

# MARKDOWN ********************

# ---
# 
# # 0. Governance guardrails
# 
# This notebook must follow the Menja governance workbook.
# 
# Rules:
# - Only FINAL workbook decisions govern.
# - This notebook may only populate columns allowed by D-203 and current governed field bindings.
# - All UNGOVERNED or UPSTREAM_DEPENDENT columns must stay NULL.
# - Do not invent joins, mappings, fallback rules, revenue logic, or group logic.
# - Do not use PropertyID as the Mews EnterpriseId lookup target.
# - Use D_Property.PMS_PropertyCode for the Mews EnterpriseId lookup.
# - PropertyKey is the Menja internal relationship key.

# MARKDOWN ********************


# MARKDOWN ********************

# ---
# 
# # 1. Lakehouse attachment check
# 
# This notebook should be attached to the DEV Lakehouse:
# 
# `LH_Menja_BI_v1_Mews_DEV`
# 
# The Lakehouse is where Fabric stores files and tables for this notebook.
# 
# Before any code is added later, confirm:
# - The Lakehouse is visible in the left Explorer pane.
# - The Lakehouse is pinned as the default.
# - This is the DEV Lakehouse, not PROD.
# 
# No data should be written yet.
# 
# ---
# 
# # 2. Runtime parameters
# 
# This section will later hold notebook settings.
# 
# Planned parameters:
# - Environment: DEV
# - PMS source: Mews
# - Target table: I_Reservations
# - Raw reservations input location
# - Raw services input location
# - Raw age categories input location
# - Seed input location
# - Data-quality output location
# - RunID
# 
# No secrets should be stored or printed here.
# 
# Secrets must stay in Key Vault.
# 
# ---
# 
# # 3. Required raw inputs
# 
# This Phase-1 notebook requires these raw Mews inputs:
# 
# 1. reservations/getAll
# 2. services/getAll
# 3. ageCategories/getAll
# 
# Why they are needed:
# 
# - reservations/getAll is the main reservation source.
# - services/getAll is needed for property lookup.
# - ageCategories/getAll is needed for Adults and Children classification.
# 
# Do not load rate, segment, channel, customer, company, room-type, revenue, block, or group source objects for Phase-1 population unless later FINAL governance explicitly requires them.
# 
# ---
# 
# # 4. Required seed inputs under D-198
# 
# This notebook should load only these required seed inputs:
# 
# 1. D_Property
# 2. D_ReservationStatus
# 
# D_Property is needed for:
# - matching Mews Services[].EnterpriseId to D_Property.PMS_PropertyCode
# - returning PropertyKey
# - reading TimeZone for ArrivalDate and DepartureDate conversion
# 
# D_ReservationStatus is needed for:
# - mapping PMSStatusCode to ReservationStatusKey
# 
# D-198 rule:
# - Only explicitly allowed seed sheets may be loaded.
# - Sheets starting with "_" must not be imported as seed data.
# - _README is documentation only.
# 
# ---
# 
# # 5. Governance allow-list check
# 
# Before creating the output table, the notebook must know which columns are allowed to be populated.
# 
# Allowed:
# - Columns with Mews I_Reservations binding status GOVERNED_FINAL
# - Columns explicitly allowed by D-203 and supported by FINAL governance for implementation
# 
# Blocked:
# - UNGOVERNED columns
# - UPSTREAM_DEPENDENT columns
# - Any column whose logic is not FINAL-governed
# 
# Blocked columns must be written as NULL.
# 
# A raw Mews field existing in source data is not enough. It must be governed before it can populate an I_Reservations column.
# 
# ---
# 
# # 6. Load and shape reservations
# 
# This section will later read raw reservations/getAll data.
# 
# Required reservation source fields include:
# - Reservations[].Id
# - Reservations[].Number
# - Reservations[].ServiceId
# - Reservations[].State
# - Reservations[].CreatedUtc
# - Reservations[].UpdatedUtc
# - Reservations[].CancelledUtc
# - Reservations[].ActualStartUtc
# - Reservations[].ActualEndUtc
# - Reservations[].ScheduledStartUtc
# - Reservations[].ScheduledEndUtc
# - Reservations[].PersonCounts[]
# 
# This section should only prepare fields needed for governed Phase-1 columns.
# 
# Do not derive rate, segment, channel, customer, room type, revenue, block, or group logic here.
# 
# ---
# 
# # 7. Load and shape services
# 
# This section will later read raw services/getAll data.
# 
# Required service source fields:
# - Services[].Id
# - Services[].EnterpriseId
# 
# Governed property lookup path:
# 
# Reservations[].ServiceId
# -> Services[].Id
# -> Services[].EnterpriseId
# -> D_Property.PMS_PropertyCode
# -> D_Property.PropertyKey
# 
# Important:
# Do not use PropertyID as the Mews EnterpriseId lookup target.
# 
# ---
# 
# # 8. Load and shape age categories
# 
# This section will later read raw ageCategories/getAll data.
# 
# Required age category fields:
# - AgeCategories[].Id
# - AgeCategories[].Classification
# 
# Governance rule:
# - Do not hardcode AgeCategoryId values.
# - Do not guess Adult or Child from names, labels, order, or sample data.
# - Classification must come from Mews ageCategories/getAll.
# 
# ---
# 
# # 9. Property resolution
# 
# This section resolves each reservation to a Menja property.
# 
# Governed logic:
# 
# 1. Take Reservations[].ServiceId.
# 2. Match it to Services[].Id.
# 3. Read Services[].EnterpriseId.
# 4. Match Services[].EnterpriseId to D_Property.PMS_PropertyCode.
# 5. Return D_Property.PropertyKey.
# 6. Carry D_Property.TimeZone for stay-date conversion.
# 
# Data-quality checks:
# - Missing Reservations[].ServiceId
# - No matching Services[].Id
# - Missing Services[].EnterpriseId
# - No D_Property.PMS_PropertyCode match
# - Duplicate or ambiguous D_Property match
# - Missing D_Property.TimeZone
# 
# Do not guess a property.
# 
# ---
# 
# # 10. Reservation status mapping
# 
# This section maps Mews reservation status to the governed Menja status.
# 
# Governed logic:
# 
# 1. Copy Reservations[].State exactly into PMSStatusCode.
# 2. Use PMSStatusCode to look up ReservationStatusKey in D_ReservationStatus.
# 3. Use governed UNKNOWN handling only if allowed by D-190.
# 
# Data-quality checks:
# - Missing Reservations[].State
# - Status not found in D_ReservationStatus
# - UNKNOWN status mapping used
# 
# Do not write the raw Mews state directly as ReservationStatusKey.
# 
# ---
# 
# # 11. SnapshotDateTime and duplicate snapshot checks
# 
# SnapshotDateTime identifies the source-state version of a reservation.
# 
# Governed logic:
# 
# SnapshotDateTime = Reservations[].UpdatedUtc
# 
# Data-quality checks:
# - Missing Reservations[].UpdatedUtc
# - Duplicate ReservationID + SnapshotDateTime
# - Same ReservationID + SnapshotDateTime with conflicting governed state
# 
# Do not use notebook run time or extraction time as SnapshotDateTime.
# 
# Do not invent a tie-break timestamp.
# 
# ---
# 
# # 12. StatusDateTime
# 
# StatusDateTime describes the timestamp for the reservation's current status on this snapshot row.
# 
# Governed logic from D-191:
# 
# - Canceled = Reservations[].CancelledUtc
# - Started = Reservations[].ActualStartUtc
# - Processed = Reservations[].ActualEndUtc
# - Confirmed and Optional = first-observed SnapshotDateTime in the stored version stream
# 
# This is not a separate lifecycle-event table.
# 
# Do not bind StatusDateTime to UpdatedUtc as a general shortcut.
# 
# ---
# 
# # 13. BookingDateTime
# 
# BookingDateTime is the governed booking timestamp proxy for Mews.
# 
# Governed logic:
# 
# BookingDateTime = Reservations[].CreatedUtc
# 
# Data-quality checks:
# - Missing CreatedUtc
# - Invalid CreatedUtc datetime format
# 
# ---
# 
# # 14. ArrivalDate and DepartureDate
# 
# ArrivalDate and DepartureDate are scheduled stay-boundary dates.
# 
# Governed logic:
# 
# ArrivalDate:
# 1. Take Reservations[].ScheduledStartUtc.
# 2. Convert it to the resolved D_Property.TimeZone.
# 3. Take the local date part.
# 
# DepartureDate:
# 1. Take Reservations[].ScheduledEndUtc.
# 2. Convert it to the resolved D_Property.TimeZone.
# 3. Take the local date part.
# 
# Only populate these fields if:
# - property lookup resolves
# - D_Property.TimeZone exists
# - scheduled timestamp exists
# 
# If not, keep the field NULL and log a data-quality issue.
# 
# Data-quality checks:
# - Missing ScheduledStartUtc
# - Missing ScheduledEndUtc
# - Missing property
# - Missing TimeZone
# - Invalid timezone conversion
# - ArrivalDate after DepartureDate
# 
# ---
# 
# # 15. Adults and Children
# 
# Adults and Children are derived from Mews person counts and age category classification.
# 
# Governed logic:
# 
# 1. Explode Reservations[].PersonCounts[].
# 2. Join PersonCounts[].AgeCategoryId to AgeCategories[].Id.
# 3. Sum Count where AgeCategories[].Classification = Adult into Adults.
# 4. Sum Count where AgeCategories[].Classification = Child into Children.
# 
# Data-quality checks:
# - Missing PersonCounts
# - Missing AgeCategoryId
# - AgeCategoryId does not resolve
# - Duplicate age category match
# - Blank classification
# - Unsupported classification
# 
# Do not treat unresolved age categories as zero.
# 
# Do not hardcode Mews AgeCategoryId values.
# 
# ---
# 
# # 16. BookedRooms
# 
# BookedRooms is governed as a constant at canonical reservation-unit grain.
# 
# Governed logic:
# 
# BookedRooms = 1
# 
# This means one I_Reservations row represents one booked room/space reservation unit per snapshot.
# 
# Do not use Mews ReservationGroupId to calculate BookedRooms.
# 
# Do not interpret this as "one guest booking always equals one room."
# 
# ---
# 
# # 17. IsGroupReservation
# 
# Current Phase-1 logic uses a conservative fallback.
# 
# Governed logic:
# 
# IsGroupReservation = FALSE
# 
# This is temporary and safe for Phase-1.
# 
# Do not implement TRUE logic from:
# - non-blank GroupId
# - reservation group name
# - channel manager fields
# - block fields
# - company/account linkage
# - segment
# - rate plan
# - row-count heuristics
# 
# Positive group logic requires a later FINAL governance decision.
# 
# ---
# 
# # 18. Forced NULL columns
# 
# The following logic areas must remain NULL in Phase-1:
# 
# - Rate-plan lookup
# - Segment lookup
# - Channel mapping
# - Account/customer lookup
# - Room-type lookup
# - Revenue fields
# - Revenue normalization
# - RevenueStreamKey
# - AppliedCommissionPct
# - MarketCountryKey derivation
# - Block pickup flags
# - Positive IsGroupReservation TRUE logic
# 
# No placeholder keys.
# No guessed fallback values.
# No invented UNKNOWN values unless that exact column has FINAL governance allowing it.
# 
# ---
# 
# # 19. Build final I_Reservations output shape
# 
# This section will later assemble the final I_Reservations table.
# 
# The output should include:
# - governed populated columns
# - forced NULL columns
# - no extra business columns
# - no ungoverned mappings
# 
# Target grain:
# 
# ReservationID + SnapshotDateTime
# 
# Before writing, the notebook must verify that this grain is unique.
# 
# ---
# 
# # 20. Data-quality output
# 
# The notebook should produce data-quality output for unresolved or unsafe rows.
# 
# Required DQ categories:
# - Missing ServiceId
# - Missing matching Services[].Id
# - Missing Services[].EnterpriseId
# - Missing D_Property.PMS_PropertyCode match
# - Duplicate or ambiguous property match
# - Missing D_Property.TimeZone
# - Unknown reservation status
# - Unresolved age category classification
# - Duplicate ReservationID + SnapshotDateTime
# - Missing required source timestamps
# 
# Suggested DQ fields:
# - DQCategory
# - DQSeverity
# - ReservationID
# - PMSReservationID
# - SourceObject
# - SourceField
# - SourceValue
# - Explanation
# - GoverningDecision
# - RunID
# 
# DQ output must explain issues. It must not silently repair or invent business logic.
# 
# ---
# 
# # 21. Write output table
# 
# This section will later write the governed Phase-1 I_Reservations output.
# 
# Planned outputs:
# - I_Reservations Phase-1 table
# - data-quality output table or file
# 
# Important:
# - Do not write production tables yet.
# - Do not overwrite existing governed tables without explicit user confirmation.
# - Do not claim Fabric was run unless the user runs it and confirms the result.
# 
# ---
# 
# # 22. Notebook summary
# 
# At the end of a run, the notebook should show a simple summary.
# 
# Planned summary:
# - reservation rows read
# - service rows read
# - age category rows read
# - seed rows read
# - I_Reservations rows prepared
# - rows with property issues
# - rows with status issues
# - rows with age-category issues
# - duplicate snapshot issues
# - columns populated
# - columns forced to NULL
# - excluded logic areas
# 
# If data-quality issues exist, the summary must make that clear.
# 
# ---
# 
# # 23. Out-of-scope reminder
# 
# This notebook must not implement:
# 
# - rate-plan lookup
# - segment lookup
# - channel mapping
# - account/customer lookup
# - room-type lookup
# - revenue population
# - revenue normalization
# - RevenueStreamKey
# - AppliedCommissionPct
# - MarketCountryKey
# - block pickup flags
# - positive IsGroupReservation TRUE logic
# - forecasting
# - optimization
# - RMS logic
# - AI automation logic
# - any fallback or mapping not governed by FINAL workbook decisions
# 
# If any of these are needed, classify it as:
# 
# Governance gap — implementation must not continue for that part.
# 
# ---
# 
# # 24. Final notebook status
# 
# This notebook skeleton defines the governed Phase-1 Mews I_Reservations implementation structure.
# 
# It has not implemented production transformation code yet.
# 
# It has not run Fabric.
# 
# It has not committed anything to GitHub.
# 
# Next step after this skeleton is reviewed:
# Create a separate BUILD_DRAFT notebook using the same section order, with Markdown and code cells arranged section by section.


# CELL ********************

# Section 1 — Lakehouse attachment check
# Purpose:
# Confirm that this notebook is attached to the correct DEV Lakehouse:
# LH_Menja_BI_v1_Mews_DEV
#
# No transformation logic here yet.
# No tables should be written from this cell.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Planning placeholder — Governance allow-list check
# Purpose:
# Later, the production build notebook must check which I_Reservations columns
# are allowed to be populated.
#
# Allowed:
# - GOVERNED_FINAL field bindings
# - anything explicitly allowed by D-203
#
# Blocked:
# - UNGOVERNED
# - UPSTREAM_DEPENDENT
#
# Blocked columns must stay NULL.
#
# No code implemented here yet.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Planning placeholder — Data-quality checks
# Purpose:
# Later, the production build notebook must create data-quality checks for:
# - missing ServiceId
# - missing Services[].Id match
# - missing Services[].EnterpriseId
# - missing D_Property.PMS_PropertyCode match
# - missing D_Property.TimeZone
# - unknown reservation status
# - unresolved age category classification
# - duplicate ReservationID + SnapshotDateTime
#
# No code implemented here yet.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

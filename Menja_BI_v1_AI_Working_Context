# Menja BI v1 — AI & Fabric Working Context

**Single working reference for Menja BI v1.** Update by editing this file in the repo and committing.
Not a governance authority. The governance workbook and FINAL decisions remain authoritative.

Last updated: 2026-06-19

---

## 0. How context is organised

Three live files only:

| File | Home | Role |
|---|---|---|
| `Menja_Schema_Governance_0626.xlsx` | OneDrive `00_Governance/Compressed_current_state` | **Authority** — single source of truth for the governed model |
| `Menja_Governance_Specification.docx` | OneDrive `00_Governance/Compressed_current_state` | Governance mechanics (sheet structures, row formats, decision rules) |
| `Menja_BI_v1_AI_Working_Context.md` | GitHub repo (this file) | Working context for sessions — read live via raw URL |

Everything else is in the `Archive` subfolder and is historical.

**Reading note:** the Microsoft 365 connector returns only snippets for `.xlsx`. For full governance workbook reads, upload the xlsx to chat — the connector cannot read all cells.

---

## 1. Fabric Environment

| Item | Value |
|---|---|
| Workspace | Menja BI v1 - DEV |
| Lakehouse | LH_Menja_BI_v1_Mews_DEV (attach to notebook before running) |
| Capacity | fabaurorabiv1devf2 (F2 — pause in Azure when not working to save cost) |
| Environment | DEV — no PROD yet |
| Data source | Mews PMS demo |
| Reservations endpoint | reservations/getAll/2023-06-06 (current; old version deprecated 10 Jan 2026) |
| Key Vault | kv-menja-biv1 — secrets: mews-access-token, mews-client-token |
| Secret read | notebookutils.credentials.getSecret(vault_url, name) |

---

## 2. GitHub

| Item | Value |
|---|---|
| Repo | https://github.com/menja-bi/menja-bi-v1 (public — code only, no guest data) |
| Git sync | Connected. Commit from workspace Source control. Confirm GitHub actually updates after each commit. |
| Branch | main only — no PRs needed solo |
| Trigger config | config/reservation_trigger_fields.json (read by transformation notebook) |

---

## 3. Tool Roles

| Tool | Allowed | Must not |
|---|---|---|
| Claude | Review against governance, explain concepts, draft code, flag gaps, read notebook/config via raw URL | Touch Fabric or GitHub directly, invent business logic, infer mappings |
| Copilot | Python syntax only | Transformation logic, joins, mappings, business rules, fallback |
| Fabric | Execute notebooks, store data, refresh | Be treated as model authority |
| GitHub | Version history, rollback, Claude visibility via raw URL | CI/CD (not yet) |
| You | Every decision, commit, and run | Delegate governance decisions to any AI tool |

**Learning rule:** Claude explains every new concept and code block before it is applied. If you do not understand it, do not run it.

---

## 4. Authority Order

1. `Menja_Schema_Governance_0626.xlsx` — governance workbook
2. FINAL decisions in that workbook
3. `Menja_Governance_Specification.docx`
4. Implementation notes / PBIP-TMDL notes
5. PBIP / Fabric / scripts / chat assumptions

Only FINAL decisions govern. OPEN and DRAFT decisions are **not** authoritative.
Sequence for model changes: **Question → Issue → Discussion → Decision → Governance update.**
No decision exists unless logged. No model change is governed unless in the workbook.

---

## 5. Hard Governance Boundaries

Quick reference — full authority is the workbook.

- Do not invent revenue logic or revenue allocation
- Do not infer channel mapping
- Do not infer segment mapping
- Do not infer source lineage
- Do not invent relationships
- Do not choose readable-name selection rules unless governed
- Do not invent language fallback rules
- Do not invent rate-plan grouping
- Do not invent fallback rules of any kind
- No forecasting, optimization, recommendations, automation, or RMS logic in BI v1
- No multi-PMS abstraction beyond keeping PMS + endpoint metadata clean
- If something required for BI logic is missing → classify as governance gap, stop

---

## 6. Current State (2026-06-19)

**DONE and proven:**
- Raw landing pipeline: `NB_Menja_Mews_Reservations_Raw_Landing_DEV` lands raw Mews reservations JSON, with D-186 run/file logging in two Delta tables (ExtractionRunLog, ExtractionFileLog). Two clean runs, no overwriting, committed to GitHub.
- Key Vault secrets working.
- Field discovery complete: 09_ObjectDictionary has 42 assessed reservations fields.

**GOVERNED and ready to build:**
- I_Reservations: grain ReservationID + SnapshotDateTime (D-122), change-aware snapshot versioning (D-143), lineage with 6 source mappings (D-159), 45 columns in 03_Columns.
- Snapshot model: a new version persists only when governed row state changes (D-143) — not on every extraction.

**PROVISIONAL:**
- Change-trigger field set: DRAFT decision D-188 + config at config/reservation_trigger_fields.json. Cannot lock until real-hotel data shows which fields churn.

---

## 7. The Next Wall — Transformation Template Specs (Issue 2)

Transformation templates named in 08_I_Table_Lineage (TPL_MEWS_RESERVATIONS_SNAPSHOT, the lookup templates, TPL_MEWS_RES_TO_ROOMNIGHTS) have **no field-level mapping defined**. This blocks building I_Reservations — a template can't be built from a name without inventing the mapping.

Both ends of the mapping already exist: 09_ObjectDictionary (raw field assessments) and 03_Columns (target columns). The work is connecting them into a spec.

**This is the next substantial session.**

---

## 8. Known Blockers Ahead (tracked in ISSUES sheet — do not re-describe, resolve there)

Relevant to the I_ layer build:
- **I-132** — I_Reservations design may still imply latest-state rows; reconcile with D-143 change-aware versioning
- **I-041** — snapshot grain / as-of interpretation not fully aligned across D-122/D-128/D-143
- **I-143** — I_Reservations lacks IsBlockPickupReservation (governed field missing)

Commercial-attribution blockers (will hit at dimension/fact layer, not raw):
- **I-075 / I-076** — Mews channel source unresolved; channel/segment mapping rules ungoverned
- **I-099** — rate-plan grouping (B_RatePlanGroupLeader) has no governed columns
- **I-102** — revenue normalization (D-138) not implemented; D_RevenueStream missing

Plus the two issues logged this session:
- Trigger-set issue (paired with DRAFT D-188)
- Template-spec gap (Issue 2 above)

---

## 9. Key Risk — Room-Night Multiplier

Every I_Reservations version re-explodes all room-nights in I_RoomNights
(grain: ReservationID × SnapshotDateTime × StayDate × BookedRoomIndex).

So the trigger set controls **room-night history volume**, not just reservation row count. An over-sensitive trigger inflates room-nights, not just reservations. **Keep the trigger set tight.**

Deferred escape hatch: a two-tier trigger model (separate reservation-level vs room-night-relevant trigger sets) — documented in DRAFT D-188, build only if I_RoomNights grows heavy.

---

## 10. Governed Flow (raw → F) for reservations

```
RAW_MEWS_RESERVATIONS (raw JSON — DONE)
   ↓ change-aware snapshot versioning (D-143, D-051)
I_Reservations (ReservationID + SnapshotDateTime) [D-122]
   ↓ explode per night
I_RoomNights (+ StayDate + BookedRoomIndex) [D-124, D-142]
   ↓ filter to inventory-deducting status, pick revenue
F_RoomNights (RoomNightID) [D-125]

I_Reservations also → F_Reservations (snapshot fact) [D-131]
```

Booked vs realized revenue kept separate down the chain (D-124); F_RoomNights applies precedence realized-else-booked (D-125).

---

## 11. Working Pattern

1. Draft approach in Claude with governance context
2. Claude reviews against workbook, flags gaps
3. Write/refine notebook (Copilot for syntax only)
4. Claude final governance check (paste, or give raw GitHub URL)
5. Apply in Fabric manually — you run and confirm
6. Commit to GitHub, confirm it actually updated
7. Update this file's Current State + session notes

---

## 12. Fabric Lessons Learned

- Notebooks run cells in click order, NOT top-to-bottom. After a restart, re-run from top (or Run all).
- Attaching a lakehouse restarts the session and wipes variables. Attach FIRST, then run.
- Lakehouse file path is /lakehouse/default/Files/... and needs the lakehouse attached.
- Spark cannot infer a column type from all-NULL values — give log tables an explicit schema.
- Generate the RunID INSIDE the extraction cell so each run self-stamps. Do not depend on a separate config cell.
- If a variable's builder line goes missing during edits, the cell silently reuses a stale value. Watch this in long cells.
- DROP TABLE on Delta can leave residue; DELETE FROM clears rows reliably.

---

## 13. Governing Decisions (reservations / raw landing)

| ID | Status | What |
|---|---|---|
| D-148 | FINAL | Raw lands as JSON, source-shaped, unchanged |
| D-149 | FINAL | Extractor is raw-only, no modeled tables or business logic |
| D-151 | FINAL | Stable root folder, endpoint subfolder, timestamp in filename |
| D-153 | FINAL | Bounded date windows, chunking, page caps for heavy endpoints |
| D-186 | FINAL | ExtractionRunLog + ExtractionFileLog as Delta tables |
| D-122 | FINAL | I_Reservations grain + columns |
| D-143 | FINAL | Change-aware snapshot versioning + IsLatestCurrent |
| D-051 | FINAL | Reservation version = ReservationID + SnapshotDateTime |
| D-159 | FINAL | I_Reservations lineage (6 source mappings) |
| D-124 | FINAL | I_RoomNights structure |
| D-125 | FINAL | F_RoomNights structure |
| D-188 | DRAFT | Change-trigger field set (provisional, pending real-hotel data) |

---

*Menja BI v1 — AI & Fabric Working Context | Update by editing in repo + commit | Not a governance authority*

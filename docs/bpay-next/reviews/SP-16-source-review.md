# SP-16 certified-absence source review

11 October 2026. Independent verify-only review of approved sequence114, full `05_SOURCE_AND_PROTECTED_PAY.md:548–572`, current working checklist ITEM/D01/D02 (`IMPLEMENTATION_CHECKLIST.csv:1707–1709`;MD3369–3370), current registry and saved investigation. No product/checklist edits, SQL/database execution, tests, fixtures, business/hosted calls, captures or commits. Runtime, joined/application acceptance and timing are **NOT_RUN**.

**SOURCE PASS for the named verify-and-record scope. No confirmed NULL-as-absence caller bug was found; no SQL, Worker, screen or new named error-code change is required.** The card564–566 explicitly retains0645 and reserves later classifier changes for SP17 then SP34. Installed/catalogue/config/ACL proof is separate builder evidence; this receipt is not a whole-item/runtime PASS.

## Current owners and provenance

Paths below are relative to BUILD-BE. The initial investigation's L/Q definition-census and old registry/hash statements describe its pre-SP15 checkpoint; they are not current final-owner claims.

| Alias | Current saved physical source and exact signature |
| --- | --- |
| W | `supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql`; `private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)` at5. |
| K | `supabase/repeatable/05102026_0512_weekly_source_operational_category_v2.sql`; `private.weekly_source_timesheet_category_v2(uuid)` at211. |
| E | `supabase/repeatable/05102026_0650_weekly_source_accepted_removal_query_evidence_v1.sql`; `private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid)` at4. |
| C | `supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql`; current registry `public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)` at979 and `public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)` at1991. |
| L/Q | **Current SP15 successor** `supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql`; `public.weekly_exceptional_pay_complete_local_v1(jsonb)` at2867 and `public.weekly_source_manual_review_resolve_v1(jsonb)` at134. Earlier1257/0428 are retained complete parents, not final definers. |
| A | `supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql`; SCX056 `public.weekly_source_no_shifts_attest_atomic_v1(jsonb)` at5388. |
| Z | `supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql`; current registry NX011 `private.bpay_next_source_zero_scope_v1(uuid,uuid,uuid)` at384. |

The current registry positively selects C/L/Q/Z as above. W/K/E/A are retained Source owners in the saved repeatable census, not new registry `chains` rows. No absent row in that registry is treated as permission to replace a Source owner. W remains STABLE, fixed search_path and owner-only (7–8,450–452); E remains STABLE/owner-only (6–7,109–111). Their witness/domain/signatures/ACL bytes are unchanged.

## D01/D02 exact NULL and closed-union checks

| Required check | Current source evidence | Result |
| --- | --- | --- |
| Two current roster Finals on one date | W383–387 counts CURRENT-pointer CYCLE Finals with exact Source group, Client manifest and event-date coverage; **388 `if v_count>1 then return null`**. W63–75 independently refuses multiple qualified current source rows. Card approximate75 is the row-ambiguity branch;388 is the exact covering-Final branch. | Unknown, never a certificate. Keep original NULL/error behaviour. |
| 0512 caller | K324–325 obtains W;326–333 appends a waiting-import fact only for positively `CERTIFIED_ABSENCE`, exact selected event and `source_present=false`. SQL NULL makes the IF predicate nontrue and adds nothing. K335–337's hint depends on this positive evidence;249–252 adds no Candidate-pay gate. | NULL is unavailable to this factual classifier, never absence. No new named ambiguity code. |
| 0650 caller | E79–82 explicitly raises `WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_UNAVAILABLE` on NULL/proposal/group mismatch. E89–96 requires a real Final CURRENT_ROW matching source revision/presence;97–100 requires CERTIFIED_ABSENCE with false/0;101 refuses every other kind. | Original closed union CURRENT_ROW / CERTIFIED_ABSENCE. |
| Current C1 caller | C1338–1354, only genuine WITHDRAW/RECONCILE:1342 rejects NULL,1343 requires the exact proposal,1344–1351 qualify each union arm,1353 fail-closes missing/unknown kind,1354 raises `WEEKLY_PROTECTED_C1_SELECTED_SOURCE_UNAVAILABLE`. | No null/default-zero path. Current0853 retained, not historical1534 alone. |
| Witness interface | W442–447 retains `WEEKLY_SOURCE_SELECTED_SOURCE_WITNESS_V1` and exactly seven top-level fields: schema_version,scope,kind,basis_kind,source_proposal,basis,basis_sha256; original hash domain unchanged. E102–105 retains its existing eight-field result with this native witness nested. | No signature, witness bytes, certificate field or protocol change. |

## Full card behaviour supplied by retained owners

Removal is action-qualified to the selected native event: W38–59 binds family/Client/Contract/date/group and one selected client source; C1338–1354 and E89–101 accept exact current Source, requiring actual evidence before any zero. C1629–1661 distinguishes ACCEPTED_SOURCE from RECORD_NOT_WORKED; unrelated WAIT siblings are counted separately at1637–1648. C1777–1790 appends the event on **v_work_event_id**, and1823–1828 completes pending targets only for that same event. Thus the named source path supports protected4h→currentSource3h and only the absent selected3h becoming zero beside unrelated10h; those numerical application journeys remain NOT_RUN.

W91–117 refuses damaged/rebuilding/partial authority,185–202 rejects unresolved/null mappings,283–290 refuses compatible/overlapping/missing-clock ambiguity. W315–316 requires positive false/0 proposal before an absence arm. NHSP requires actual full positive/negative physical history and no unmatched current movement (317–357), not omission. Roster cancellation uses a native CANCEL transition and exact prior snapshot (359–378); the coverage-omission certificate requires unique current Final, actual London confirmed coverage, omission-authorising profile/current cycle pointer and exact Client manifest (383–423). NO_IMPORT_YET requires genuine OFFICE_PROTECTED_SHIFT, source_observed=false and no resolution/source-link/snapshot/movement history (426–438). No duration-only, absent-row or provisional-empty inference is added.

For SRQ104, current Q193–205 requires/locks a real OPEN review;207–215 accepts only the actual approval+generation removal-evidence arm, forbids the alternative row-hash arm, and calls E. E22 requires OPEN;26–37 binds native WITHDRAW/RECONCILE/actor/event tuple,38–70 requires completed local receipt/latest ACCEPTED_SOURCE/target event and a post-opening action. Q308–317 changes only review state/audit/immutable command result; its SP15 refresh318 changes Workbench projection through the existing owner.

Current L3248–3254 stores request/run/local completion,3263–3272 records native observation and completion step, then **3281–3285** resolves OFFICE_ACCEPTED_SOURCE only when a real OPEN review was found and action is WITHDRAW/RECONCILE. There is no fabricated review or second browser command. A refusal rolls the same outer transaction back. Other action kinds retain their original separate branches; SP15's direct-refresh condition3292–3294 does not manufacture a removal receipt.

Exact replay retains the original accepted facts/cut-off: C1237–1253 checks retained staged identity/hash before fresh selected-witness qualification; current L2928–2948 checks original receipt/actor/request/key and original action report authority before freshness; current Q169–191 returns exact stored command result before OPEN/currentness and refuses changed bytes. Later imports cannot silently replace the saved accepted proposal. The SP15 complete-parent inverse is recorded separately in `reviews/SP-15-source-hook-review.md`; its only changes are required projection hooks, not witness/removal/pricing semantics.

For SRQ110/RQ224, W/E are factual STABLE readers; Q writes review/audit/command metadata, not existing paid/held ledger facts. The retained C path appends native approval/generation/event evidence and preserves prior vectors/hashes; Z binds native Source-current scope and is not a monetary writer. This verify-only item adds no financial mutation. Actual certified-zero display and unchanged paid/held balances are joined acceptance, NOT_RUN.

SCX056 remains a distinct client-cycle attestation: A5397–5400 closed actor/cycle/group/Client/version/text fields;5450–5460 native cycle lock and FINALISE_WEEK authority;5467–5475 exact existing attestation replay;5483–5539 refuses existing Final/source/unresolved/open issues;5563–5571 stores NO_SHIFTS_TO_IMPORT client-cycle completion. It is not selected-event removal or RECORD_NOT_WORKED, and no new removal/neutral payable/Restore/date-ban action was introduced.

## Final reviewed saved-byte SHA256

These are source hashes, not independent observations of the installed catalogue or deployed application.

| Artifact | Raw SHA256 |
| --- | --- |
| W0645 | `1D455CA1A9088CE591B046E9C4659B5994967A5F901808FAF5AB7FBD1CE04C8D` |
| K0512 | `602DB4EDB834BE6A6B3428B5D47828B62F4C7FF3589738BC36D756B30A1B8700` |
| E0650 | `EB2C87DAB3FDE48A9239750D409AD31894498BB12D0E7E8ADA1047C3A6E35144` |
| C0853 | `ABC874B2D4ED90529AD591EA08A0A5131A6BC32C86109911631C8516E18B2E94` |
| Current L/Q2337 | `7A5B4B56A424B2A07A8A54C895C4841CE6E0C2CBB0E9A80D06C27842FDE5C96A` |
| A1534 | `DACE15A8DF0B37DECF261BEB3CD99AC5C59EA362F22114B382C1C0148D4BF0E4` |
| Z0475 | `BECDEC3D6892A241DEC27995B1C1DBA592DA1372FB043C911DD11C55F94FCCAE` |
| Current registry | `6D804AFB96533069147FE3B4FF3CB2A6B2A50D5157D94C85153D8D2676C66EFA` |
| Read-ahead investigation, historical L/Q provenance explicitly superseded above | `78540195DF26D58DD4EB62B81EB523A42315E6CB4A51D58D74518C781A55F575` |

No essential source finding remains for SP16 D01/D02. Builder must retain fresh installed/config/grant/include receipts and record the exact branches above. No SQL patch, new interface or early SP17/SP34 work is indicated. Whole-item and every later Done-means application journey remain **NOT_RUN** by this reviewer.

# WB-01 rate authority source review

Independent read-only investigation, 10 October 2026. Read WB-01's complete card (`02_WORKBENCH.md:84–118`), its twelve child rows, current ordinary publisher, rate reader, core tables, immutable guards, genuine Daily evidence and actual TF producers. No SQL, tests, gates, application, hosted reads or business execution. This is a source/design assessment, not whole WB-01 acceptance.

## Boundary and existing authority

The master approval chain is WB-01 → WB-36 → WB-02 → DY-04 → DY-06 → DY-10 → DR-38. Keep Rule B and parked F25 unchanged. The missing Workbench projection/refresh schema belongs to WB-36/WB-08; an early fake refresh hook does not close it. WB-01's present supporting change should freeze approved authority and deliver the exact missing-rate refusal in the existing authorise path. Later refresh/tax owners remain explicitly pending.

`private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid)` remains final in `03102026_1430_banking_pay_next_position_writer_v1.sql:7–102`. It checks exact work/revision/physical identity, seals the revision, advances the work pointer, then receives the existing POSITION_APPLY command and creates its member/publication in the same transaction. Do not replace this with a second notification command. The original authorisation trigger (`03102026_1530:608–690`) publishes only a new authorised TF transition under NEXT; unauthorisation preserves old revisions and changes the work's future offer state. Its monetary and withdrawal branches need no rewrite for the rate freeze.

The existing immutable approval policy is already correct: core `1324:62–73` takes a shared lock on policy control and supplies the then-current immutable UUID through the revision default. Policy changes take the same control row for update and copy the effective windows once (`1324:76–109`). Retain `valuation_policy_id`; do not copy current settings into each revision or assert TARGET VAT at approval.

## Exact persisted rate sources

| Authority | Actual retained shape/source | Capture consequence |
|---|---|---|
| Weekly available schedule | `contracts.rates_json` has `paye_*`, `umb_*`, `charge_*`; Contract labels and `additional_rates_json` carry the actual configured codes (`baseline:2123+`; publisher `2018:198–200`). | Freeze all five canonical rows, separately nullable PAYE/Umbrella/charge, plus every actual Additional and mileage identity. |
| Weekly approved values | TF `pay_day…pay_bh`, `charge_day…charge_bh`; actual snapshot writers set `{mode:'CONTRACT_RATES_JSON',contract_id}` (`index.js:83980–83993`, also 171443/171893/172265/176029). | Store exact TF values separately from available Contract values, including unused approved bucket values. Do not fabricate weekly override/default UUIDs absent from that retained shape. |
| Daily approved provenance | `daily-canonical-authority.js:287–291` stores `{kind,override_id,default_id,rate_type}`; current preview also writes `source_kind` (`index.js:177129–177134`). | Recognise `mode`, `kind`, and actual `source_kind` without pretending they are one mandatory shape. Store native UUIDs from actual TF refs, not a new lookup of mutable source rows. |
| Daily available channels | Existing `0018:302–329` resolves PAYE/Umbrella through the actual dated resolver and checks the selected channel exactly against retained TF provenance and every used rate. It already refuses stale authority. | Preserve this check and the actual two-channel availability; do not switch Daily to Contract rates. |
| Additional used values | TF `additional_units_json[code]` stores actual `unit_count`/`units_week`, `pay_rate`/`rate`, `charge_rate`, and stored `pay_ex_vat`/`amount_ex_vat` (`index.js:177386–177395`; publisher `2018:559+`). | Keep each real code and its used quantity/rate. An unmatched used code gets an explicit available-NULL row, not a generic ADDITIONAL label or invented Contract rate. |
| Mileage approved values | Existing publisher resolves pay TF → Contract → Candidate and charge TF → Contract → Client (`2018:183–187`). | Preserve this exact effective order; do not infer a rate by dividing a stored amount. |

`rates_candidate_overrides.id` and `rates_client_defaults.id` are UUID (`baseline:3715–3766`); resolver `05012026_tsfin_resolve_rates_batch.sql:20–42` returns native UUIDs and exact numeric rates. Candidate override supplies pay; Client default supplies charge and channel-specific pay. The actual source is mutable, so a stored ID is provenance, while copied exact approved values are historical authority. A new FK to those mutable rate rows would add no immutable value proof and could unnecessarily prohibit their existing lifecycle.

## Confirmed gaps and minimum corrections

1. Reader `1510` omits all-NULL canonical standard rows and mileage, and only accepts EX1–EX5. Core `1324:262+` also prohibits an all-NULL schedule. Permit actual missing rows, always emit the five canonical standards and MILE, and permit every nonblank normalised genuine Additional code with duplicate-normalised-code refusal. Preserve the actual shared Additional `pay_rate` format rather than inventing separate source keys.
2. Schedule stores available values only; TF selected/default/override values can differ. Add nullable `approved_pay_rate`/`approved_charge_rate` and fill them in the initial schedule INSERT from exact TF values, never by child UPDATE. Used line quantities/rate detail continue to retain the actual line authority and stored money.
3. Existing approved component key limit is 256; `ADDITIONAL:` consumes eleven characters. A 245-character schedule code is the exact compatible bound. Do not truncate 129–245-character genuine codes to the old 128 limit.
4. Revision lacks immutable source Umbrella identity. Capture the actual Candidate Umbrella under the genuine current approval transaction only when source TF channel is UMBRELLA. Retain PAYE NULL. Do not reconstruct historical records or assert later target Umbrella/VAT.
5. Original segment missing-rate refusal has no typed DETAIL; aggregate missing rate is combined with invalid quantity/rate. Separate actual absent used pay rate from invalid precision/quantity, retain the latter errors, and attach only `{bucket,contract_id}` to `BPAY_NEXT_ORDINARY_USED_RATE_MISSING`. Apply equivalent honest absent-rate refusal to real Additional/mileage use without changing stored principal totals or using absent × zero as authority.

Existing revision guard `1342:31–37` compares the full row via `to_jsonb`, excluding only seal timestamps; new revision columns are automatically pinned. Existing child guard `1342:96–117` refuses UPDATE/DELETE and post-seal INSERT for the actual parent. No extra trigger or money-owner change is needed for these fields. Core's current rate-schedule constraint names are confirmed by `current-contract.json:94228–94257`; remove only the all-NULL prohibition and widen the exact code constraint. Existing RLS/ACL/ownership and unique `(revision_id,rate_family,rate_code)` remain.

## Fresh 1009 proposal observations

The narrow migration on disk matches the above type/constraint proposal: all-NULL availability is permitted, exact code bound is 245, two nullable numeric(18,6) approved fields, and nullable revision source identity/provenance. It creates no counters, financial event or projection owner. This is an in-scope supporting schema; no historical identity is backfilled.

The fresh publisher constructs the available schedule once, adds absent genuine used Additional identities, and initially inserts exact TF approved values. Keep its Daily combiner careful: `0018:343–352` still omits all-NULL STANDARD rows. Filtering all standards from the canonical `{}` reader avoids duplicate existing standards but also omits genuinely missing Daily standard codes. If the carried Daily path is included in the freeze, merge only missing canonical codes as all-NULL rows. This does not require changing the Daily resolver or calculator.

The shared reader's actual current Source/protected callers are compatible with explicit missing rows. `03102026_1550:349–356/394–404` and `04102026_0345:357–374` count and insert through the same reader, so their expected schedule cardinality stays coupled. The positive protected calculated-rate predicate at `0345:776–780` requires equality to the selected pay rate and charge rate, not existence alone; SQL NULL equality cannot certify an all-NULL row. Registry confirms those physical files remain the finals of their source-stage functions. No Source producer edit is needed for this reader change. The final ordinary product review is separate from this proposal assessment.

## Evidence hashes

SHA-256, raw saved bytes:

| File | SHA-256 |
|---|---|
| `03102026_1510_banking_pay_next_rate_rows_v1.sql` | `98db776a421ca42b1b8e5a996f30c9739cdb340509e14f01e00c06908fb08188` |
| `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `03102026_1342_banking_pay_next_guards_v1.sql` | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql` | `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2` |
| `10102026_1009_banking_pay_next_ordinary_rate_authority_v1.sql` | `071cf3af4a6ae9ef1c63c93eb32d33b00c9bc93fbc51558d0598d3da5f00b0b4` |

Runtime/app joined acceptance remains NOT_RUN. This note does not close later Workbench projections, tax consumers, or all twelve checklist rows merely because the supporting schema exists.

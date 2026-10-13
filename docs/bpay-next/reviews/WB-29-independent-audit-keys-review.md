# WB-29 independent audit keys source review

12 October 2026, sequence140, BD161. **SOURCE PASS — narrow2303 migration only.** No remaining source defect found in the four index replacements. This does not approve the future W2 carry constructor/publisher, whole WB29, installation or runtime/business acceptance. Reviewer performed saved-file reads and this Markdown write only; no product edits, database/PostgreSQL execution, tests, fixtures, RPC or hosted actions.

Reviewed `supabase/migrations/12102026_2303_banking_pay_next_resolution_independent_audit_keys.sql` against original2044 DDL,2100 compatibility/event/audit guards, the saved original-index catalogue and `design/WB-29-inclusive-carry-decision.md`.

| Check | Result |
| --- | --- |
| Exact original preconditions | PASS. All four literal `pg_get_indexdef(to_regclass('private.'||name))` expectations exactly match the four saved PG17 catalogue definitions, including table/schema, key order, btree, original predicates and NULLS NOT DISTINCT. Missing or different index returns a distinct value and raises before replacement. |
| EO01 command-event identity | PASS. Only the existing event-request key gains `decision_id,decision_version`; command/event/line/family/code and `WHERE command_id IS NOT NULL` remain. Legitimate independent variants can audit under one real publication child. |
| EO04 actual VAT compatibility | PASS. Version, ACTIVE and published-ACTIVE keys add only `target_umbrella_vat_chargeable` beside target Umbrella. Every other key operand and original NULLS NOT DISTINCT/predicate remains, matching2100's actual VAT compatibility comparison. Equal full compatible identities still fail uniqueness; differing VAT variants are retained. |
| Immutable decision/event rules | PASS. Per-version SAVED/CLEARED uniqueness, NEED uniqueness, original boundary indexes, FK/check/state rules, event INSERT-only guard and deferred audit guards are untouched. No `ON CONFLICT` suppression or event omission is introduced. |
| Scope/atomicity | PASS. One transaction with four exact prechecks and four fixed DROP/CREATE replacements on private indexes only. No rows, table schema, public trigger, function/producer, command kind, quote/money formula, ACL or RLS is changed. Applied2044 is not edited. |
| Design alignment | PASS. Implements only BD161/EO01/EO04 index support. EO02 inclusive carry and EO03 exact server-authored carry provenance/manual-amount saved scope remain future implementation; this migration does not claim them built. |

The new event-request key does not replace closed caller validation: future W2 must still reject duplicate requested identities, detect exact mapped-compatibility ambiguity, preserve all distinct structurally mapped variants and use event→state order. Existing per-version event uniqueness remains stronger for duplicate events on one immutable version. Exact-owner CLEAR and unchanged carry metadata still require their separately reviewed native guards.

| Evidence | SHA-256 |
| --- | --- |
|2303 migration | `720AD72E771F21AB5263F9C0DDC198781A2F88315D968582460260978D54905B` |
|BD161 inclusive carry design | `176E92503E55A99C813A504E27B22423ADFE888E849BF4EE23D8568A76A4AD51` |
|Saved original independent-index catalogue | `208EEA9B7C6FEEB6DD3795DCB1CA8E6B9E21DC6D88BD0597A3F8708A436C3BD3` |

Catalogue provenance is root-supplied sole-local-PG17 metadata; this reviewer did not query or compile PostgreSQL. SOURCE PASS is limited to the saved bytes and their design alignment.

## Final source-gate classification receipt

**SOURCE PASS — exact inert resolutionIndexCensus allowance.** Re-read current scripts/bpay-next-source-gates.mjs:1920–1930 and the installed2303 migration. A static string comparison, without importing/running the gate or executing SQL, confirms the allowance's decoded literal equals the migration's complete normalized DO body exactly (1,743 characters).

Admission requires all of: `!dynamic`, the one exact2303 migration path, the exact `outer:<that file>` signature, and equality of the whole trimmed body. The four dollar-quoted CREATE INDEX definitions are consumed only as pg_get_indexdef comparison data. No EXECUTE, dynamic identifier/DDL producer, alternate file, routine signature or changed statement is admitted. Adding another statement or altering any predicate/key/body makes this allowance false; executable SQL remains subject to the original dynamic branch. The final hazard change adds only this exact census predicate to the existing inert-literal classification, leaving other hazard checks and closed construct rules in place.

The installed migration remains SHA-256 `720ad72e771f21ab5263f9c0ddc198781a2f88315d968582460260978d54905b`, matching the initial reviewed bytes. Current gate SHA-256 is `716dea02a5f0940f1ee0688e8658fe8ef281a75abb51c5925eb5c20402fb43e4`. No remaining source finding in this narrow classification. Root-reported1542 SQL gate PASS is not a test run by this reviewer. Wider W1/carry/whole-WB29 acceptance is outside this amendment and remains separately recorded.

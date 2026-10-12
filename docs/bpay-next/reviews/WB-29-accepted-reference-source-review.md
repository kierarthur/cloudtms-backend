# WB-29 accepted reference metadata SOURCE review

12 October 2026, seq140, BD158. **SOURCE PASS for current2227 metadata batch only.** Root alone wrote product. Independently read migration, complete three-function repeatable, current registry and exact pin source-gate delta; compared complete inherited Create and guard definitions against current2223/0426. No SQL/RPC, fixtures, tests, business rows, remote actions or app journey was run. No compiler/catalogue outcome is claimed here. Whole WB-29 and captured quote/hold/share/advice consumption remain open/NOT_RUN.

## Current reviewed bytes

| Saved source | SHA256 |
|---|---|
| `supabase/migrations/12102026_2227_banking_pay_next_accepted_resolution_refs.sql` | `2BC1B4E50FED7F33FAB5ECE420A7C53CDCE04F1DE92865DDB6DB79E8D6EFD85D` |
| `supabase/repeatable/12102026_2227_banking_pay_next_accepted_resolution_pin_v1.sql` | `739E238D39D6848C50EB90746C8F5486091EB39E38948EA28C95AE5D6353F8E8` |
| `docs/bpay-next/design/WB-29-accepted-reference-design.md` | `BD20105FD0D825972282F39C037A9C571FB5D06BA2CC34C8086C7EEC55BDC116` |
| `scripts/bpay-next-source-gates.mjs` | `ADF9405FBFAD5F6917467BE2A1069055E7B0C3988CB03943E92318FD2CD6B957` |
| `docs/bpay-next/final-definer-closures.json` | `411448EAD6BCFE9D8D34A556473EDC19CE255D6BFB7C4A60B1187C7A5854C7BA` |

Predecessor bytes were2223 `B3D7B39A31D35F573DBCC4EA21E9FC0015C801D09A02F1508FE8C9B3ED4535DD` and0426 `AEC4C904CEE22E748EA1AB3006E8CFDBF50BCDB9DD567B1FB1B2BC9DA7391B92`.

## Complete-parent preservation

Full definition comparison of six-argument `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)` shows539 predecessor lines versus540 current lines. There is exactly one changed SQL insertion: current2227:648 adds `accepted_resolution_refs` to the universal WorkChoice column list and650–651 appends `bpay_next_accepted_resolution_refs_v1(run_id,work_id,expected_revision_id)` to its values. No other function-body line differs. This preserves original descriptor/guard validation, original identical replay before new capture, locks, earlier-command refusal, native skip/query choices, same financial/selection counters, memory/ordinary paths and final acceptance. The cut remains outside memory-only handling and precedes SelectionCandidate/RunCommand creation.

Full comparison of `private.bpay_next_work_choice_guard_v1()` shows99 predecessor lines versus106 current lines. Only INSERT same-helper comparison88–91 and all-UPDATE ref immutability100–102 are added. Original exact open-Run/Work locks, accepted pay-hold facts, sealed-row immutability, native component/segment page counters and errors are otherwise unchanged. Current guard89 uses `IS DISTINCT FROM` and skips helper comparison only for historical SQL NULL. UPDATE101 uses `IS DISTINCT FROM` even while a SUBSET is OPEN and never calls the live helper. Original historical append0454 omits the new column, retaining NULL; no legacy input acquires an invented empty accepted cut.

Current Create remains SECURITY DEFINER with its original `pg_catalog,private,public` search path; guard remains SECURITY DEFINER with `pg_catalog,private`. Owner/revoke contracts715–718 are retained. No public routine or mutation action is introduced.

## Pin helper and bounds

`private.bpay_next_accepted_resolution_refs_v1(uuid,uuid,uuid)`9–62 is STABLE SECURITY INVOKER, private, owner-postgres and revoked from public/anon/authenticated/service_role. It point-loads exact Work, same-Work approved/sealed boundary revision, Run/pay date and native Work Candidate/Umbrella22–30. PAYE target U/VAT are explicitly NULL32–33; Umbrella requires the actual enabled identity and VAT fact. It has no dependency on not-yet-created SelectionCandidate, accepted descriptor or RunCommand. Original outer locks/cut remain authority; this helper takes no new lock, source seed or money operation.

The single mapped-row query36–39 reads ACTIVE rows for exact Work and based-on OR published boundary, then LIMIT101. A row cannot be duplicated by its OR predicate. `v_seen` increments40 and excess refuses41 **before** Candidate/target/policy/date compatibility filtering42–43. Thus100 compatible refs cannot conceal a101st mapped candidate. Existing2044 private boundary/published ACTIVE indexes support that native scope; the ordering may require a residual sort and this receipt does not claim constant physical scan cost or timing.

Each retained candidate uses the correct based-on or published line44, then the existing ten-argument ACTIVE helper45–46. That helper rederives exact native line/family/code/basis, target/policy/date and refuses two matching compatible versions. Returned ID/version must match the mapped row47–48; missing/basis-changed/ambiguous candidates cannot silently disappear. Duplicate output line/family/code is refused49–50. Each reference is built only from native UUIDs, family/code and canonical exact bigint version text52–53. The deterministic native ordering includes mapped line/family/code/ID39. No browser quote, amount, unit, decision list or selected rate is accepted.

The helper starts with explicit []17, returns it when no compatible mapped decision exists, and never turns an empty accepted cut into a later ACTIVE lookup. Result bytes are refused above32768 at55–56; mapped count was already limited100. No native child/history enumeration is used to discover absence. It records treatment metadata only, freezing no balance or financial headroom.

## Migration and line boundary

The migration consists solely of two nullable JSONB columns on existing private WorkChoice/RunLine. Array type is protected by CASE before `jsonb_array_length`; SQL NULL remains historical, JSON null/non-array is rejected. Work refs are at most100/32768 bytes; line refs at most5/8192. There is no default, row backfill/rewrite, table, public trigger, index family, grant or money event. Exact Work array shape is enforced by the INSERT helper equality, rather than trusting arbitrary nested JSON.

RunLine is deliberately not populated in this batch. Its existing08102026_0718:52–60 all-column BEFORE UPDATE/DELETE and BEFORE TRUNCATE immutable triggers cover the new field. Later hold INSERT must filter the Work cut to this exact captured line and validate original IDs/versions; that required consumer is still open. Existing native work-choice/preparation readers use explicit output fields, so the private metadata is not a new accepted browser contract in this batch.

## Registry and source gate

Current registry points Create, WorkChoice guard and the new exact helper signature to2227. The source-gate diff adds only that private helper signature and its finite actual callee list: existing ACTIVE helper plus upper/jsonb builders/array_append/octet_length (52,223). It introduces no SQL/dynamic/history waiver, broad construct exception, public exposure or relaxed inherited control. Root's normal source/schema checks remain required; this reviewer did not run them.

## Closed historical finding

**AR-S01 — closed before installation.** First reviewed repeatable hash `C24F004095D4431F121519695BB20C9E74161CA649ADD83BB70D2580B0EF1957` had inherited Create regex endings `$'` corrupted during assembly. Examples at its202–203 and215–216 lost the closing quote; namespace/revision checks were also altered. This was reported immediately before local compilation. Root reassembled using literal callback replacement. Fresh complete-body comparison at current `D330B8E…FA57C75` proves every inherited regex/body line restored, with only the intended WorkChoice insertion change remaining. No compiler or business success was inferred from that text comparison.

**Final narrow type amendment — SOURCE PASS retained.** Current2227 at16 declares the result of actual SETOF ACTIVE helper as `active record`, while native decision `d` remains its actual table rowtype. The SELECT at45–46 and exact ID/version check47–48 are unchanged. Inverting only that one ASCII declaration in memory reproduces the previously reviewed full-file SHA256 `D330B8E53521875857D78448D31DA53BEC1C5A64D3E68D3BD18E4D328FA57C75` exactly, proving no other source byte changed. Current receipt governs `739E238D39D6848C50EB90746C8F5486091EB39E38948EA28C95AE5D6353F8E8`. This correctly represents a function-result record without a new source-gate exception. Root reports normal source checks1539/157 PASS; that is Root-attributed evidence, not reviewer execution or database/business first use.

No essential source defect remains in this metadata batch. This PASS is limited to the reviewed saved bytes and does not pass the unwritten captured composer, line population, selected quote, share/advice parity or whole item.

# DY-05 SQL source review

Independent read-only review, 11 October 2026. **Current source verdict: PASS** for the final identities recorded at the end. Earlier CHANGES REQUIRED findings and source identities are retained below as history. No SQL compilation/execution, database rows, tests, fixtures, hosted calls or app acceptance were performed.

Read full card982–1006/all checklist D01–D04/dependencies and `design/DY-05-build-scope.md`. Read both new SQL files completely; compared complete publisher/guard definitions with actual 0939 parents and complete public wrapper with 0250. Product remained read-only.

Initial source identities:

| Source | SHA256 |
| --- | --- |
| `supabase/repeatable/11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql` | `02FECFC75856B82FBFEF68E522708EAF0EB6032C075290CFABB4B315FAC7D7E9` |
| `supabase/repeatable/11102026_1005_banking_pay_next_frozen_client_origin_v1.sql` | `A99894B65DCFD141A1152076F069160F2FD439CF7CBAACB8950A8E05DAAFAA08` |

## Essential findings

1. **Named Client-origin reader lacks closed classification and frozen tuple validation.** `1005:23–42` infers Daily from origin or two JSON flags, extracts a regex-shaped Client UUID from any Daily JSON, and returns no CLASSIFIED / UNCLASSIFIED_MISMATCH / UNKNOWN outcome. It does not compare the retained JSON's physical/version/financial/booking/Candidate/Contract/date or copied native settings Client/Contract to the immutable revision/Work tuple. Older or incomplete captured Daily evidence is indistinguishable from the new complete context; contradictory evidence may be displayed as authoritative. DY-05-D02 assigns this exact reader and the controlling PS-05 definition specifies closed outcomes (`card:1420–1426`); missing/contradictory authority must remain unknown/mismatch and never be a current Client inference.

   Minimal correction: validate only sealed captured JSON plus immutable Work/revision identity, with finite missing-versus-mismatch outcomes; emit NULL Client UUID if the required frozen Client proof is absent or mismatched. Keep frozen labels/amounts visible in all outcomes. No live public Client/Contract/Timesheet read, new snapshot/digest framework, backfill or Source producer change. Positively genuine contracted Daily may return its factual Contract; Contractless Daily remains NULL. Non-Daily without a captured Client fact stays UNKNOWN.

2. **Positive Daily seal guard does not bind native physical booking to immutable Work booking.** `1001:815` point-reads the actual captured physical Timesheet; `:831` compares JSON booking to immutable Work but never compares **`v_daily_timesheet.booking_id`** to **`v_daily_work.booking_id`**. A self-consistent JSON/Work pair can therefore pass this branch even if the referenced physical Daily belongs to a different booking. The card requires exact physical UUID/version/booking authority, including allowed unpaid physical rotations.

   Minimal correction: one NULL-safe native booking equality in the existing positive Daily guard. Retain the original physical/TF version check `:792–799`, current/nonstale authorised SELF_REPORTED TF and copied approval/role/band checks `:839–849`; no lock or money formula change.

These findings were reported promptly to root before this receipt. Source PASS is withheld pending the exact corrections and current-byte re-review.

## Correct retained composition

The complete 0939 publisher diff contains only:

- all-Daily aggregate base/detail creation rather than Contractless-only;
- reused corrected Daily evidence enriched with actual settings JSON, factual nullable Contract and native financial/Timesheet authorisation stamps `1001:227–242`;
- exact TF role/band and actual London worked date for both contracted and Contractless Daily;
- WORK:DAY key/date and original Daily shift/break branch for both genuine Daily shapes; real local break labels are added alongside native timestamps/duration.

Corrected 0925 remains the only evidence/resolver owner. Existing counted available-rate array/insertion, all fifteen genuinely nullable cells, approved rate fields, source Umbrella/rate references, labels, original amount/precision/count checks, staged publication and non-Daily/Source/expense behavior are retained. No monetary calculation was added. No Daily evidence/calculator/receipt/settings/Source producer, schema/table/column or new public trigger was created.

The new guard retains whole-row seal-only immutability/deletion refusal `1001:768–783`, original amount/line/financial/rate count checks `:785–812` and original required shift-detail proof `:852–867`. Its new contracted-Daily exception positively point-binds actual physical DAILY/HOURS, factual Work Contract, exact TF and captured JSON rather than merely accepting JSON on CONTRACT Work; non-Daily JSON remains refused. Work origin CONTRACT is not converted into DAILY.

## Existing native settings validation is already in the capture chain

An initial concern was that `1001:228–238` copies native JSON after tag/identity checks without an explicit frozen-reader call. Focused source trace closes that concern for the genuine producer:

- current Daily evidence `0925:66–72` uses `tsfin_load_context_batch` with the exact physical ID;
- actual context `21072026_1235_36_tsfin_load_context_batch.sql:245–256` calls the current settings core with that exact ID;
- current core `06102026_1214_self_bill_candidate_expense_separation_v1.sql:61–76` delegates any nonempty native snapshot to **`_timesheet_settings_authority_frozen_v1`** and refuses processed authority if no frozen snapshot exists;
- `03092026_1641_contract_settings_effective_authority_v1.sql:20–38` validates the native stored version/fingerprint and recomputes its original digest.

Thus a new direct helper call or new digest is not required if this exact existing chain is retained. Frozen context reads after capture use the copied sealed JSON, not this live pre-publication context. The capture and seal tuple checks remain distinct from the historical reader's missing/mismatch classification in finding 1.

## Wrapper, ACL and boundedness

The complete 0250 public-wrapper diff is confined to captured revision lookup, one origin context, encoded-byte accounting and the added `origin` envelope member (`1005:119–150`). Existing four kinds, fixed SQL choices, arguments/signature, admin/service-role/NEXT checks, row cap<=100 and 120000-byte ceiling remain unchanged. No new RPC/route/command/kind is introduced. For run-line kinds the original frozen-line guard is used before context; for RATE_SCHEDULE the actual original schedule source guard still executes before any successful response. A refusal rolls back the whole response; no metadata escapes via an exception path.

Origin JSON bytes are charged before row accumulation at `:128–130`; each row is charged before appending at `:138–145`. The inherited 512-byte envelope allowance remains conservative. Empty terminal pages still carry the same bounded captured origin; incomplete pages preserve original row/byte continuation semantics. The named private reader is STABLE/security-definer with fixed private search path, exact indexed revision/Work point reads, and owner-only revokes `:153–154`. The public wrapper retains only service-role execution `:155–157`; no browser-role/table ACL exposure is added.

No independent SQL syntax/column/type hazard was identified from these source comparisons. This is **not** compilation proof. Current Worker/Office must extend their exact closed envelope schemas for `origin` and retain the previously reported STANDARD-only all-NULL bucket allowance; those changed consumers are outside this SQL-only review.

## Two-channel and acceptance boundary

These files do not change original preparation/confirm/channel identity. Current 0847 confirm exact-revision replay and original readiness/enrolment fences, with 0236's finite PAYE-then-UMBRELLA existence probes, remain the shared two-channel authority. No new financial/channel protocol or synthetic child run was introduced.

Native compile/load, real Daily/contracted/ordinary/Source application behavior, copied native settings/approval evidence readback, rename/history/unpaid rotation, both channel 2/2 seals/retry and all app journeys remain **NOT_RUN by this reviewer**. No item PASS, build, installation or app-tested box is inferred.

## Corrected-source re-review — one finite classification amendment

Read the full current 1005, the complete 1001 definitions and complete-parent diffs again. Current 1001 SHA256: `B5F2F9EC742A11A88EDE05AE0EB433725B410907827BF10300B3540824A92A72`; current 1005 SHA256: `12BDAA095D3B8C292A7876D4C2D17FEC15EC66622D49398AD67B4E17CEA1C3C2`.

Initial finding 2 is closed: `1001:824` now compares the actual physical Timesheet booking with immutable Work booking using `IS DISTINCT FROM`. The native physical/version/financial checks, seal-only immutability, money/count/rate branches and positive contracted/contractless Daily identity remain intact. The publisher also calls the existing native frozen-settings helper directly at `:229`, validates its exact Client/Contract/DAILY/date binding, and copies that validated native object; no new digest or settings owner is introduced.

Initial finding 1's authority and closed-output gaps are substantially closed: `1005:36–57` validates captured revision/Work identity and sealed settings identity before assigning CLASSIFIED, and emits NULL Client UUID otherwise. Non-Daily without frozen evidence remains UNKNOWN. The existing four-kind wrapper, exact original caller guards, private ACL and row/byte ceilings are retained.

**One required amendment remains at `1005:34–35`:** a Daily JSON object with any Client string is assigned UNCLASSIFIED_MISMATCH before checking whether its required snapshot/tuple fields exist. An older sealed Daily evidence object containing Client but lacking the new settings/sheet-scope/native-authorisation context is therefore labelled mismatch despite missing authority. Keep UNKNOWN when that required captured authority is absent or damaged; enter the contradiction validation branch only after its required fields/shapes are present. Present, well-shaped but inconsistent identity values may then yield UNCLASSIFIED_MISMATCH. This is a finite presence/shape gate over the same sealed JSON, not a live lookup, new framework, backfill or producer change. It follows the initial finding's missing-versus-mismatch requirement and `PS-05:1419–1420,1432`.

**Current verdict: CHANGES REQUIRED for this classification branch only.** No additional essential SQL source issue was found. Compilation, installation, application acceptance and all earlier NOT_RUN boundaries remain unchanged.

## Final current-source closure

Reread both complete latest SQL files after the final missing-proof correction. **SOURCE REVIEW PASS: no remaining essential defect identified in these two files.** This verdict supersedes the preceding historical source verdicts only.

| Final reviewed source | Lines | SHA256 |
| --- | --- | --- |
| `supabase/repeatable/11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql` | 879 | `B5F2F9EC742A11A88EDE05AE0EB433725B410907827BF10300B3540824A92A72` |
| `supabase/repeatable/11102026_1005_banking_pay_next_frozen_client_origin_v1.sql` | 197 | `CCB57080D3119372BE6408C1CED497C95783EB06521D2727D4BE3FCE4436EA53` |

The final presence gate `1005:34–42` retains UNKNOWN when the required captured tuple/native settings context is absent, the Client UUID shape is invalid, native approval stamps are absent, or the settings object/required members are absent. This closes the evidenced older-Daily path from the preceding amendment. With captured authority present, `:44–63` requires exact immutable revision/Work/settings identity before assigning CLASSIFIED; a contradiction yields UNCLASSIFIED_MISMATCH with NULL Client UUID. Both unclassified outcomes retain the original frozen labels and payment visibility, without today's Client/Contract inference. Genuine NULL Contract is retained as a JSON-null identity operand.

The booking correction remains `1001:824`; actual physical/version/financial identity, copied native settings/authorisation, role/band and seal-only whole-row immutability remain checked. The direct existing settings helper `:229–242` preserves its native validation owner. Complete 0939 parent comparison retains original publication, rates, exact approved money/conservation, Source/non-Daily and expense branches; all-Daily WORK:DAY/clock/break metadata is the intended narrow change. The complete 0250 wrapper retains its four kinds, fixed queries, original caller guards, same cursor behavior and same <=100-row/120000-byte bounds, charging origin bytes before appending rows (`1005:157–188`). Private/helper revokes and service-only public wrapper ACL remain exact (`:191–196`).

No new storage, Client field, route, command, financial formula, producer, settings owner or hash/proof framework was added. The two-channel source boundary described above remains unchanged. Worker/Office closed-envelope and display consumers require their own source/screen review; they are not certified by this SQL receipt.

Both files were reported **uninstalled** at this review. No compilation/load proof was observed or claimed. All real application journeys, physical rotation/rename/history behavior, native settings/approval readback and PAYE/UMBRELLA 2/2 seal/retry acceptance remain **NOT_RUN by this reviewer**. This is a source verdict, not item completion or application acceptance.


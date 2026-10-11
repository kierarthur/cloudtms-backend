# EX-01 independent SQL source review

Reviewed 11 October 2026. Scope: saved source only, against card `04_EXECUTION_AND_OUTCOMES.md:102–128`, its EX-01 checklist rows, BD110 in `design/EX-01-design.md`, the independent design receipts, and `design/EX-01-scope-replay-investigation.md`. No SQL execution, database query, tests, fixtures, hosted action, product edit or commit was performed by this reviewer.

**Verdict: SOURCE PASS for the three saved SQL files identified below, after the two replay-preview corrections.** This is not compilation, installed-definition parity, Worker/Office integration, operational acceptance, or item completion. Those checks remain NOT_RUN by this reviewer.

## Reviewed bytes

- Migration `supabase/migrations/11102026_1302_banking_pay_next_reauth_proof_v1.sql`: SHA256 `E44001B0DE6BCC49384027D93A9DDE8FC8711B9BA996B4D4836EE734F0B68B30`.
- Repeatable `supabase/repeatable/11102026_1302_banking_pay_next_reauth_proof_v1.sql`: SHA256 `4E01BFB59BD7D414A79E12822649A06412AEE0D3313199539FEF9FCE944FFCE8`.
- Intake successor `supabase/repeatable/11102026_1306_banking_pay_next_reauth_intake_v1.sql`: SHA256 `6C94FE2563F53EDBFC77F61529FF8172215844C07F9AD3CA812ABFE7623DE3D5`.
- Design authority `design/EX-01-design.md`: SHA256 `84281490155A52DEC8F71276D1E39D9A6776A0EB364F9E2042685A5DF3313175`.

Line references below use these saved files. The migration and repeatables were still uninstalled and editable at review.

## Schema, authority and scope

The migration has precisely the card's proof fields: UUID identity, native actor FK, finite action kind, canonical scope/token SHA256, second-factor flag, verification/expiry timestamps, and optional unique consumed native identity. There is no raw token, credential, amount, secondary scope table, producer, or queue job. `consumed_command_id` deliberately has no command-table FK because CSV_ISSUE consumes its existing instruction UUID. The finite fifteen-action enum registers future contracts without manufacturing future owners. The challenge extension retains the three existing purposes and permits the two new scope fields only together for BPAY_NEXT_ACTION (migration:9–46).

The proof table enables and forces RLS, revokes browser/service-role direct table privileges, and uses the repository's owner-only policy. Private resolver/consume EXECUTE privileges are revoked from all public/browser/service roles; the two public service wrappers and retained intake keep service-role-only EXECUTE and explicit JWT role guards (migration:27–32; proof repeatable:282–320,363–372; intake:32–43,310–312). No unrelated security surface is widened.

The four-argument private resolver accepts only the six current protected native actions and closed, bounded string inputs; its owner-only Boolean selects locked admission or read-only preview. UUIDs and typed amount/time operands are canonicalized by the server; the digest is derived from actual actor/action/epoch, native identity, relevant inputs and finite native facts, rather than a browser-supplied scope (proof:26–58,65–72,190–197,260–278). The preview wrapper passes `false`; its current helper callees perform native point reads, not financial mutation or business locks.

## Native admission and replay

For first use, the resolver takes the original native business locks before the proof-row lock: WRITE_OFF uses the existing command mutex then Candidate control/Case/component; the other actions use their original Run, applicable Candidate control, worker, transfer, actor and returned-cash locks. It re-reads mutable native worker/transfer/cash/component rows after those locks. CSV bank inputs are held with their native read locks through the original binder/issue calls (proof:73–82,104–138,228–248; intake:149–157).

Replay uses actual retained requests/instructions. WRITE_OFF compares component, original actor, expected revision, scope, raw requested amount and reason; CASH_REISSUE and REISSUE_CANCEL compare their retained native identities, actor and exact whole amount; CSV_RETURN compares original command/actor, transfer/worker/Candidate, outcome kind, whole amount, receipt and typed timestamp; CSV_ISSUE compares instruction/transfer/worker/original issuer and immutable cash (proof:83–91,144–150,160–165,178–183,199–207,219–224). Non-CSV accepted recovery additionally requires the original native command kind/current epoch and that identity's consumed proof. It returns that stored scope after qualifying the retained operands, so later balance mutations cannot reprice an accepted request (proof:260–272).

The exact five-argument consume helper locks the proof row, rejects mismatched actor/action/identity or scope, allows only the same consumed identity to replay before checking expiry, and records first consumption once. A failure in the subsequent native acceptor aborts the same transaction and rolls proof consumption back (proof:342–359; intake:149–157,300–304). Audit recording takes only a token audit mutex; it does not invert the native business-lock order (proof:321–338).

## Corrections inspected

The initial CASH_REISSUE replay preview subtracted today's held/paid values, which becomes zero after its build. Saved proof:172–173 now uses immutable `amount_owed` for accepted recovery, consistent with the independently traced current native whole-pool writers. The first-use branch still rejects positive partial held/paid shapes; there is no new partial reissue model (proof:167–175; scope investigation section 5).

The initial accepted CANCEL preview could lose its retired Case cash projection and switch basis implicitly. Saved proof:150–153 now explicitly labels the immutable frozen Draft gross as DRAFT_GROSS; it does not claim that amount is original authorized bank cash. Accepted scope remains the original consumed scope. New row projections use `r.*` rather than an unqualified SELECT star; no source-gate waiver was requested or introduced.

## Intake and retained owners

The successor extracts only `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` from current 2150. Its closed schemas add a proof UUID only to the six existing protected branches; unprotected outcome/net/prepare-expiry/retry behavior retains its parent guards and shapes. The entire fixed native dispatch and bounded result section is text-identical to the parent, including all original callee arguments and result fields (1306:162–308 versus 2150:338–483). The changes occur before dispatch: closed proof parsing, locked authoritative scope, and same-transaction consume (1306:28–30,71–93,149–157).

Tokenless CSV admission is possible only when that exact original instruction has already been issued and the retained original issuer/transfer/worker/cash tuple matches. A new instruction has `accepted=false`, reaches consume with a NULL proof and is refused; a different UUID for an already-issued transfer is refused. Original readback skips destination rebinding and returns original CSV bytes through the unchanged owner (proof:219–227,260–268; intake:154–157,245–265). There is no generic tokenless command recovery exemption.

Remaining delivery boundaries are explicit: genuine code/password policy and trusted token-to-proof recording must be completed in the Worker; Office must preserve original unknown submission bytes and distinguish a new issue from retained instruction readback; current catalogue/source gates, normal local compilation, browser-role exposure and screen evidence must be recorded by root. No acceptance result is inferred from this source review.

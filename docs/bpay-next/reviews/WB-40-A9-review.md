# WB-40 A9 correction review

12 October 2026. **SOURCE PASS.** No essential source defect found. This is the exact owner A9 correction/BD136, not a reopened WB40 feature or DR17 build. Read-only source/design review; no database/SQL/business execution, fixtures, tests, hosted edit, product change or runtime/install claim by this reviewer.

Reviewed `design/WB-40-A9-correction.md` in full and complete `supabase/repeatable/12102026_0611_banking_pay_next_pay_hold_missing_control_guard_v1.sql` against complete current parent `11102026_0426_banking_pay_next_pay_hold_controls_v1.sql`. Extracted full function-body comparison shows exactly one executable replacement: parent0426:45's missing-control exception becomes0611:44 `if not found then return new;end if;`, plus its two explanatory comments. No other function-body difference exists.

## Correctness and retained behavior

- The exact Candidate control lookup remains `candidate_id=v_candidate FOR UPDATE` (0611:39–44). FOUND is read immediately after that statement. If one Candidate lacks a control row, the public native edit returns NEW before **any** private Work lock/mutation, financial-view increment or Workbench refresh; those first occur at46/56–61. Earlier iterations only acquired existing Candidate locks. There is no partial projection write and no invented control row. If no native Work is in scope, the original early return36 remains.
- Normal order is unchanged: public edit already owns its native row; distinct Candidates are ordered/locked first (39–45), then exact Work IDs in UUID order (46), then the original per-Work canonical point/update/refresh loop (47–62). The successor introduces no public lock after Candidate/Work, new lock inversion or generic deferral behavior.
- NEXT inertness10, exact public relation11–12, native currentness/event-tuple and unchanged-update returns13–34, booking-to-native-Work discovery35–36, canonical current Timesheet/financial tuple48–53, identical-value skip54–55 and exact stored hold-control/version/FVR/refresh behavior56–61 are retained verbatim. The correction does not alter accepted Draft capture, financial command guards, hold allocation or execution money.
- The signature stays `private.bpay_next_pay_hold_control_trigger_v1()`, RETURNS trigger, SECURITY DEFINER, fixed `search_path=pg_catalog,private,public` (4–6), same logical owner and PUBLIC/anon/authenticated/service_role direct EXECUTE revocations66–67. The schema reload notification68 adds no public capability. Current `final-definer-closures.json` resolves this signature to0611.
- No trigger is added or rebound in0611. Original0426:69–84 owns the two existing AFTER INSERT/currentness/hold UPDATE bindings on `public.timesheets` and `public.timesheets_financials`, including its exact two-binding verification. CREATE OR REPLACE retains the same function identity used by those existing bindings. Their native public-caller permission/transaction behavior remains the original owner; this review does not infer catalogue installation solely from saved SQL.

The documented reason is correct: a Work's Candidate identity does not itself guarantee NEXT control enrolment, and an existing native public edit can reach this projection trigger before that enrolment. Returning NEW safely leaves the native edit to its original caller. It does not weaken strict financial command/lease/control admission or silently create a private projection.

## Reviewed saved bytes

| File | SHA-256 |
|---|---|
| successor0611 | `70B3AC78A246BEE6CE440FE838D30C9B8D94E1904B44C2214AC5ECDFD47741F1` |
| parent0426 controls | `AEEDD544D3E1A89D4CC5802E8304E0826B4D0044D1EA43BCFB52B5B3B6AE2DE9` |
| `WB-40-A9-correction.md` | `9487ABC88D460C9D0F60DD9C2660AEBEA0D9C615FD3BFE2C1234EC2535CA3D31` |

Source PASS does not prove a missing-row business journey. Root's authorised compile/catalogue/release evidence is separate; business/hosted acceptance remains NOT_RUN by this reviewer.

## DR17 policy correction

Also corrected `design/DR-17-build-investigation.md` after reading the complete binding `SCREEN_DESIGN_POLICY.md`:172, SDP34: expanded children, result lists, fixed business order and **dialogs are exempt from whole-column sorting**. DR17's existing native five-tab readonly dialog is exempt. The earlier sort-extension suggestion is withdrawn: no new sort system, argument, index or sort reader is required. Keep existing finite native continuations and fixed canonical bucket presentation. This exception does not modify later DR31's owning list requirements.

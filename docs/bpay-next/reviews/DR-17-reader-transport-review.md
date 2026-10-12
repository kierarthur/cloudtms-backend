# DR-17 frozen reader and transport source review

12 October 2026. Independent source review against complete card03:449–471, all D01–D08/ITEM, BD137 `DR-17-build-scope.md`, and the supplied SCR47 comparison recorded in `DR-17-screen-investigation.md`. Root is the sole product author. **SOURCE PASS for this bounded reader/transport and current review-control slice.** Two presentation-scope findings were reported and are closed in the current saved Office source. No remaining essential source defect identified.

This does not approve A4, joined financial acceptance, or the whole DR17 item. D06's actual DR31 tree host is expressly deferred to its generated sequence158 and remains partial.

## Exact native successor and authority

Compared the complete `12102026_0620_banking_pay_next_frozen_used_line_detail_v1.sql` with current parent1005. Its behavior delta is exactly the captured `source_kind`, the guarded `used_line` point read/serialized-byte charge, its declaration and the additional response key. The private origin helper reads `v_revision.source_kind`; no live Source/Timesheet/Client/rate classification was introduced. The public reader's actual line quantity/unit/rate is selected from `bpay_next_approved_line` by the original `simple_frozen_line_guard_v1` approved-line ID and captured-revision pair. All other kinds return NULL for `used_line`. No pricing, conversion, rate division, rounding, money write, publisher/capture change, new kind or route.

Sources:0620 origin return67–82, used-line point168–180, response budget181–200 and final return202. Schema authority: original core migration1324 approved-line188–207 permits nullable unconstrained numeric quantity/unit-rate; existing rate-detail245–254 and rate-schedule262–276 use numeric(18,6). Therefore the new line decimal validator correctly preserves native text within the response budget rather than imposing the older rate-schedule precision on these different operands. Genuine historical NULL is accepted and displayed as a dash.

The same public signature, SECURITY DEFINER/search_path configuration, service-only execute grant, browser-role revocations, active-admin/NEXT gates, fixed four-kind SQL dispatch and 1–100 limit are preserved. Original private detail/schedule guards still require parent REVIEW/DRAFT and worker READY. History/paid/cancelled state-independent ownership is not widened. The registry promotes only `private.bpay_next_payment_client_origin_v1(uuid)` and `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)` to0620. Four original run_work/run_line immutable attachments and their effect guard remain unchanged.

## Closed Worker/Office parity and bounds

Worker `broker/src/bpay-next-frozen-read.js:72–73,142–166,319–326,378–393` and Office `js/bpay-next-office.js:340–352,499–502,867–894` have the same closed 18-field origin contract after the existing validator type-name aliases are normalized. The captured source enum is exactly ORDINARY/SOURCE/PROTECTED/EXPENSE/ADJUSTMENT. Both used-line schemas have the same five required keys, the native six-kind component enum, 1–256-character non-NUL key, nullable unit text, and nullable signed decimal text. RATE_DETAILS requires that object; every other kind requires literal NULL. Unknown fields, missing fields, wrong kind and malformed values fail closed.

Canonical rate codes now admit 1–245 native characters and forbid NUL in both consumers. Removing the former non-Standard all-NULL refusal admits genuine nullable Additional/mileage schedules without converting missing authority to zero. Standard and mileage code qualification, row identity/ordinal uniqueness, finite family enum, original cursor pair/null rules and exact returned next-cursor checks remain. Existing decimal precision is unchanged for native numeric(18,6) rate rows.

The SQL byte account charges origin and used-line JSON before rows within its existing120000 budget; a row that cannot fit produces the named refusal, and byte-short pages retain continuation. Worker input is bounded2048 bytes, dependency/output128KiB, one native page<=100 and an awaited8-second abort deadline; it checks serialized final output again. Office retains the same bounded response decoding and five-key page shape after extracting origin/used-line. No relaxed request identity, partial/unknown mutation semantics, permission, or new public exposure is introduced.

## Native child and review integration

The five READY expansion controls all call the same exported `openFrozenWorkerDetail` with the actual native run/Candidate/worker tuple and one finite `detailInitialTab` value. Existing callers default to Lines. No-Case READY expansion now offers these controls without fetching an empty Case list; non-READY Case expansion retains its existing behavior. The child still uses the original returned line, exact returned Shift and captured Work discovery, not guessed IDs, a current Timesheet lookup or an assembled all-Candidate inventory.

Its RATE_DETAILS attach step additionally checks returned `used_line.component_key` and `component_kind` against the selected native LINES row, alongside the original captured revision/origin equality and actor/owner/namespace fences (`Office1735`). Original three-page retention,32KiB history, natural server keysets, continuous scrolling, scroll/focus restoration, selected Shift preservation and exact failed-read retry remain. The controls load the requested detail through this same child; no all-tab eager fetch or separate window host was added.

Original approved quantity, unit and pay-rate facts are shown separately from frozen payment amounts and native STATUS totals. Significant numeric precision is retained through string formatting. Available rates keep separate PAYE/Umbrella/charge cells and units, fixed Standard order, every returned Additional/mileage row, and genuine NULL dashes. Available charge is not relabelled actual used charge. The common caveat is Approved quantities, not paid hours. No browser financial calculation or live repricing was added.

## Reported findings and fresh closure

- **R17-01, closed:** the selected revision's Source/Protected caption was also shown above the whole-Candidate Lines table. Current Office1769 requires `tab !== 'Lines'`, so it only labels the selected detail views; the Lines payment kind still uses each native line's explicit PROTECTED_WORK classification. Whole SOURCE revision classification does not become a per-Shift protected claim.
- **R17-02, closed:** Additional/mileage can return a genuine used-line object with no hourly bucket rows. Current Used rates rendering shows its captured facts/component subject, creates a bucket table only when those rows exist, and the complete-empty notice1794 is suppressed when `rates.used_line` exists. It no longer says No recorded detail beside genuine detail.

SDP34 exempts this existing dialog/expanded child and fixed business order, so the original native line/detail/bucket continuation remains sufficient for this slice. No fake whole-list sort arrows or loaded-row monetary totals were added. The actual DR31 beneficiaries host, tree/address behavior and sorting remain its later owning deliverable; this callback seam is not evidence that tree acceptance ran.

The reviewer performed only saved-file reads, static source comparison and hash calculation, plus this review-file write. No product edits, tests/syntax/build checks, SQL/database/business invocation, fixtures, browser/render/capture/hosted calls or commits. Root's compile/catalogue/seal evidence is separate and was not independently executed here. A4 and the card's unchanged-after-edit/immutability/mixed-origin/both-channel acceptance remain NOT_RUN by this reviewer.

## Reviewed SHA-256

| File | SHA-256 |
| --- | --- |
| Native0620 | `96a26c83328dbb7253e8526c789dd11544d04d1b8a9dcf1b421e946fcd7dbc65` |
| Worker frozen reader | `79e2a9b16e1d09d879d121e011cee10f88ee2adff535fb0f39d1936a2bc217e3` |
| FE Office, after R17-01/02 fixes | `fd6640d1218eb84e59270ec4b4bf38942b1311cc4518bf59d6cbffee2e68894c` |
| FE Office CSS | `3c6e8faeea37f90d4b830733d27deedf9aba816a762edfb217c65d742dfd2c64` |
| BD137 scope | `cc43f49920e1b8a8b0f6b4bce0368c8c3532e939302df6d089e57ea360831357` |

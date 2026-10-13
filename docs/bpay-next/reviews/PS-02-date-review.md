# PS-02 stored business dates source review

10 October 2026. **PASS for the reviewed finite migration, source reuse and saved future checklist.** Read the complete new `10102026_0544_banking_pay_next_ps_c02_stored_business_dates_v1.sql`, `design/PS-02-SCOPE.md`/BD032 and `PS-02-JOINED-ACCEPTANCE.md`, against full PS-02 card/checklist, approved PSP-15/18 and the actual stored clock/guard/final-owner source. Supporting producer details are in `PS-02-date-investigation.md`. No SQL, tests, source gates, fixtures, runtime, network, secrets or product edits were performed. Physical compile/apply success and business acceptance are not claimed.

The migration adds exactly one stored generated `business_date` to each of `private.bpay_next_transfer_outcome` and `private.bpay_next_internal_receipt`. Both derive solely from their own existing `occurred_at_utc` using fixed Europe/London and date cast, are NOT NULL, and reject nonfinite dates. The fixed-zone timestamptz conversion plus timestamp-to-date cast is an immutable expression suitable for PostgreSQL stored generation; it does not depend on the session TimeZone, current time, input text, mutable tables or a public date argument. The original UTC occurrence/ingestion values remain unchanged. Physical PG17 generation/catalog validation belongs to root's authorized apply receipt, not this read-only review.

The outcome comment and scope correctly restrict paid meaning to an actual SETTLED outcome and return meaning to RETURNED. They retain independent server ingestion and reject using economic posting/request finish as a bank date. Internal date is a real zero-cash accounting receipt occurrence, never a bank payment date. This matches PSP-15/18 without extending a new public financial input or creating fictitious events.

The existing outcome trigger invokes the immutable effect guard for every UPDATE/DELETE; the internal guard rejects every non-INSERT operation and checks original clocks at INSERT. These protections cover the new columns automatically. No existing BEFORE INSERT guard accesses the generated date, so generated-column computation order introduces no new seam. All active producers use explicit column lists and retain their current transactional/replay behavior. Current closed history/command/public reply fields do not gain an unsolicited field. Ownership, revokes and RLS configuration are not changed; no money/Source/Case/provider writer is altered.

BD032/D02/D04 accurately reuse actual 2018 settlement receive/post and 0200 claim rather than cloning old installed bodies. SETTLED/RETURNED intake inserts its append-only fact and linked request/status in one transaction and retains exact original occurrence/ingestion on receipt replay. Internal acceptance does the same for its genuine receipt and INTERNAL_PROCESSING; later completion never rewrites receipt occurrence. Reissue creation remains cash lineage, not paid or earnings; its later genuine settlement owns its paid date. No additional owner successor is needed for these two schema additions.

D05 is a finite saved future checklist marked actual app/installed/business/Worker/queue/minute NOT_RUN. It covers reported occurrence versus ingestion, UK midnight/BST, replay/conflict, atomic abort, independent return/reissue dates, internal zero accounting, generated-date immutability and security. It correctly defers bank CANCELLED schema/producer to EX-16/EX-27, treats BLOCKED as no cancelled cash, and keeps provider-state enums unavailable until actual producer delivery. This source verdict does not qualify those future producers, install status or all runtime channels.

Fresh source SHA-256:

| File | SHA-256 |
|---|---|
| `10102026_0544_banking_pay_next_ps_c02_stored_business_dates_v1.sql` | `7537d120fb13c61b78b06a77e5697065152529f4c7ad911bdf7e83653fd5ce5b` |
| `design/PS-02-SCOPE.md` | `5adddf2a3a6d8b36bf02e704277585cab1a64f4f11674e178373c2efbfdea1d9` |
| `PS-02-JOINED-ACCEPTANCE.md` | `3e2ad439bb3371b92edc997e6e025c3626ea5734cfb77a5e028b84426dd9a29d` |
| Existing 1342 immutable guard | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |

No actionable source finding remains within this item. Only review documents were written by this reviewer.

Fresh closure: re-read the current scope and joined checklist after their readability edits. Their finite meaning and NOT_RUN limits remain unchanged; the table now records those current saved bytes. The migration source is unchanged and the source PASS remains valid.

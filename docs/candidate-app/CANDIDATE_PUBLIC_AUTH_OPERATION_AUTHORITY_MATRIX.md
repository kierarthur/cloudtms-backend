# Candidate public authentication operation authority matrix

Decision closure date: 12 August 2026.

This matrix is controlling for the bounded public-authentication correction family. It covers the public Candidate broker, signed private Candidate backend, Candidate RPC/database receipt, public response construction and supported credential rotation/rollback. It does not change Candidate product workflow, Office modal behaviour, financial calculations, Banking Pay, Policy X or production authority.

## Family-wide invariants

- One caller-owned bounded `idempotency_key` identifies one factual operation.
- Same key plus the same factual request returns the same durable result.
- Same key plus a changed action, email/purpose, session, device, provider, token or payload returns `CANDIDATE_IDEMPOTENCY_CONFLICT`.
- Server-generated session IDs, refresh tokens, challenge tokens, mail rows, token hashes, expiry timestamps, storage IVs and credential wrapping are execution detail, not factual request identity.
- The database receipt is written in the same transaction as the factual mutation. A crash before that transaction commits leaves no success; a crash after it commits is recovered by receipt-first replay.
- Public credentials use v3 envelopes containing the issuing key version. Every authority has one current writer version and an explicit reader catalogue.
- The broker proves the selected access, refresh, public-session and PHONE wrapping secrets before forwarding a mutation that will freeze those versions. A version misconfiguration therefore cannot commit a private winner and only then fail public reconstruction.
- Login, password completion and refresh freeze their public access, refresh and public-session mapping versions in the durable private result.
- The approved rollback is the same version-aware build with the former writer versions restored and all credentials issued by the newer build retained in reader catalogues. A pre-v3 broker is not an approved rollback build.
- No raw password, private refresh token, challenge token, PHONE token or push token enters an ordinary audit receipt or public response.

## Operation map

| Operation | Factual identity | Generated execution | Durable owner/result | Exact replay and changed input | Rotation/rollback |
|---|---|---|---|---|---|
| Challenge start | environment, normalised email, purpose, caller key | challenge UUID/token, expiry, deterministic mail key | challenge transition plus auth mutation receipt; public response is enumeration-safe 202 | missing/oversized key is 400; same request is 202; same key with changed email/purpose is 409; only eligibility/account-state outcomes are masked to 202 | challenge token uses retained private key versions; broker credential versions are not involved |
| Challenge resend | environment, normalised email, purpose, exact challenge ID, caller key | replacement token/expiry and deterministic mail | challenge transition plus receipt | same factual resend replays; changed challenge/email/purpose conflicts; missing/oversized key is 400 | retained challenge-token reader authority reconstructs the winning token |
| Challenge verify | environment, email, purpose, challenge ID/token proof, caller key | none beyond response | challenge receipt and single-use state transition | exact verification replays; changed proof or identity conflicts | private challenge-token authority only |
| Password completion | verified challenge, selected Candidate, password proof, device/platform facts, caller key | proposed session, verifier material, deterministic private refresh token, public credential wrapping | account/session mutation and auth receipt; result freezes winning session, issue/expiry facts, private replay key version and public credential versions | exact lost-response/concurrent retry reconstructs the database winner's private refresh token and byte-identical public v3 credentials; changed password/device/selection conflicts | frozen v3 writer versions are used after rotation; retained reader catalogues and secrets support rollback |
| Login success/failure | normalised email, selected Candidate, password proof, device/platform facts, caller key | proposed session and deterministic refresh token; failed-login counter mutation | auth receipt contains success or generic failure; successful result freezes public versions | one failure advances the counter once; exact success/failure replays; changed password/email/selection conflicts | exact replay uses recorded private and public versions, not current writer versions |
| Refresh | old private session, presented refresh proof, caller key | proposed successor session and deterministic successor refresh token | refresh-family transition and receipt freeze the database winner and public versions | concurrent/lost-response retry returns the same successor; a different key using the rotated token retains theft detection; changed old token/session conflicts | access/refresh writers may rotate; the initiating public-session mapping version remains frozen from the incoming public refresh credential |
| Logout | authenticated private session plus caller key | none | private session revoke and auth receipt | public broker must unwrap the public access token and forward the exact private bearer; exact concurrent retry recovers the receipt after the session becomes revoked; missing/invalid public access is 401 | public access v1/v2/v3 readers apply; logout cannot use the unauthenticated route catalogue |
| Select TEST Candidate | authenticated session, selected Candidate, caller key | replacement private/public access token | session selection and receipt | exact retry uses original issue time and access key version; changed Candidate conflicts | selected access remains on the initiating public access envelope's issuing version |
| Notification preferences/read | authenticated session, exact preference object or notification ID, caller key | read timestamp for first acknowledgement | account/notification mutation and receipt | exact replay returns the first state/timestamp; changed object/notification conflicts | no public credential generation beyond authenticating the request |
| Password change | authenticated session, current/new password proofs, caller key | verifier salt/digest | account mutation, session revocation and receipt | exact concurrent retry recovers after the old verifier moved; changed password facts conflict | private auth replay key version is frozen in receipt metadata |
| Push registration | authenticated private/public session, provider, raw-token semantic HMAC and caller key | randomized versioned storage ciphertext | session token storage plus receipt; metadata freezes semantic identity key version | new storage IV or storage-key version does not change identity; during identity-key overlap the backend selects the originally frozen proof; changed token/provider/session conflicts | ciphertext embeds a positive database-safe storage version; proof catalogue covers configured identity readers; rollback retains all issued/receipt versions until bounded expiry |
| PHONE selection/handoff | workflow, expected generation, caller key, public-session digest and optional supplied-device digest | private manager token/hash/expiry and public v3 handoff wrapping | global environment/key workflow receipt plus exact approval request; response freezes private and public handoff key versions | exact replay from the same binding returns the same usable handoff; another workflow, session, changed supplied device, generation or factual payload conflicts | v3 handoff embeds broker version; private and public retained versions reconstruct the first result; rollback reader accepts credentials issued by the new build |

## Durable boundaries and crash recovery

| Boundary | Required recovery result |
|---|---|
| Before signed private request reaches the backend | No factual mutation. Repeating the public request proceeds normally. |
| After private request validation but before RPC commit | No durable success. Repeating the same request may execute once. |
| After RPC mutation/receipt commit but before private response | Receipt-first retry returns the committed result without a second mutation. |
| After private response but before broker wrapping completes | Private receipt returns the same frozen generated facts and key versions; broker reconstructs the same public credential. |
| After broker returns but client loses the response | Exact public retry returns the same status and public result. |
| Concurrent same-key callers | The environment/key or workflow/key lock serialises them; all exact callers receive usable copies of the database-winning result. |
| Same key with a changed factual request | The receipt hash/action/workflow binding fails with `CANDIDATE_IDEMPOTENCY_CONFLICT` before a second mutation. |

## Reader and writer deployment order

1. Install database/private response support and verify old public traffic remains accepted.
2. Deploy the private Candidate Worker and normal TEST backend readers.
3. Provision new versioned broker secrets without changing the current writer version.
4. Prove the new broker build reads v1, v2 and the planned v3 version and that the approved rollback configuration reads the planned new version.
5. Deploy the public broker last, with one current writer version and explicit overlapping readers.
6. On rollback, restore former writer versions in the same version-aware build; do not remove newly issued reader versions until all bounded credentials/receipts have expired.

## Mandatory verification matrix

For every applicable operation, verification must include:

- public broker to signed private backend to RPC/database to public response;
- exact lost-response replay;
- two concurrent same-key requests;
- same key with changed factual input;
- generated execution drift after the first commit;
- crash recovery after each durable boundary that can be simulated without external communications;
- writer-key rotation with retained readers;
- approved rollback-reader compatibility;
- invalid or retired reader-version rejection;
- proof that no extra mutation, mail, notification or external provider call occurred.

The independent audit must inspect one layer beyond each reported seam. It should issue GO and stop when the bounded family, controlling decisions and mandatory matrix pass with no concrete release blocker. If it finds a genuine blocker, the next correction must cover the whole affected operation family rather than only the first reported function.

## No-change boundary

This authority does not alter DAILY/WEEKLY calculations, rates, pay, charge, VAT, ERNI, margin, TSFIN, Process, Authorise, invoice grouping/generation/issue, payments, Banking Pay, Policy X, settlement, remittances, Office modal behaviour, Candidate timesheet Current/History rules or production.

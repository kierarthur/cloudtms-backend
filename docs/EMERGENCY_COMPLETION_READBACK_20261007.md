# TEST emergency completion read-back — 7 October 2026

## Failure and correction

Google can finish sending an approved alert after the Worker's bounded request expires. The agency's original effect-attempt journal correctly records UNKNOWN; that row alone cannot prove whether the continuing Google execution finished. The candidate must not resend an uncertain effect.

The Availability specialist adapter now saves a durable completion receipt only after an approved effect returns accepted=true. Its receipt identity contains the environment, exact operation and effect key. EFFECT_STATUS_READ is a signed, closed read-only Google operation, not another public Candidate method or provider action. It never creates a sheet or sends a message.

The existing getCandidateDailyEffectStatus path first calls candidate_daily_effect_status_candidate_v1 to prove Candidate ownership. For UNKNOWN/IN_PROGRESS only, it checks the Google receipt and requires the same effect/operation and a valid completion timestamp no earlier than the original attempt. A confirmed result is projected as COMPLETED. The original agency attempt row remains immutable UNKNOWN: this is derived final status, not an invented journal update. Missing, stale, crossed or unavailable proof remains pending. Completed/failed-final results retain their existing authority. Same-key replay consults this read path and never reclaims or resends.

## User experience

Pending: Checking your alert; automatic status reads for up to two minutes, then a Check status action which reads only. Completed: Update completed and the normal sent-through-agency-service message. No instruction to contact the agency to check a completed alert. A genuinely failed-final send still explains its failure. Provider acceptance never claims independently verified handset delivery.

## Compatibility and rollout

Publish the reviewed Availability adapter/delivery/date changes to the existing TEST Google deployment first. Preserve its owner, URL, enabled legacy emergency service and settings-controlled TEST/LIVE targeting. Then use the managed TEST backend coordinator with fresh four-Worker connection evidence; compatible private/synthetic authority precedes the public broker. Refresh the USB development app through its canonical launcher only.

No SQL, Candidate operation inventory, financial policy, eligibility window, source date, recipient route, provider credential, transport key or LIVE resource changes. No historical receipt backfill and no resend of alert f32098a48e12ee20b2357d6da569a27f487c92a4c3353de25a8089c709dd745e.

## Pre-publication evidence

Full backend suite: 1,473 passed. Native: 609 tests / 67 suites, TypeScript, Doctor 21/21 and native/web exports passed. Responsive browser: 214 passed. Google focused fixtures cover every emergency family, acknowledgement dates, UK-day suffix, per-phone cross-route counts, exact signed read authority, saved completion, replay and uncertainty. Physical new-completion and absent-person delivery acceptance remain pending until installation and a separately authorised test send.

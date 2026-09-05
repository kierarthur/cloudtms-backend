# F-010 rejected whole-route executor evidence

The provisional file `02092026_2314_banking_pay_draft_atomic_execute_v8.sql`
was rejected before release because it attempted to execute the complete Draft
business route inside one database call. Its SHA-256 was
`C5D3D9026C5AB9BCA227A2A56CF5E41030E31930907E46B779BA1E0FCDB413C0`.
The associated source-only test had SHA-256
`3C064F2BB2EDFBB075A0CD79CA5D4E232B354D18D2463A1C147DAF6114F24423`.

That design could move the existing 99% delay into a single long statement and
could not prove the 50,000-row path remains within the existing Miget statement
and lock budgets. It was never committed, published, installed or deployed and
must never be restored to `supabase/repeatable`.

The replacement is `02092026_2330_banking_pay_draft_bounded_advance_v8.sql`.
It performs at most one bounded existing-owner business page per external call,
stores a replay-safe receipt, preserves the complete server-backed selected
universe and leaves every payment-policy decision with the existing owner.

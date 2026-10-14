# Published HealthRoster helpers on TEST

## WP-PP v2 — installed and read back, 14 October 2026

Installed backend source: **f743b81fe537830d81b1de93ec4f9cb0331532a7**. Protected database release **20261007-candidate-emergency-complete-roster-v1-upgrade-f743b81fe537**, [run38583323424](https://github.com/kierarthur/cloudtms-backend/actions/runs/38583323424), VERIFIED at15:47:03.075975UTC. All158 registered verifiers passed with zero failures, including the WP-PP actor/read/ACL verifier. Repository and installed PostgreSQL17 contract: **7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6**. Ledger:389 migrations and1019 repeatables. No feature activation occurred.

SQL ABI: **private.weekly_source_pay_actor_allowed_v1(p_actor_user_id uuid) → boolean**, STABLE SECURITY DEFINER, fixed search_path pg_catalog,pg_temp. Installed canonical pg_get_functiondef SHA-256: **736fa960e78a591c8f1dbc3b3750dffa03cb504f87c8c46a0b2d0bf913042289**. Logical owner postgres maps only to the audited Miget service owner. EXECUTE is granted to service_role; PUBLIC,anon,authenticated are denied. Permission is fresh active Admin OR payment_authoriser OR payment_golden_key. Missing/inactive/unflagged actors are denied. This helper decides permission only; it does not classify money, evidence, source scope, Draft freshness or payment/provider action.

Source root: supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql, exact Git blob SHA-256 **8f65ee0c9cf46b5465c920057212e6d2499b119e05c4c1dedd2f0cfecb89eea7**. Its installed managed repeatable closure SHA-256 is **327a0e4169fd5c9cc66f1bad209ee796ddf07dcc6c07d995cd3a1590e3667c44**; the closure digest is distinct from the root file digest. Existing native Banking hooks are retained. Worker ABI: broker/src/payment-actor-permission.js exports isActivePaymentActor(row) and requireActivePaymentActor(env,actorUserId,{onDenied,readUser}); fresh authority fields are id,is_active,role,payment_authoriser,payment_golden_key. Exact installed Git blob SHA-256 **cca0e4fa2d6ca5627d900538ea1d9176d4ae31dc29bb775067c997f3dff70c4e**. It has no independent version constant: WP-PP v2 names the reviewed implementation; the SQL v1 signature is unchanged.

All four active100% Worker versions were inspected through Cloudflare and matched successful Git builds of f743b81fe537830d81b1de93ec4f9cb0331532a7 on their dedicated deployment branches:

| Worker | Active version | Successful build |
| --- | --- | --- |
| test-cloudtms-backend | 5501aaa3-3302-456d-8805-d5e7e82ec86a | 8c641156-38d2-4104-b663-57a38e925d1f |
| test-cloudtms-candidate-private-api | 0ed515b5-6da0-48cd-a7dd-c2b3e1468adc | c0f100d0-043a-4a83-bee9-6666b463830e |
| test-cloudtms-candidate-synthetic-private-api | 811f47bc-5492-4ea7-860c-57648be4bbdb | 750cd396-5a2d-44bb-a177-ca4a95932c56 |
| test-cloudtms-candidate-broker | e4aa235c-8527-486e-b35b-7cfc800b3b95 | 94ade13d-1cfe-4ba1-806f-e6a3d2ed1d69 |

Office followed the public broker: source8ea8c8af0435702d3bf032b3197fe0e0eda64d1c, Pages built16:06:22UTC; coordinator recorded completion16:06:33.035UTC. The five served index/review/workspace/Checklist/CSS files exactly match their pinned Git blob bytes. Shared turn(20261013,1), our PID18462, held14:44:41.796–16:06:33.336UTC; explicit unlock and connection close confirmed, then fresh PID absence proved16:08:56UTC. TEST postmaster start09:07:33.240096 remained unchanged. Normal PostgREST/gateway/readyz are healthy; no Hyperdrive refresh or background-job pause occurred.

Genuine deployed HTTP first use: authenticated Admin GET workspace200 with the expected workspace contract, anonymous GET401. The final bounded browser allowed only GET/HEAD/OPTIONS plus the exact existing security POST /auth/refresh; no business write endpoint. Its one normal refresh succeeded. Native registered verifier supplies the actor/operation/read/ACL matrix; the browser observation does not claim every actor combination or financial/mutation journey. A preceding over-restrictive observation blocked normal security refresh and failed in the harness; preserved as such, then corrected without changing application source. No authentication bypass, password disclosure or account/Worker secret change occurred.

Exact receipts, sources, event list and file SHA-256 manifest: **docs/healthroster-build/install1-completed-20261014/PUBLICATION_MANIFEST.json** and its sibling files. Status: **DEPLOYED_ACCEPTANCE_PENDING**, featuresOFF. Banking HR-06 may consume this installed compatible permission ABI and exact definition/version receipt. Full HealthRoster financial, security, UI, TestFlight/phone and PartB qualification are still in progress. No Banking chat message was sent.

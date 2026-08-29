# Candidate Daily Phase 3 R12 current state

Date: 17 August 2026

Environment: TEST only

## Disposition

The corrected source is published and installed in both TEST Google Apps Script projects. Both active web-app deployments use the corrected source, and both retain the immediately preceding deployment version for rollback.

The bridge remains disabled. Installation/versioning is complete; feature activation and signed route proving have not begun.

## Current authorities

| Surface | Current authority |
| --- | --- |
| Backend `test` source/test head | `4e5e2a9f7667d2866f5061ae75eadc008ea0cc26` |
| Availability API active version | 216; version 215 retained |
| NEW MASTER ROTA active version | 102; version 101 retained |
| Public Candidate broker secret-change version | `19199ecd-aa2a-40b5-b415-296d4cd25f21` |
| Private Candidate secret-change version | `f697b9e3-2f9c-4e67-bea9-951651c670a9` |
| Public Candidate health/readiness | 200 / 200 |
| Candidate feature state | disabled |
| Candidate business-data change | none |
| Supabase definition change | none |
| Worker source deployment in R12 | none |
| Production action | none |

## Disabled behavior

When `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` is missing, blank or false, both helpers return before identity derivation, Script Lock, bridge state, bridge log or network work. Existing legacy responses and Sheet/cache behavior remain the authority.

No new Daily tile appears in the legacy Availability browser merely because the source is installed. No CloudTMS Daily row is created and no broker route is called while disabled.

## Enabled behavior awaiting separate approval

When the bridge is later set true for one approved TEST cohort:

- Availability tile reads retain the complete legacy envelope and overlay only canonical by-date facts from the TEST Candidate broker;
- Availability writes complete in Google first and mirror only the durable accepted subset;
- NEW MASTER ROTA continues its existing Availability publication first, then publishes an exact fourteen-day hashed generation only after the legacy publication is accepted;
- transport uncertainty retains one exact operation and uses status-first recovery;
- projection delivery preserves booked and system-blocked legacy overlays;
- Emergency and retained specialist provider actions remain unchanged until Phase 6.

## Source hashes

| Project/file | SHA-256 |
| --- | --- |
| Availability `Code.gs` | `01a737620116b387776d1bcb24992abecb9cdde523f09a936c587b17a7b55309` |
| Availability helper | `2a44bce4dd72613178370c342e8dcfc45f13bdaedf16ef29b828b6a64c079bdf` |
| Availability rollback | `eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f` |
| Master complete `Code.gs` | `e0d4ac48bffd4e9e79b1c0439cb952a298a2d11f90e3caf2a53df4b9d779d700` |
| Master helper | `58e8da3948f2890b42abd802485776169ff500dedcf060d27b449a60597bcb2c` |
| Master rollback | `c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8` |

## Remaining gate

Independent R12 operation-level review must confirm the complete pack. Only after GO may one approved TEST cohort be enabled for signed transport, generation, recovery, projection and coexistence proving. All Candidate public flags and entitlements remain false until later cutover gates.

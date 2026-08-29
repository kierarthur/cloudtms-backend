# Candidate Daily Phase 3 R13 current state

Date: 17 August 2026

Environment: TEST only

## Disposition

The R13 Master Rota durability/quota correction is implemented, tested and published in backend source. The exact helper is saved in the editable head of NEW MASTER ROTA but deliberately not deployed pending independent GO.

Both Google bridges remain disabled. Candidate feature flags and entitlements remain disabled. No signed route or Candidate mutation has occurred.

## Current authority

| Surface | Current fact |
| --- | --- |
| Backend `test` runtime implementation | `ccf193eb49d4e022a971d845ce77120f53cd6bb8` |
| R13 evidence commit | `6a62cf9864cdb41a63b8516f81540b0fa434a41c` |
| Availability active/rollback | 216 / 215; unchanged by R13 |
| Master active/rollback | 102 / 101; R13 helper saved to Head only |
| Master R13 Google deployment | Paused pending independent GO |
| Bridge property | `false` in both projects |
| Worker runtime change/deployment | None |
| Supabase definition/data change | None |
| Candidate public feature activation | None |
| Production action | None |

## Read-only identity qualification

TEST contains one active Kier Arthur candidate record. Its existing non-empty global Candidate key matches the exact value supplied by the product owner. That proves Kier's established Candidate-product mapping.

The Phase 2/3 Google Daily resolver deliberately does not resolve from that reversible/global key. It resolves only from a separate non-reversible `GOOGLE_CREDENTIALLY_PUBLIC_ID` HMAC in `private.candidate_daily_source_links`. The current read-only audit records zero such rows for Kier and zero such active TEST rows overall. The first enabled journey is therefore blocked on controlled source-link bootstrap even after code GO/deployment. Existing global-key presence and Daily source-link readiness must not be conflated.

Kier is an observational first journey, not a hard-coded runtime restriction.

This onboarding authority also applies after the legacy browser is retired. Google normalizes `Public ID - Credentially` and generates the `CID1-...` Candidate_ID (Crockford Base32 payload plus keyed checksum). A CloudTMS administrator enters that exact generated value on the canonical Candidate record; they do not enter the raw Credentially ID. Controlled onboarding then attaches the separate derived Google source HMAC to that same existing Candidate UUID. It must not require prior legacy-app use or create a second Candidate row.

## Source hashes

| Project/file | SHA-256 |
| --- | --- |
| Availability `Code.gs` | `01a737620116b387776d1bcb24992abecb9cdde523f09a936c587b17a7b55309` |
| Availability helper | `2a44bce4dd72613178370c342e8dcfc45f13bdaedf16ef29b828b6a64c079bdf` |
| Availability rollback | `eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f` |
| Master complete `Code.gs` | `e0d4ac48bffd4e9e79b1c0439cb952a298a2d11f90e3caf2a53df4b9d779d700` |
| Master R13 helper | `141538b2c7d3b9719963484d057fafbfe733fd150712a50a5b9fb673fdc3783f` |
| Master rollback | `c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8` |

## Next gate

Independent review must first issue GO. Only then may the saved Master Head be deployed as a new version while the flag remains false. A controlled bootstrap must derive each exact source HMAC from the existing Google Credentially public ID and bind it to the already-established CloudTMS Candidate row without exposing either raw identity. It must be proved for both a legacy-coexistence Candidate and a new-app-only Candidate. Source-link verification and an independently authorized enabled TEST window follow as separate steps.

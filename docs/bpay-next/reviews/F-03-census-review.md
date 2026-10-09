# F-03 writer-census source review

PASS, 9 October 2026. Independent source inspection only; no SQL, tests, runtime or hosted reads.

Compared saved BUILD-BE `supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql` against fixed fresh baseline `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. Exactly two changes exist: Twelve becomes Thirteen in the fixed-reader comment, and one exact private-reader acknowledgement is added. Removing that row and restoring the comment reproduces the baseline LF text byte-for-byte. Scanner/refusal logic and every other acknowledgement remain unchanged.

The new row's complete schema, name and named identity arguments match `supabase/release/current-contract.json`, selected by `supabase/release/current-release.json`. Its sealed catalogue definition SHA-256 is `9266c9d46ebf05d5b4684794991c53ab38c5f262bcc0bbd12765c6274a0e59e3`. This is a PostgreSQL routine-definition hash, not the SQL source-file raw hash. Catalogue metadata remains STABLE, SECURITY INVOKER, `search_path=pg_catalog, private`, owner postgres, and owner-only EXECUTE ACL.

Saved verifier raw SHA-256: `98ec5e5273064aec702c00e9b9d1d3ab977da794130477fde2bad1ae0753376f`. Baseline Git blob `a8d27fd74898a23d1c6065845a89315dc047fbd0`; saved LF Git blob `6b62e1f3db88f88405198f548fe77eff820a1912`. Saved catalogue raw SHA-256: `2e8c1a727c022b403fb7663ca1c5724bf28b262f462de7c3e079cfd7abce12e2`. Catalogue equality confirms the precise acknowledgement; it does not claim a fresh installed-body check.

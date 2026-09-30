# Weekly Source policy successor — 30 September 2026

Editable successor of the accepted `CloudTMS_Weekly_Source_HANDOVER_2026-09-19.zip` (SHA-256 `a062d8a7baaee282937acc4316dc9c3945e5685e00ec6adf624537e3e502f3bb`), governed by [the dated amendment](../WEEKLY_SOURCE_IMPORT_QUERY_POLICY_AMENDMENT_20260930.md). The accepted archive remains unchanged.

`04_MODAL_POLICY.json` is policy version 6.3. The manager-email policy is retained unchanged. The original policy-owned renderer is retained with generic support for the added Office checks section, per-screen context, wider desktop layout and stacked actions. Existing renderer assertions remain enabled, with the explicitly renamed missing-timesheet reminder assertion updated.

The three committed PNGs are **policy mockups using example records**, not screenshots of deployed TEST or proof of delivery. All 33 policy images are regenerated locally; unchanged images and intermediate HTML remain untracked. The Office application's local Playwright screenshots are separate runtime-layout evidence.

Run `node docs/weekly-source/policy-successor-20260930/rendering/render_modal_mockups.cjs` from the backend checkout with `CLOUDTMS_PLAYWRIGHT_MODULE` pointing to the installed Playwright module and `CLOUDTMS_MODAL_TEMP_DIR` pointing to this directory's `rendering/generated-html`. A local Chrome executable can be selected using `CLOUDTMS_CHROME`.

The dated textual policy amendment preceded the application layout changes. This consolidated successor and its regenerated images were completed during the pre-release review; they are not claimed to have been rendered before those local code edits.

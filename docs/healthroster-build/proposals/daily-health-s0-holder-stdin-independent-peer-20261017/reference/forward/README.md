# Owned stdin disposal correction

Root's actual catalogue capture succeeded under the genuine A turn and the strict holder transport predicate. A explicitly unlocked/closed at 10:28:04.556Z with proved=true. The holder then stayed alive, so root correctly retained its outer `S0_OWN_RELEASE_NOT_CONFIRMED` orchestration HOLD and closed only its own TTY. This forward does not rewrite that outcome or repeat the native capture.

The original `Controls.dispose()` removed listeners but left its resumed input flowing. The corrected method adds exactly `this.input.pause()` after removing those same listeners. The existing holder's work `finally` already calls disposal. No holder body, query, native guard, source parent, clock, role check, budget, ownership, unlock or close predicate changes.

Two selected target images are in `FULL_PARENT_FORWARD.json`: `protocol.mjs` plus the enclosing holder `MANIFEST.json`'s single protocol row. All16 other manifest rows and every original saved file remain unchanged until root explicitly authorises mounting. Complete original protocol/manifest images are retained under `reference/`, byte-equal to 474d819407e7fbf2c2c815d306c584fb30c65811. The full raw inverses are independently executable.

```powershell
node --test docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-stdin-forward-20261017/stdin.test.mjs
node docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-stdin-forward-20261017/verify.mjs
```

Five checks pass. The real negative child runs the exact original class, receives RELEASE with its parent input pipe deliberately kept open, disposes, and remains alive; the test then cleans up only that owned child. The exact corrected class exits normally after RELEASE without EOF or forced kill. Its ABORT/failure path also exits with the expected closed nonzero status without EOF or forced kill. A controlled stream vector proves only the owned input is paused. Neither child runs a holder, browser, auth loader, IPC pipe, database or native client. No actual state or credentials are read.

After independent peer/root review and a separately authorised two-target Report mount, use `verify.mjs --mounted` to verify the selected forward images against saved/index/HEAD and all retained original images against474d. This is a source/runtime cleanup correction only. Actual native capture is already complete; no retry is needed for this correction. Reuse still requires root's exact reviewed setup and original shared-turn admission.

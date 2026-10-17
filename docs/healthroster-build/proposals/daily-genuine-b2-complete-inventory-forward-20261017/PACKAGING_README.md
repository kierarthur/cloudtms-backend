# Complete source packaging successor

This is a packaging-only successor to the unchanged de17 frozen packet. Its manifest generator and verifier now exclude only the root `MANIFEST.json`, rather than every file with that basename. All eighteen omitted nested parent/reference manifests are included and individually bound to the original PARENTS rows. Payload manifests remain old metadata references, never mountable resealed metadata.

The complete base contains 152 original non-cache files plus the retained original manifest at `history/de17-MANIFEST.json` (153 files before this forward's evidence). Only `freeze.py` and `verify.py` change, each through one literal exclusion-predicate replacement recorded with full originals in `PACKAGING_FORWARD.json`. The eighteen functional postimages, all76 inverse hunks, source-chain map, runtime modules, tests and original results remain byte-exact.

Safe replay:

```powershell
python packaging_verify.py
python verify.py --current
node --experimental-vm-modules --test test.mjs
```

`--current` additionally requires the original current saved/index/historical parents and additive vacancies, and truthfully refuses after integration. Do not run generators on a frozen packet. The root manifest now seals every nested manifest. The old de17 packet and its original source-scoped peer verdicts remain unchanged; adoption requires this complete inventory. There is no runtime/native/auth/private/source-admission change.

"""Add the V5 current entry without rewriting historical approval evidence."""
from pathlib import Path
import hashlib
import json

repo = Path(__file__).resolve().parents[1]
workspace = repo.parents[1]
base = workspace / "cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925"
active = repo / "docs/healthroster-build/v5-amendment-20261015"
plan = base / "HEALTHROSTER_PLAN_V2_CONSOLIDATION_13102026/HEALTHROSTER_SELFBILL_IMPLEMENTATION_PLAN_V2"
body = (active / "ACTIVE_PLAN.md").read_bytes()
target = plan / "11_V5_CURRENT_IMPLEMENTATION_ADDENDUM.md"
if target.exists() and target.read_bytes() != body:
    raise SystemExit("Preserve existing additive amendment; review its change first")
target.write_bytes(body)
note = """## V5 owner amendment — active 15 October 2026

The current plan is docs/healthroster-build/v5-amendment-20261015/ACTIVE_PLAN.md, with all326 Health mappings,100 Banking dependency mappings and the contact/shared-native schema interfaces retained in its exact authority capture and working registers. Original approved v2 files/rounds1–22 and historical ticks are preserved; an additive11_V5_CURRENT_IMPLEMENTATION_ADDENDUM.md joins the original plan. Source provenance/immutable Source invoice preservation, whole move/inherited evidence, canonical pay-only decision/ASTRA09 native acceptance, report/checking/contact pause and agency-local quiet-push amendments reopen only affected claims. All V5 native acceptance is REQUIRED_NOT_RUN. The preimplementation review/explanation was delivered before affected source work resumed.

Install1 remains COMPLETE at backendf743b81fe537830d81b1de93ec4f9cb0331532a7 / Office8ea8c8af0435702d3bf032b3197fe0e0eda64d1c,158 mandatory verifiers; featuresOFF. Install2 remains NOT_INSTALLED and is the one remaining normal install. Current canonical source checkpoint748a5d6b910313a5a7e43ca8c4bc9d473ae7c654 contains product9d2873bd9180acd8588a44a6223d70bcf6403310 plus diagnostic evidence, not V5 qualification. The earlier242-function predecessor recipe is not a completed V5 closure. Current implementations must preserve contemporary Money820/Report4822/Native5b and actual saved owners; old audit baselines cannot overwrite them.

Native Kier1978 rollback works in this chat; no connector/configuration repair is pending. Exact full-rehearsal route/envelope and isolation disposition, final native/source/ACL/registry/PG17 contract, protected database-first ordered Install2, eligible activation, genuine acceptance, TestFlight/phone and both final implementation PartB critics remain required. Normal background jobs stayON. No unanswered owner decision stops independent implementation. NEXT-dependent acceptance waits BankingF17; run everything else eligible first.

Estimate replaces older estimates: V5 requires several engineering hours per source/native area, then roughly1–3hours per clean rehearsal/ordered release cycle plus reviewed repair rounds, real app/device and BankingF17 dependencies. No guaranteed completion time is claimed. No new per-package releases or automatic22October wait. No V5 deployment, real external effect, secret change, Banking message or cleanup accompanies this record. Historical paragraphs below retain their dated scope and must not override this current entry.

"""
for relative in ["PROGRESS.md", "TEST_INSTALL_PLAN.md", "TEST_INSTALLS.md", "PUBLISHED_HELPERS.md"]:
    path = repo / "docs/healthroster-build" / relative
    before = path.read_bytes()
    marker = note.splitlines()[0].encode()
    if marker in before:
        continue
    split = before.find(b"\n") + 1
    path.write_bytes(before[:split] + b"\n" + note.encode() + before[split:])

manifest = []
for path in sorted(active.rglob("*")):
    if path.is_file() and path.name != "SHA256_MANIFEST.json":
        data = path.read_bytes()
        manifest.append({"path": path.relative_to(active).as_posix(), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
(active / "SHA256_MANIFEST.json").write_text(json.dumps({"scope": "V5 amendment source/plan capture, no installed or acceptance claim", "files": manifest}, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"original_plan_addendum": str(target), "authority_and_plan_files": len(manifest), "live_records_updated": 4, "historical_files_rewritten": False}))

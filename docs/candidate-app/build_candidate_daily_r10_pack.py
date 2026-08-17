#!/usr/bin/env python3
"""Build the compact, self-contained Candidate Daily R10 audit handover."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import zipfile
from pathlib import Path


REVIEW_ENTRIES = [
    "CloudTMS_Candidate_App_Phase2_Phase1B_R9_Independent_Verification_20260817.md",
    "CloudTMS_Candidate_App_Phase2_Phase1B_R9_Issue_Register_20260817.csv",
    "CloudTMS_Candidate_App_Phase2_Phase1B_R9_Final_Verdict_20260817.json",
    "CloudTMS_Candidate_App_Phase2_Phase1B_R9_Evidence_Digest_20260817.json",
]


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_file(source: Path, root: Path, destination: str) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    target = root / destination
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--r9-pack", type=Path, required=True)
    parser.add_argument("--r9-decisions", type=Path, required=True)
    parser.add_argument("--r9-review-archive", type=Path, required=True)
    parser.add_argument("--r10-decisions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    repo = args.repo_root.resolve()
    docs = repo / "docs" / "candidate-app"
    mappings = {
        docs / "R10_HANDOVER.md": "00_HANDOVER.md",
        docs / "R10_INDEPENDENT_REVIEW_BRIEF.md": "01_INDEPENDENT_REVIEW_BRIEF.md",
        docs / "R10_CURRENT_STATE.md": "02_CURRENT_STATE.md",
        docs / "R10_VERIFICATION_SUMMARY.md": "03_VERIFICATION_SUMMARY.md",
        docs / "R10_FINDING_CLOSURE_MATRIX.md": "04_FINDING_CLOSURE_MATRIX.md",
        docs / "AUTHORITY_MAP.md": "decisions/AUTHORITY_MAP.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md",
        docs / "IMPLEMENTATION_PLAN.md": "decisions/IMPLEMENTATION_PLAN.md",
        repo / "supabase" / "repeatable" / "17082026_0015_candidate_daily_phase2_rpcs_v1.sql":
            "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
        repo / ".github" / "workflows" / "candidate-db-runtime.yml":
            "source/.github/workflows/candidate-db-runtime.yml",
        repo / "tests" / "17082026_0955_candidate_daily_authority_transition_runtime_verification.sql":
            "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql",
        repo / "tests" / "candidate-daily-authority-transition-concurrency.integration.js":
            "tests/candidate-daily-authority-transition-concurrency.integration.js",
        repo / "tests" / "candidate-daily-phase2-source-contract.test.js":
            "tests/candidate-daily-phase2-source-contract.test.js",
        docs / "17082026_1222_candidate_daily_r10_readonly_verification.sql":
            "tools/17082026_1222_candidate_daily_r10_readonly_verification.sql",
        docs / "build_candidate_daily_r10_decisions_pdf.py":
            "tools/build_candidate_daily_r10_decisions_pdf.py",
        docs / "build_candidate_daily_r10_pack.py": "tools/build_candidate_daily_r10_pack.py",
        docs / "validate_candidate_daily_r10_pack.py": "validate_candidate_daily_r10_pack.py",
        docs / "R10_PROVENANCE.json": "PROVENANCE.json",
        args.r9_pack.resolve(): "baseline_r9/CloudTMS_Candidate_App_Phase2_Phase1B_R9_Handover_20260817.zip",
        args.r9_decisions.resolve():
            "baseline_r9/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R9.pdf",
        args.r10_decisions.resolve():
            "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R10.pdf",
    }

    with tempfile.TemporaryDirectory(prefix="candidate-r10-pack-") as temporary:
        root = Path(temporary)
        for source, destination in mappings.items():
            copy_file(source, root, destination)

        with zipfile.ZipFile(args.r9_review_archive.resolve(), "r") as review:
            for name in REVIEW_ENTRIES:
                target = root / "incoming_audit" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(review.read(name))

        for evidence in sorted(args.evidence_dir.resolve().iterdir()):
            if evidence.is_file():
                copy_file(evidence, root, f"evidence/{evidence.name}")

        payloads = sorted(
            path for path in root.rglob("*")
            if path.is_file() and path.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
        )
        sha_lines = []
        size_lines = []
        for path in payloads:
            relative = path.relative_to(root).as_posix()
            sha_lines.append(f"{digest(path)}  {relative}")
            size_lines.append(f"{path.stat().st_size}  {relative}")
        (root / "MANIFEST.sha256").write_text("\n".join(sha_lines) + "\n", encoding="utf-8")
        (root / "MANIFEST.sizes").write_text("\n".join(size_lines) + "\n", encoding="utf-8")

        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            output.unlink()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for path in sorted(root.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(root).as_posix())

    result = {
        "output": str(args.output.resolve()),
        "size": args.output.resolve().stat().st_size,
        "sha256": digest(args.output.resolve()),
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build the bounded, self-contained Candidate Daily R9 review pack."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import zipfile
from pathlib import Path


DOC_MAP = {
    "docs/candidate-app/R9_HANDOVER.md": "00_HANDOVER.md",
    "docs/candidate-app/R9_INDEPENDENT_REVIEW_BRIEF.md": "01_INDEPENDENT_REVIEW_BRIEF.md",
    "docs/candidate-app/R9_CURRENT_STATE.md": "02_CURRENT_STATE.md",
    "docs/candidate-app/R9_VERIFICATION_SUMMARY.md": "03_VERIFICATION_SUMMARY.md",
    "docs/candidate-app/R9_FINDING_CLOSURE_MATRIX.md": "04_FINDING_CLOSURE_MATRIX.md",
    "docs/candidate-app/R9_PROVENANCE.json": "PROVENANCE.json",
    "docs/candidate-app/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R9.pdf":
        "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R9.pdf",
    "docs/candidate-app/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R8.pdf":
        "baseline_r8/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R8.pdf",
    "docs/candidate-app/AUTHORITY_MAP.md": "decisions/AUTHORITY_MAP.md",
    "docs/candidate-app/IMPLEMENTATION_PLAN.md": "decisions/IMPLEMENTATION_PLAN.md",
    "docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md":
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md",
    "docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md":
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md",
    "docs/candidate-app/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md":
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md",
    "docs/candidate-app/build_candidate_daily_r9_decisions_pdf.py":
        "tools/build_candidate_daily_r9_decisions_pdf.py",
    "docs/candidate-app/validate_candidate_daily_r9_pack.py": "validate_candidate_daily_r9_pack.py",
    "supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql":
        "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
    ".github/workflows/candidate-db-runtime.yml": "source/.github/workflows/candidate-db-runtime.yml",
    "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql":
        "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql",
    "tests/candidate-daily-authority-transition-concurrency.integration.js":
        "tests/candidate-daily-authority-transition-concurrency.integration.js",
    "tests/candidate-daily-phase2-source-contract.test.js":
        "tests/candidate-daily-phase2-source-contract.test.js",
}


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def copy_file(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--r8-handover", type=Path, required=True)
    parser.add_argument("--r8-audit", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()

    repo = args.repo.resolve()
    staging = args.staging.resolve()
    output = args.output.resolve()
    if staging.exists():
        raise FileExistsError(f"staging path must not already exist: {staging}")
    if output.exists():
        raise FileExistsError(f"output ZIP must not already exist: {output}")
    if not args.evidence.is_dir():
        raise FileNotFoundError(args.evidence)

    staging.mkdir(parents=True)
    for relative_source, relative_destination in DOC_MAP.items():
        copy_file(repo / relative_source, staging / relative_destination)

    copy_file(
        args.r8_handover.resolve(),
        staging / "baseline_r8/CloudTMS_Candidate_App_Phase2_Phase1B_R8_Handover_20260817.zip",
    )
    copy_file(
        args.r8_audit.resolve(),
        staging / "incoming_audit/CloudTMS_Candidate_App_Phase2_Phase1B_R8_Independent_Review_Artifacts_20260817.zip",
    )
    with zipfile.ZipFile(args.r8_audit.resolve(), "r") as audit_archive:
        review_names = [
            name for name in audit_archive.namelist()
            if name.endswith("_Independent_Verification_20260817.md")
        ]
        if len(review_names) != 1:
            raise RuntimeError(f"expected one independent R8 review Markdown; found {review_names}")
        review_target = staging / "incoming_audit/INDEPENDENT_R8_REVIEW.md"
        review_target.parent.mkdir(parents=True, exist_ok=True)
        review_target.write_bytes(audit_archive.read(review_names[0]))
    for path in sorted(args.evidence.rglob("*")):
        if path.is_file():
            copy_file(path, staging / "evidence" / path.relative_to(args.evidence))

    files = sorted(
        p for p in staging.rglob("*")
        if p.is_file() and p.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
    )
    (staging / "MANIFEST.sha256").write_text(
        "".join(f"{digest(path)}  {path.relative_to(staging).as_posix()}\n" for path in files),
        encoding="utf-8",
    )
    (staging / "MANIFEST.sizes").write_text(
        "".join(f"{path.stat().st_size}\t{path.relative_to(staging).as_posix()}\n" for path in files),
        encoding="utf-8",
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(p for p in staging.rglob("*") if p.is_file()):
            archive.write(path, path.relative_to(staging).as_posix())
    print(f"R9_PACK_BUILT|payloads={len(files)}|zip={output.name}|sha256={digest(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

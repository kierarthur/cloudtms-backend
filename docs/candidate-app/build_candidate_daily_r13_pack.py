#!/usr/bin/env python3
"""Build the self-contained Candidate Daily Phase 3 R13 audit handover."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import zipfile
from pathlib import Path


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def copy_file(source: Path, root: Path, destination: str) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    target = root / destination
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--r12-review-archive", type=Path, required=True)
    parser.add_argument("--r12-review-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    repo = args.repo_root.resolve()
    docs = repo / "docs" / "candidate-app"
    phase3 = docs / "phase3-apps-script"
    mappings = {
        docs / "R13_HANDOVER.md": "00_HANDOVER.md",
        docs / "R13_INDEPENDENT_REVIEW_BRIEF.md": "01_INDEPENDENT_REVIEW_BRIEF.md",
        docs / "R13_CURRENT_STATE.md": "02_CURRENT_STATE.md",
        docs / "R13_VERIFICATION_SUMMARY.md": "03_VERIFICATION_SUMMARY.md",
        docs / "R13_PROVENANCE.json": "PROVENANCE.json",
        docs / "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R13.pdf":
            "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R13.pdf",
        docs / "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R12.pdf":
            "accepted_r12/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R12.pdf",
        docs / "CloudTMS_Candidate_App_Phase3_R12_Handover_20260817.zip":
            "accepted_r12/CloudTMS_Candidate_App_Phase3_R12_Handover_20260817.zip",
        docs / "R12_HANDOVER.md": "accepted_r12/R12_HANDOVER.md",
        docs / "R12_CURRENT_STATE.md": "accepted_r12/R12_CURRENT_STATE.md",
        docs / "R12_VERIFICATION_SUMMARY.md": "accepted_r12/R12_VERIFICATION_SUMMARY.md",
        docs / "R12_INDEPENDENT_REVIEW_BRIEF.md": "accepted_r12/R12_INDEPENDENT_REVIEW_BRIEF.md",
        docs / "R12_PROVENANCE.json": "accepted_r12/R12_PROVENANCE.json",
        docs / "AUTHORITY_MAP.md": "decisions/AUTHORITY_MAP.md",
        docs / "BACKEND_API_CONTRACT.md": "decisions/BACKEND_API_CONTRACT.md",
        docs / "IMPLEMENTATION_PLAN.md": "decisions/IMPLEMENTATION_PLAN.md",
        docs / "GOOGLE_EVIDENCE_GATE_20260816.md": "decisions/GOOGLE_EVIDENCE_GATE_20260816.md",
        docs / "CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md":
            "decisions/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE3_IMPLEMENTATION_AUTHORITY.md":
            "decisions/CANDIDATE_DAILY_PHASE3_IMPLEMENTATION_AUTHORITY.md",
        docs / "CANDIDATE_DAILY_PHASE3_DECISION_COMPLIANCE_MATRIX.md":
            "decisions/CANDIDATE_DAILY_PHASE3_DECISION_COMPLIANCE_MATRIX.md",
        docs / "CANDIDATE_DAILY_PHASE3_INSTALLATION_RUNBOOK.md":
            "runbooks/CANDIDATE_DAILY_PHASE3_INSTALLATION_RUNBOOK.md",
        docs / "CANDIDATE_DAILY_PHASE3_DIAGNOSTIC_AND_ROLLBACK.md":
            "runbooks/CANDIDATE_DAILY_PHASE3_DIAGNOSTIC_AND_ROLLBACK.md",
        docs / "CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml":
            "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml",
        phase3 / "README.md": "source/README.md",
        phase3 / "SCRIPT_PROPERTIES.md": "source/SCRIPT_PROPERTIES.md",
        phase3 / "availability-api" / "Code.gs": "source/availability-api/Code.gs",
        phase3 / "availability-api" / "CloudTMSCandidateBridge.gs":
            "source/availability-api/CloudTMSCandidateBridge.gs",
        phase3 / "availability-api" / "rollback" / "Code.gs":
            "source/availability-api/rollback/Code.gs",
        phase3 / "master-rota" / "Code.gs": "source/master-rota/Code.gs",
        phase3 / "master-rota" / "CloudTMSCandidateBridge.gs":
            "source/master-rota/CloudTMSCandidateBridge.gs",
        phase3 / "master-rota" / "rollback" / "Code.gs":
            "source/master-rota/rollback/Code.gs",
        repo / "candidate-broker" / "src" / "index.js": "runtime/candidate-broker/src/index.js",
        repo / "candidate-broker" / "src" / "candidate-broker.js":
            "runtime/candidate-broker/src/candidate-broker.js",
        repo / "candidate-broker" / "wrangler.jsonc": "runtime/candidate-broker/wrangler.jsonc",
        repo / "candidate-private-api" / "wrangler.jsonc": "runtime/candidate-private-api/wrangler.jsonc",
        repo / "broker" / "src" / "candidate-private-worker.js":
            "runtime/shared/candidate-private-worker.js",
        repo / "broker" / "src" / "candidate-app-backend.js":
            "runtime/shared/candidate-app-backend.js",
        repo / "broker" / "src" / "candidate-daily-contract-v1.js":
            "runtime/shared/candidate-daily-contract-v1.js",
        repo / "broker" / "src" / "candidate-daily-hmac-v1.js":
            "runtime/shared/candidate-daily-hmac-v1.js",
        repo / "broker" / "src" / "candidate-daily-phase1a.js":
            "runtime/shared/candidate-daily-phase1a.js",
        repo / "broker" / "src" / "candidate-daily-phase1b.js":
            "runtime/shared/candidate-daily-phase1b.js",
        repo / "supabase" / "repeatable" / "17082026_0015_candidate_daily_phase2_rpcs_v1.sql":
            "runtime/database/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
        repo / "tests" / "candidate-daily-phase1a-contract.test.js":
            "tests/candidate-daily-phase1a-contract.test.js",
        repo / "tests" / "candidate-daily-phase1b-contract.test.js":
            "tests/candidate-daily-phase1b-contract.test.js",
        repo / "tests" / "candidate-daily-phase2-source-contract.test.js":
            "tests/candidate-daily-phase2-source-contract.test.js",
        repo / "tests" / "candidate-daily-phase3-apps-script.test.js":
            "tests/candidate-daily-phase3-apps-script.test.js",
        repo / "tests" / "candidate-daily-phase3-r13-master-recovery.test.js":
            "tests/candidate-daily-phase3-r13-master-recovery.test.js",
        repo / "tests" / "08082026_1522_candidate_route_daily_runtime_verification.sql":
            "tests/sql/08082026_1522_candidate_route_daily_runtime_verification.sql",
        repo / "tests" / "17082026_0053_candidate_daily_phase2_runtime_verification.sql":
            "tests/sql/17082026_0053_candidate_daily_phase2_runtime_verification.sql",
        repo / "tests" / "17082026_0955_candidate_daily_authority_transition_runtime_verification.sql":
            "tests/sql/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql",
        repo / "tests" / "fixtures" / "candidate-daily-r5" / "canonicalization-v1-vectors.json":
            "fixtures/candidate-daily-r5/canonicalization-v1-vectors.json",
        repo / "tests" / "fixtures" / "candidate-daily-r5" / "r5-operation-error-matrix.json":
            "fixtures/candidate-daily-r5/r5-operation-error-matrix.json",
        repo / "package.json": "package.json",
        docs / "build_candidate_daily_r13_decisions_pdf.py":
            "tools/build_candidate_daily_r13_decisions_pdf.py",
        docs / "build_candidate_daily_r13_evidence.py": "tools/build_candidate_daily_r13_evidence.py",
        docs / "probe_candidate_daily_r13_broker.py": "tools/probe_candidate_daily_r13_broker.py",
        docs / "build_candidate_daily_r13_pack.py": "tools/build_candidate_daily_r13_pack.py",
        docs / "validate_candidate_daily_r13_pack.py": "validate_candidate_daily_r13_pack.py",
    }

    with tempfile.TemporaryDirectory(prefix="candidate-r13-pack-") as temporary:
        root = Path(temporary)
        for source, destination in mappings.items():
            copy_file(source, root, destination)

        copy_file(
            args.r12_review_archive.resolve(), root,
            "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Review_Artifacts_20260817.zip",
        )
        copy_file(
            args.r12_review_report.resolve(), root,
            "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Verification_20260817.md",
        )

        evidence = docs / "r13-evidence"
        for path in sorted(evidence.iterdir()):
            if path.is_file():
                copy_file(path, root, f"evidence/{path.name}")

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

    print(json.dumps({
        "output_name": args.output.name,
        "size": args.output.resolve().stat().st_size,
        "sha256": digest(args.output.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

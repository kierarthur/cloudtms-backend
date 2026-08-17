#!/usr/bin/env python3
"""Generate bounded, non-secret Candidate Daily Phase 3 R13 evidence."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path


FOCUSED = [
    "tests/candidate-daily-phase1a-contract.test.js",
    "tests/candidate-daily-phase1b-contract.test.js",
    "tests/candidate-daily-phase2-source-contract.test.js",
    "tests/candidate-daily-phase3-apps-script.test.js",
    "tests/candidate-daily-phase3-r13-master-recovery.test.js",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(repo: Path, command: list[str], output: Path, timeout: int = 900) -> None:
    result = subprocess.run(
        command,
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    rendered = result.stdout + ("\n" if result.stdout and result.stderr else "") + result.stderr
    output.write_text(rendered, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}")


def smoke(url: str) -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"User-Agent": "CloudTMS-R13-evidence/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read(4096).decode("utf-8", "replace")
        return response.status, body


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    repo = args.repo_root.resolve()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)

    node = shutil.which("node") or "node"
    npm = shutil.which("npm.cmd") or shutil.which("npm") or "npm"
    npx = shutil.which("npx.cmd") or shutil.which("npx") or "npx"

    run(repo, [node, "--test", *FOCUSED], output / "focused-candidate-daily.tap")
    run(repo, [npm, "test"], output / "complete-backend.tap", timeout=1200)

    with tempfile.TemporaryDirectory(prefix="candidate-r13-dry-build-") as temporary:
        temporary_path = Path(temporary)
        run(
            repo,
            [npx, "wrangler", "deploy", "--dry-run", "--config", "candidate-broker/wrangler.jsonc",
             "--outdir", str(temporary_path / "broker")],
            output / "candidate-broker-dry-run.txt",
        )
        run(
            repo,
            [npx, "wrangler", "deploy", "--dry-run", "--config", "candidate-private-api/wrangler.jsonc",
             "--outdir", str(temporary_path / "private")],
            output / "candidate-private-api-dry-run.txt",
        )

    sources = {
        "availability-api/Code.gs": repo / "docs/candidate-app/phase3-apps-script/availability-api/Code.gs",
        "availability-api/CloudTMSCandidateBridge.gs": repo / "docs/candidate-app/phase3-apps-script/availability-api/CloudTMSCandidateBridge.gs",
        "availability-api/rollback/Code.gs": repo / "docs/candidate-app/phase3-apps-script/availability-api/rollback/Code.gs",
        "master-rota/Code.gs": repo / "docs/candidate-app/phase3-apps-script/master-rota/Code.gs",
        "master-rota/CloudTMSCandidateBridge.gs": repo / "docs/candidate-app/phase3-apps-script/master-rota/CloudTMSCandidateBridge.gs",
        "master-rota/rollback/Code.gs": repo / "docs/candidate-app/phase3-apps-script/master-rota/rollback/Code.gs",
    }
    (output / "source-authority.sha256").write_text(
        "".join(f"{sha256(path)}  {name}\n" for name, path in sources.items()),
        encoding="utf-8",
    )

    health_status, health_body = smoke("https://test-cloudtms-candidate-broker.kier-88a.workers.dev/healthz")
    ready_status, ready_body = smoke("https://test-cloudtms-candidate-broker.kier-88a.workers.dev/readyz")
    (output / "worker-smoke.md").write_text(
        "# Candidate Worker smoke\n\n"
        f"- Public Candidate `/healthz`: HTTP {health_status}; body `{health_body.strip()}`.\n"
        f"- Public Candidate `/readyz`: HTTP {ready_status}; body `{ready_body.strip()}`.\n"
        "- R13 changes no Worker runtime source and performs no Worker deployment.\n"
        "- No secret, authorization header, token or candidate payload was captured.\n",
        encoding="utf-8",
    )

    print(f"R13_EVIDENCE_PASS|output={output.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Apply only the owner's 434 guarded existing-record updates.

The delivered scripts are evidence, never executed here. Validate every source
context and replacement before writing any target. Default is read-only.
"""
import argparse
import hashlib
import json
from pathlib import Path


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return digest(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pack", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args()
    pack = options.pack.resolve()
    repo = Path(__file__).resolve().parents[1]
    workspace = repo.parents[1]
    authority = pack / "NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6"
    prepared_relative = "NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6/WORKING_PREPARATION/HEALTH/PREPARED_RECORD_EDITS.json"
    manifest = json.loads((pack / "PACK_MANIFEST.json").read_text(encoding="utf-8-sig"))
    expected_content = "afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88"
    assert manifest["builder"] == "HealthRoster" and manifest["reviewed_content_sha256"] == expected_content
    required = [prepared_relative, "NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6/FINAL_REVIEW_RECORD.md"]
    entries = {item["path"]: item for item in manifest["original_source_files"]}
    for relative in required:
        data = (pack / relative).read_bytes()
        assert len(data) == entries[relative]["bytes"] and digest(data) == entries[relative]["sha256"], relative
    prepared_bytes = (pack / prepared_relative).read_bytes()
    prepared = json.loads(prepared_bytes.decode("utf-8-sig"))
    assert len(prepared["edits"]) == 434 and prepared["append_overlay_rows"] is False
    plan = repo / "docs/healthroster-build/v5-amendment-20261015"
    original_plan = workspace / "cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_PLAN_V2_CONSOLIDATION_13102026/HEALTHROSTER_SELFBILL_IMPLEMENTATION_PLAN_V2"
    allowed = {plan / "CHANGE_TRACEABILITY.json", plan / "SCHEMA_INTERFACE_REGISTER.json", plan / "ACTIVE_PLAN.md",
               original_plan / "11_V5_CURRENT_IMPLEMENTATION_ADDENDUM.md"}
    allowed.update(repo / "docs/healthroster-build" / name for name in ["PROGRESS.md", "TEST_INSTALL_PLAN.md", "TEST_INSTALLS.md", "PUBLISHED_HELPERS.md"])
    original = {}
    texts = {}
    objects = {}
    applied = []
    reused = []
    for edit in prepared["edits"]:
        path = Path(edit["target_path"]).resolve()
        assert path in allowed, (edit["id"], "outside exact approved targets")
        if path not in original:
            original[path] = path.read_bytes()
            texts[path] = original[path].decode("utf-8-sig").replace("\r\n", "\n")
        if edit["kind"] == "JSON_VALUE_REPLACE":
            obj = objects.setdefault(path, json.loads(texts[path]))
            parts = [part.replace("~1", "/").replace("~0", "~") for part in edit["json_pointer"].split("/")[1:]]
            parent = obj
            for part in parts[:-1]:
                parent = parent[int(part)] if isinstance(parent, list) else parent[part]
            key = int(parts[-1]) if isinstance(parent, list) else parts[-1]
            current = parent[key]
            if current == edit["replacement_value"]:
                reused.append(edit["id"])
                continue
            assert canonical(current) == edit["expected_before_canonical_sha256"], (edit["id"], "current named row drift")
            assert current == edit["expected_before_value"], (edit["id"], "exact before value")
            for selector, value in edit["stable_selector"].items():
                # The reviewed handover additionally carries a contract
                # annotation in some selectors. It is not an existing-row key;
                # exact before-value+hash and identity selectors still apply.
                if selector == "move_stage_contract":
                    assert value.startswith("MOVE_STAGE_CONTRACT.md")
                    continue
                assert selector in ["map_row", "authority.object", "object", "authority"], (edit["id"], selector)
                selected = current
                for segment in selector.split("."):
                    selected = selected[segment]
                assert selected == value, (edit["id"], "stable selector")
            parent[key] = edit["replacement_value"]
        else:
            assert edit["kind"] == "EXACT_TEXT_SPAN_REPLACE"
            before, after = edit["expected_before_text"], edit["replacement_text"]
            # Some final replacements deliberately retain the old dated span
            # as history. Test the complete replacement first to avoid nesting
            # another amendment around that retained historical occurrence.
            if texts[path].count(after) == 1:
                reused.append(edit["id"])
                continue
            assert texts[path].count(before) == 1, (edit["id"], "named text span drift")
            assert digest(before.encode()) == edit["expected_before_utf8_sha256"], (edit["id"], "prepared before hash")
            texts[path] = texts[path].replace(before, after, 1)
        applied.append(edit["id"])
    after_bytes = {}
    for path, before in original.items():
        text = json.dumps(objects[path], indent=2, ensure_ascii=False) + "\n" if path in objects else texts[path]
        if b"\r\n" in before:
            text = text.replace("\n", "\r\n")
        after_bytes[path] = (b"\xef\xbb\xbf" if before.startswith(b"\xef\xbb\xbf") else b"") + text.encode()
    trace = objects[plan / "CHANGE_TRACEABILITY.json"]
    schema = objects[plan / "SCHEMA_INTERFACE_REGISTER.json"]
    assert len(trace["rows"]) == 326 and len(schema["objects"]) == 92
    assert after_bytes[plan / "ACTIVE_PLAN.md"] == after_bytes[original_plan / "11_V5_CURRENT_IMPLEMENTATION_ADDENDUM.md"]
    receipt = {
        "version": "HEALTH_V6_GUARDED_RECORD_ADOPTION_V1", "mode": "APPLY" if options.apply else "READ_ONLY",
        "reviewed_content_sha256": expected_content, "pack": str(pack),
        "prepared_sha256": digest(prepared_bytes), "edit_count": 434,
        "applied_edits": applied, "already_adopted_edits": reused, "overlay_rows_added": 0,
        "health_existing_rows": 326, "schema_existing_entries": 92,
        "native_acceptance": "REQUIRED_NOT_RUN", "files": [
            {"path": str(path), "before_sha256": digest(original[path]), "after_sha256": digest(data),
             "before_bytes": len(original[path]), "after_bytes": len(data)} for path, data in after_bytes.items()
        ]
    }
    if options.apply:
        # Re-read immediately before mutations: relevant concurrent edits fail
        # closed. No whole-file captured predecessor is ever restored.
        for path, before in original.items():
            assert path.read_bytes() == before, (str(path), "concurrent target change")
        for path, data in after_bytes.items():
            path.write_bytes(data)
        receipt_dir = repo / "docs/healthroster-build/v6-owner-delivery-20261016"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        receipt_file = receipt_dir / "ADOPTION_RECEIPT.json"
        if receipt_file.exists():
            # Keep the first genuine before/after adoption receipt immutable.
            assert not applied, "existing receipt with new adoption changes"
        else:
            receipt_file.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        manifest_path = plan / "SHA256_MANIFEST.json"
        old_manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        for entry in old_manifest["files"]:
            data = (plan / entry["path"]).read_bytes()
            entry["bytes"], entry["sha256"] = len(data), digest(data)
        old_manifest["current_record_amendment"] = {"content_sha256": expected_content,
            "receipt": "../v6-owner-delivery-20261016/ADOPTION_RECEIPT.json",
            "historical_v5_authority_files_preserved": True}
        manifest_path.write_text(json.dumps(old_manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in receipt.items() if key not in ["files", "applied_edits", "already_adopted_edits"]}, indent=2))
    print(json.dumps({"matched_to_apply": len(applied), "already_adopted": len(reused), "exact_targets": len(original)}))


if __name__ == "__main__":
    main()

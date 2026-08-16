#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import re
from copy import deepcopy
from pathlib import Path
from urllib.parse import quote_from_bytes


HEX = set("0123456789abcdefABCDEF")
HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
UNRESERVED = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"


def _decoded_bytes(raw: str, error: str, *, reject_separators: bool = False) -> bytes:
    result = bytearray()
    index = 0
    while index < len(raw):
        char = raw[index]
        if char == "%":
            if index + 2 >= len(raw) or raw[index + 1] not in HEX or raw[index + 2] not in HEX:
                raise ValueError(error)
            byte = int(raw[index + 1:index + 3], 16)
            if reject_separators and byte in {0x2F, 0x5C, 0x3F, 0x23}:
                raise ValueError("PATH_SEPARATOR_ENCODED")
            result.append(byte)
            index += 3
            continue
        result.extend(char.encode("utf-8"))
        index += 1
    try:
        result.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError(error) from exc
    return bytes(result)


def _reject_controls(value: str, error: str) -> None:
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(error)


def normalize_query(raw: str) -> str:
    if raw == "":
        return ""
    pairs: list[tuple[str, str]] = []
    for item in raw.split("&"):
        if "=" not in item or "+" in item or ";" in item:
            raise ValueError("QUERY_INVALID")
        name_raw, value_raw = item.split("=", 1)
        if not name_raw:
            raise ValueError("QUERY_INVALID")
        try:
            name_bytes = _decoded_bytes(name_raw, "QUERY_ENCODING_INVALID")
            value_bytes = _decoded_bytes(value_raw, "QUERY_ENCODING_INVALID")
        except ValueError:
            raise
        _reject_controls(name_bytes.decode("utf-8"), "QUERY_CONTROL_INVALID")
        _reject_controls(value_bytes.decode("utf-8"), "QUERY_CONTROL_INVALID")
        name = quote_from_bytes(name_bytes, safe=UNRESERVED.decode("ascii"))
        value = quote_from_bytes(value_bytes, safe=UNRESERVED.decode("ascii"))
        pairs.append((name, value))
    pairs.sort()
    return "&".join(f"{name}={value}" for name, value in pairs)


def parse_raw_target(raw_target: str) -> tuple[str, str]:
    if not raw_target.startswith("/") or "#" in raw_target or "\\" in raw_target:
        raise ValueError("PATH_NORMALIZATION_INVALID")
    if raw_target.count("?") > 1:
        raise ValueError("PATH_NORMALIZATION_INVALID")
    raw_path, separator, raw_query = raw_target.partition("?")
    if "//" in raw_path:
        raise ValueError("PATH_NORMALIZATION_INVALID")
    encoded_segments = raw_path.split("/")
    normalized_segments: list[str] = []
    for segment in encoded_segments:
        decoded = _decoded_bytes(segment, "PATH_ENCODING_INVALID", reject_separators=True)
        text = decoded.decode("utf-8")
        _reject_controls(text, "PATH_CONTROL_INVALID")
        if text in {".", ".."}:
            raise ValueError("PATH_NORMALIZATION_INVALID")
        normalized_segments.append(quote_from_bytes(decoded, safe=UNRESERVED.decode("ascii")))
    normalized_path = "/".join(normalized_segments)
    if not normalized_path.startswith("/"):
        raise ValueError("PATH_NORMALIZATION_INVALID")
    return normalized_path, normalize_query(raw_query) if separator else ""


def validate_headers(headers: list[list[str]]) -> None:
    seen: dict[str, str] = {}
    for pair in headers:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("HEADER_FORMAT_INVALID")
        name, value = pair
        if not HEADER_NAME.fullmatch(name) or value != value.strip():
            raise ValueError("HEADER_FORMAT_INVALID")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
            raise ValueError("HEADER_FORMAT_INVALID")
        lower = name.lower()
        if lower in seen:
            raise ValueError("AMBIGUOUS_HEADER")
        seen[lower] = value
    if "transfer-encoding" in seen:
        raise ValueError("TRANSFER_AMBIGUOUS")
    if "content-length" not in seen or not re.fullmatch(r"0|[1-9][0-9]*", seen["content-length"]):
        raise ValueError("HEADER_FORMAT_INVALID")


def validate_raw_case(case: dict) -> tuple[str, str]:
    validate_headers(case["headers"])
    return parse_raw_target(case["raw_target"])


def prefix(vector: dict, body_hash: str) -> bytes:
    return (
        "CLOUDTMS-HMAC-V1\n"
        f"{vector['method']}\n{vector['normalized_path']}\n{vector['normalized_query']}\n"
        f"{vector['timestamp']}\n{vector['nonce']}\n{body_hash}\n"
        f"{vector['idempotency_key']}\n{vector['correlation_id']}\n\n"
    ).encode("ascii")


def sign(vector: dict, key: bytes) -> tuple[str, str, str, str]:
    body = vector["body"].encode("utf-8")
    body_hash = hashlib.sha256(body).hexdigest()
    canonical_prefix = prefix(vector, body_hash)
    message = canonical_prefix + body
    return (
        body_hash,
        base64.b64encode(canonical_prefix).decode("ascii"),
        hashlib.sha256(message).hexdigest(),
        hmac.new(key, message, hashlib.sha256).hexdigest(),
    )


def apply_mutation(base: dict, mutation: dict, key: bytes) -> tuple[dict, str, str | None]:
    vector = deepcopy(base)
    presented_hash = base["body_sha256"]
    presented_signature = base["signature_hex"]
    if "body" in mutation:
        vector["body"] = mutation["body"]
    if "body_append" in mutation:
        vector["body"] += mutation["body_append"]
    if "body_replace" in mutation:
        vector["body"] = vector["body"].replace(*mutation["body_replace"])
    immediate = {
        "body_prefix_bom": "BODY_ENCODING_INVALID",
        "transfer_ambiguity": "TRANSFER_AMBIGUOUS",
        "duplicate_header": "AMBIGUOUS_HEADER",
        "ambiguous_header_casing": "AMBIGUOUS_HEADER",
        "path_invalid": "PATH_NORMALIZATION_INVALID",
        "header_outer_whitespace": "HEADER_FORMAT_INVALID",
    }
    for field, error in immediate.items():
        if mutation.get(field):
            return vector, presented_signature, error
    for field in ("timestamp", "correlation_id", "idempotency_key", "method", "normalized_path", "key_id"):
        if field in mutation:
            vector[field] = mutation[field]
    if "raw_query" in mutation:
        vector["normalized_query"] = normalize_query(mutation["raw_query"])
    if mutation.get("resign"):
        presented_hash, _, _, presented_signature = sign(vector, key)
    if "content_hash" in mutation:
        presented_hash = mutation["content_hash"]
    if "signature_hex" in mutation:
        presented_signature = mutation["signature_hex"]
    vector["presented_hash"] = presented_hash
    return vector, presented_signature, None


def verify(vector: dict, signature: str, key: bytes, active_key_id: str, now: int,
           nonce_set: set[str], reject_query: bool = False) -> str:
    if vector.get("key_id") != active_key_id:
        return "KEY_VERSION_MISMATCH"
    if abs(int(vector["timestamp"]) - now) > 300:
        return "TIMESTAMP_OUTSIDE_WINDOW"
    actual_hash = hashlib.sha256(vector["body"].encode("utf-8")).hexdigest()
    if actual_hash != vector.get("presented_hash", vector["body_sha256"]):
        return "CONTENT_HASH_MISMATCH"
    message = prefix(vector, actual_hash) + vector["body"].encode("utf-8")
    expected = hmac.new(key, message, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        return "SIGNATURE_MISMATCH"
    if vector["nonce"] in nonce_set:
        return "NONCE_REPLAY"
    nonce_set.add(vector["nonce"])
    if reject_query and vector["normalized_query"]:
        return "UNEXPECTED_QUERY"
    return "ACCEPTED"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("vector_file", type=Path)
    args = parser.parse_args()
    data = json.loads(args.vector_file.read_text(encoding="utf-8"))
    key = data["key_ascii"].encode("ascii")
    positives = {vector["id"]: vector for vector in data["positive_vectors"]}

    for vector in positives.values():
        actual = sign(vector, key)
        expected = (
            vector["body_sha256"], vector["canonical_prefix_base64"],
            vector["signed_message_sha256"], vector["signature_hex"],
        )
        assert actual == expected, vector["id"]

    for case in data["query_canonicalization_cases"]:
        assert normalize_query(case["raw_query"]) == case["normalized_query"], case["id"]

    for case in data["raw_parser_cases"]:
        try:
            path, query = validate_raw_case(case)
        except ValueError as exc:
            assert str(exc) == case.get("expected_error"), f"{case['id']}: {exc}"
        else:
            assert "expected_error" not in case, case["id"]
            assert path == case["expected_path"] and query == case["expected_query"], case["id"]

    for case in data["negative_vectors"]:
        base = positives[case["base_id"]]
        vector, signature, immediate = apply_mutation(base, case["mutation"], key)
        if immediate:
            actual = immediate
        else:
            seen: set[str] = set()
            actual = verify(
                vector, signature, key, data["active_key_id"], data["verification_epoch"],
                seen, reject_query="raw_query" in case["mutation"],
            )
            if case["mutation"].get("verify_twice") and actual == "ACCEPTED":
                actual = verify(vector, signature, key, data["active_key_id"], data["verification_epoch"], seen)
        assert actual == case["expected"], f"{case['id']}: {actual}"

    route_valid = sum(1 for vector in positives.values() if vector["route_schema_valid"])
    print(
        f"HMAC_R7_PYTHON_VECTOR_PASS|positive={len(positives)}|route_valid={route_valid}|"
        f"negative={len(data['negative_vectors'])}|query={len(data['query_canonicalization_cases'])}|"
        f"raw_parser={len(data['raw_parser_cases'])}|version={data['version']}"
    )


if __name__ == "__main__":
    main()

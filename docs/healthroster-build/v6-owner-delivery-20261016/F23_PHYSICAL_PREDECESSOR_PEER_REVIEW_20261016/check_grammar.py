"""Offline PostgreSQL parser check; no database/network connections."""
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
import pglast
from pglast import parser

directory = Path(__file__).resolve().parent
sql_path = directory / "16102026_0943_money_prebody_assertion_proposal.sqlinc"
source = sql_path.read_bytes()
assert parser.get_postgresql_version()[0] == 17
parsed = json.loads(parser.parse_sql_json(source.decode("utf-8")))
assert len(parsed["stmts"]) == 2
plpgsql = json.loads(parser.parse_plpgsql_json(source.decode("utf-8")))
assert len(plpgsql) == 1
result = {
    "status": "PASS_OFFLINE_GRAMMAR_ONLY",
    "pglastVersion": pglast.__version__,
    "postgresqlParserVersion": parser.get_postgresql_version(),
    "sourceSha256": hashlib.sha256(source).hexdigest(),
    "topLevelStatements": 2,
    "completeDoBodies": 1,
    "databaseCalls": 0,
    "native": "NOT_RUN",
}
(directory / "GRAMMAR.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps(result))

"""Offline PG17 grammar only. No connection, database, provider or network."""
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, 'C:/tmp/healthroster-v6-physical-parser')
from pglast import parser
assert parser.get_postgresql_version()[0] == 17
base = Path(__file__).resolve().parent
data = (base / 'forward/01_complete_native_claim_core.sqlinc').read_bytes()
source = data.decode('utf-8')
assert len(parser.parse_sql(source)) == 1
parser.parse_plpgsql_json(source)
result = {'version': 'CONTACT_CLAIM_OFFLINE_GRAMMAR_V1',
          'source_sha256': hashlib.sha256(data).hexdigest(), 'postgresql_major': 17,
          'sql_statements': 1, 'complete_plpgsql_bodies': 1, 'grammar': 'PASS',
          'object_resolution_execution_acl_concurrency': 'NOT_RUN', 'database_actions': 'NONE'}
(base / 'OFFLINE_GRAMMAR_RESULT.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result))

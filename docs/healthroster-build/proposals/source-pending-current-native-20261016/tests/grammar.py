"""Offline PG17 grammar and declared row-field checks; no database/network."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

args = argparse.ArgumentParser()
args.add_argument('--parser-path', default='C:/tmp/healthroster-v6-physical-parser')
options = args.parse_args()
sys.path.insert(0, options.parser_path)
from pglast import parser
assert parser.get_postgresql_version()[0] == 17
base = Path(__file__).resolve().parent.parent
root = base.parent.parent.parent.parent
receipt = json.loads((base / 'SOURCE_RECEIPT.json').read_text(encoding='utf-8'))
schema_commit = '3b2f6f385052b51df4004be6b68766ad62c6c17e'
schema_path = 'supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql'
contract_path = 'supabase/release/current-contract.json'
sha = lambda data: hashlib.sha256(data).hexdigest()
def git(file):
    return subprocess.check_output(['git', 'show', schema_commit + ':' + file], cwd=root)
contract_bytes, schema_bytes = git(contract_path), git(schema_path)
contract = json.loads(contract_bytes)
columns = {r['schema'] + '.' + r['name']: {c['name'] for c in r['columns']} for r in contract['relations']}
schema_sql = re.sub(r'(?m)^[ \t]*\\[^\r\n]*(?:\r?\n|$)', '', schema_bytes.decode('utf-8'))
for statement in parser.parse_sql(schema_sql):
    node = statement.stmt
    if type(node).__name__ == 'CreateStmt':
        columns[node.relation.schemaname + '.' + node.relation.relname] = {
            c.colname for c in node.tableElts if type(c).__name__ == 'ColumnDef'}
errors, fields = [], []
counts = {'sql_leaves': 0, 'sql_statements': 0, 'plpgsql_bodies': 0, 'declared_row_variables': 0, 'declared_row_fields': 0}
catalogue = {'path': 'tests/BASELINE_FRESH.sqlinc', 'sha256': sha((base / 'tests' / 'BASELINE_FRESH.sqlinc').read_bytes())}
for item in receipt['outputs'] + [receipt['acl'], catalogue]:
    file = base / item['path']
    raw = file.read_bytes()
    assert sha(raw) == item['sha256']
    source = raw.decode('utf-8')
    counts['sql_leaves'] += 1
    try:
        counts['sql_statements'] += len(parser.parse_sql(source))
    except Exception as error:
        errors.append({'file': item['path'], 'kind': 'SQL_GRAMMAR', 'error': str(error)})
    for function in re.findall(r'create or replace function .*?\$function\$;', source, re.I | re.S):
        name = re.search(r'function\s+([^\s(]+)', function, re.I).group(1)
        try:
            # ROWTYPE substitution is parser-only. No object binding or execution.
            parser.parse_plpgsql_json(re.sub(r'\b(?:public|private)\.\w+%rowtype\b', 'record', function, flags=re.I))
            counts['plpgsql_bodies'] += 1
        except Exception as error:
            errors.append({'file': item['path'], 'routine': name, 'kind': 'PLPGSQL_GRAMMAR', 'error': str(error)})
        for variable, relation in re.findall(r'\b(\w+)\s+((?:public|private)\.\w+)%rowtype', function, re.I):
            counts['declared_row_variables'] += 1
            if relation not in columns:
                errors.append({'routine': name, 'kind': 'UNKNOWN_ROWTYPE', 'relation': relation})
                continue
            for field in sorted(set(re.findall(r'\b' + re.escape(variable) + r'\.(\w+)', function, re.I))):
                fields.append({'routine': name, 'variable': variable, 'relation': relation, 'field': field})
                counts['declared_row_fields'] += 1
                if field not in columns[relation]:
                    errors.append({'routine': name, 'kind': 'UNKNOWN_DECLARED_FIELD', 'relation': relation, 'field': field})
result = {'version': 'SOURCE_PENDING_CURRENT_OFFLINE_PG17_V1',
          'qualification': 'OFFLINE_ONLY_NATIVE_COMPILE_FIRST_USE_ACL_CONCURRENCY_NOT_RUN',
          'source_receipt_sha256': sha((base / 'SOURCE_RECEIPT.json').read_bytes()),
          'schema_commit': schema_commit,
          'schema_inputs': [{'path': contract_path, 'sha256': sha(contract_bytes)}, {'path': schema_path, 'sha256': sha(schema_bytes)}],
          'counts': counts, 'declared_fields': fields, 'errors': errors}
(base / 'tests' / 'GRAMMAR_RESULT.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'qualification': result['qualification'], 'counts': counts, 'errors': errors}))
sys.exit(1 if errors else 0)

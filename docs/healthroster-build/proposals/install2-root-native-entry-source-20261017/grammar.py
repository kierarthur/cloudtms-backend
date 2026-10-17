"""PG17 grammar/whole exporter AST proof, no SQL execution."""
from pathlib import Path
import json,sys,subprocess
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
H=Path(__file__).resolve().parent
sql=subprocess.check_output(['node',str(H/'render-predecessor.mjs')]).decode()
native=json.loads(parser.parse_sql_json(sql));assert len(native['stmts'])==2
source=(H/'parents/supabase/release/export_contract.sql').read_text()
lines=[l for l in source.splitlines() if not l.lstrip().startswith('\\')]
original=json.loads(parser.parse_sql_json('\n'.join(lines)))
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ['location','stmt_location','stmt_len']}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
select=native['stmts'][1]['stmt']['SelectStmt'];ctes=select['withClause']['ctes']
assert [x['CommonTableExpr']['ctename'] for x in ctes]==['native_identity','native_contract']
assert clean(ctes[1]['CommonTableExpr']['ctequery'])==clean(original['stmts'][1]['stmt'])
def walk(v):
 if isinstance(v,dict):
  yield v
  for x in v.values():yield from walk(x)
 elif isinstance(v,list):
  for x in v:yield from walk(x)
assert not any(any(k in n for k in ['InsertStmt','UpdateStmt','DeleteStmt','MergeStmt','CreateStmt','CopyStmt','CallStmt']) for n in walk(native))
relations=[n['RangeVar'] for n in walk(native) if 'RangeVar' in n]
assert sum(n['relname']=='cloudtms_migration_ledger' for n in relations)==1
assert sum(n['relname']=='cloudtms_repeatable_ledger' for n in relations)==1
print(json.dumps(dict(status='PG17_FIXED_METADATA_PREDECESSOR_AST_PASS',parserVersion=pglast.__version__,postgresGrammar=pglast.get_postgresql_version(),statements=2,wholeCanonicalExporterASTEqual=True,arbitrarySQLInput=False,native='NOT_RUN')))

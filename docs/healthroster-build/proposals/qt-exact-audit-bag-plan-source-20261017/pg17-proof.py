"""PG17 grammar/complete ExplainStmt query equality only; no SQL execution."""
from pathlib import Path
import sys,json,hashlib,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
H=Path(__file__).resolve().parent
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in {'location','stmt_location','stmt_len','arg_location'}}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
def parsed(s):return json.loads(pglast.parser.parse_sql_json(s))['stmts']
def check(a,e):
 aa=parsed(a);ee=parsed(e);assert len(aa)==len(ee)==1
 q=ee[0]['stmt']['ExplainStmt'];assert clean(q['query'])==clean(aa[0]['stmt'])
 opts={o['DefElem']['defname']:o['DefElem']['arg']['String']['sval'] for o in q['options']}
 assert len(q['options'])==6 and opts=={'analyze':'false','verbose':'false','costs':'true','settings':'false','summary':'false','format':'json'}
 return q
a=(H/'AUDIT_BAG.sql').read_text();e=(H/'EXPLAIN_AUDIT_BAG.sql').read_text();q=check(a,e)
assert tuple(pglast.get_postgresql_version())[:1]==(17,)
def walk(v):
 if isinstance(v,dict):
  for k,x in v.items():yield k,x;yield from walk(x)
 elif isinstance(v,list):
  for x in v:yield from walk(x)
relations=[x for k,x in walk(q) if k=='RangeVar'];assert {(x.get('schemaname'),x['relname']) for x in relations}=={('public','audit_events'),(None,'retained_business'),(None,'fixed_bag')}
for token in ['ANALYZE TRUE','VERBOSE TRUE','COSTS FALSE','SETTINGS TRUE','SUMMARY TRUE','FORMAT TEXT']:
 key=token.split()[0];old={'ANALYZE':'ANALYZE FALSE','VERBOSE':'VERBOSE FALSE','COSTS':'COSTS TRUE','SETTINGS':'SETTINGS FALSE','SUMMARY':'SUMMARY FALSE','FORMAT':'FORMAT JSON'}[key]
 try:check(a,e.replace(old,token));raise RuntimeError('NEGATIVE_ACCEPTED')
 except AssertionError:pass
try:check(a,e.replace('public.audit_events','public.clients'));raise RuntimeError('NEGATIVE_ACCEPTED')
except AssertionError:pass
try:check(a,e+'select 1;');raise RuntimeError('NEGATIVE_ACCEPTED')
except AssertionError:pass
d=json.loads((H/'SOURCE_DERIVATION.json').read_bytes());assert a==d['wholeQueryInverse']['before'] and e==d['wholeQueryInverse']['after'];assert e==d['wholeQueryInverse']['prefix']+a+d['wholeQueryInverse']['suffix'];assert hashlib.sha256(a.encode()).hexdigest()==d['auditSha256']==d['descriptor']['executedAuditBagSha256']
print(json.dumps({'postgresql':pglast.get_postgresql_version(),'completeQueryAstEqual':True,'sqlStatements':2,'negativeMutations':8,'executedAuditBagBytes':len(a.encode()),'native':'NOT_RUN'}))

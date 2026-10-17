"""PG17 parsing/AST only. No SQL execution or native plan/performance claim."""
from pathlib import Path
import json,sys,copy,re
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
H=Path(__file__).resolve().parent
old=(H/'parent/qt-assertions.sql').read_text();new=(H/'payload/qt-assertions.sql').read_text();f=json.loads((H/'FORWARD.json').read_text())
def sql(s):return json.loads(pglast.parser.parse_sql_json(s))
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
def clean(n):
 if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len','lineno','stmtid'}}
 if isinstance(n,list):return [clean(v) for v in n]
 return n
def part(s,label):return s[s.index('do $'+label+'$'):s.index('end;$'+label+'$;')+len('end;$'+label+'$;')]
assert pglast.get_postgresql_version()[0]==17
parsed=sql(new)['stmts'];assert len(parsed)==8
ctas=[s['stmt']['CreateTableAsStmt'] for s in parsed if 'CreateTableAsStmt' in s['stmt']];assert len(ctas)==2
expected=sql("select a.event_sequence,pg_catalog.decode(md5(to_jsonb(a)::text),'hex') as row_digest from public.audit_events a limit 1500001;")['stmts'][0]['stmt']
for phase,c in zip(['before','after'],ctas):
 assert c['objtype']=='OBJECT_TABLE' and not c.get('is_select_into',False) and not c.get('if_not_exists',False)
 assert c['into']['rel']['relname']=='hr_contact_qt_audit_'+phase and c['into']['rel']['relpersistence']=='t'
 assert c['into']['onCommit']=='ONCOMMIT_DROP' and not c['into'].get('skipData',False)
 assert clean(c['query'])==clean(expected)
 q=c['query']['SelectStmt'];assert not q.get('whereClause') and not q.get('sortClause') and not q.get('limitOffset') and len(q['targetList'])==2
 assert len([v for k,v in walk(q) if k=='RangeVar'])==1
for label in ['qt_audit_admission','qt_controls']:assert part(new,label)==part(old,label);assert clean(pl(part(new,label)))==clean(pl(part(old,label)))
for label in ['qt_audit_before_bound','qt_audit_equality']:pl(part(new,label))
before=part(new,'qt_audit_before_bound');eq=part(new,'qt_audit_equality')
assert before.index('count(*)')<before.index('QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED')
assert new.index('QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED')<new.index('insert into public.tms_users')
assert len(re.findall('>1500000',eq))==2
# Parse actual equality condition via SQL, proving EXCEPT ALL both directions and exact two columns.
condition=re.search(r" if exists\(select event_sequence,row_digest[\s\S]*? then\n",eq).group(0)[4:-6].strip()
expr=sql('select '+condition+';')
ops=[v for k,v in walk(expr) if k=='SelectStmt' and v.get('op')=='SETOP_EXCEPT']
assert len(ops)==2 and all(x.get('all') is True for x in ops)
names=[]
for op in ops:
 pair=[]
 for side in ['larg','rarg']:
  q=op[side];assert not q.get('whereClause') and not q.get('limitCount')
  rv=q['fromClause'][0]['RangeVar'];assert rv['schemaname']=='pg_temp'
  assert [r['ResTarget']['val']['ColumnRef']['fields'][0]['String']['sval'] for r in q['targetList']]==['event_sequence','row_digest']
  pair.append(rv['relname'])
 names.append(pair)
assert names==[['hr_contact_qt_audit_before','hr_contact_qt_audit_after'],['hr_contact_qt_audit_after','hr_contact_qt_audit_before']]
assert eq.index("current_setting('enable_hashagg')")<eq.index("set_config('enable_hashagg','off',true)")<eq.index('except all')<eq.rindex("set_config('enable_hashagg',audit_original_hashagg,true)")
assert clean(parsed[-1]['stmt'])==clean(sql('select value as programme_result from hr_contact_qt_result;')['stmts'][0]['stmt'])
for file in ['PROGRAMMES.json','OFFLINE_INPUTS.json']:
 data=json.loads((H/'payload'/file).read_text())
 for value in data.values():
  text=value['sql'] if file=='PROGRAMMES.json' else value['input']['sql']
  for statement in sql(text)['stmts']:
   if 'DoStmt' in statement['stmt']:pl(text[statement.get('stmt_location',0):statement.get('stmt_location',0)+statement.get('stmt_len',len(text))]+';')
print(json.dumps({'status':'PASS_PG17_AST_SOURCE_ONLY','pglast':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'assertionStatements':8,'twoFixedSnapshotBoundedKeyHashCaptures':True,'limit':1500001,'cap':1500000,'mainFixtureAndAdmissionRawAndAstUnchanged':True,'bothExactBinaryTupleExceptAllDirections':True,'localHashaggSaveOffRestoreSourceOrder':True,'statementAndDoParsingAllProgrammes':True,'native':'NOT_RUN','performance':'NOT_PROVED','ctasWireAbi':'UNOBSERVED','SQLExecuted':False}))

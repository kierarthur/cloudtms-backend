"""PG17 grammar/AST only. No PostgreSQL connection, SQL or EXPLAIN execution."""
from pathlib import Path
import json,sys,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
f=json.loads((HERE/'FORWARD.json').read_text());old=(HERE/'parent/qt-assertions.sql').read_text();new=(HERE/'payload/qt-assertions.sql').read_text()
def parse(s):return json.loads(pglast.parser.parse_sql_json(s))
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
def clean(n):
 if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len','lineno','stmtid'}}
 if isinstance(n,list):return [clean(v) for v in n]
 return n
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
oldq=parse(f['oldQuery']);newq=parse(f['newQuery']);aggo=next(v for k,v in walk(oldq) if k=='FuncCall' and v['funcname'][0]['String']['sval']=='string_agg');aggn=next(v for k,v in walk(newq) if k=='FuncCall' and v['funcname'][0]['String']['sval']=='string_agg')
assert clean(aggo['args'])==clean(aggn['args'])
assert clean(aggn['agg_order'])==[{'SortBy':{'node':{'ColumnRef':{'fields':[{'String':{'sval':'a'}},{'String':{'sval':'event_sequence'}}]}},'sortby_dir':'SORTBY_DEFAULT','sortby_nulls':'SORTBY_NULLS_DEFAULT'}}]
restored=copy.deepcopy(newq);next(v for k,v in walk(restored) if k=='FuncCall' and v['funcname'][0]['String']['sval']=='string_agg')['agg_order']=aggo['agg_order'];assert clean(restored)==clean(oldq)
assert [(v.get('schemaname'),v['relname'],v.get('alias',{}).get('aliasname')) for k,v in walk(newq) if k=='RangeVar']==[('public','audit_events','a')]
select=newq['stmts'][0]['stmt']['SelectStmt'];assert not any(k in select for k in ['whereClause','groupClause','havingClause','distinctClause','limitCount','limitOffset','windowClause'])
assert new.count(f['newQuery'])==2 and old.count(f['oldQuery'])==2
assert new.replace(f['newQuery'],f['oldQuery']).replace(f['guardInsertion']['after'],f['guardInsertion']['before'])==old
removed=[]
def inverse(n):
 if isinstance(n,dict):
  if 'PLpgSQL_stmt_if' in n and 'audit_order_index' in n['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query']:
   removed.append(n);return None
  return {k:inverse(v) for k,v in n.items()}
 if isinstance(n,list):return [v for x in n if (v:=inverse(x)) is not None]
 if isinstance(n,str):return n.replace(f['newQuery'],f['oldQuery'])
 return n
assert clean(inverse(pl(new)))==clean(pl(old));assert len(removed)==1
guard=removed[0]['PLpgSQL_stmt_if'];guard_query=guard['cond']['PLpgSQL_expr']['query'];guard_sql=parse('select ('+guard_query+')');relations={(v.get('schemaname'),v['relname']) for k,v in walk(guard_sql) if k=='RangeVar'}
assert relations=={('pg_catalog',x) for x in ['pg_index','pg_class','pg_namespace','pg_am','pg_attribute']}
aliases=[v['alias']['aliasname'] for k,v in walk(guard_sql) if k=='RangeVar'];assert len(aliases)==len(set(aliases))==5
variables={v['refname'] for k,v in walk(pl(old)) if k in ['PLpgSQL_var','PLpgSQL_rec'] and 'refname' in v};assert not variables.intersection(aliases)
for required in ['indisunique','indimmediate','indisvalid','indisready','indislive','indnkeyatts','indnatts','indexprs','indpred','indkey','indoption','attnotnull','atttypid','attidentity']:
 assert any(k=='ColumnRef' and v['fields'][-1].get('String',{}).get('sval')==required for k,v in walk(guard_sql))
assert any(k=='PLpgSQL_stmt_raise' and v['message']=='QT_AUDIT_EVENT_SEQUENCE_INDEX_UNAVAILABLE' for k,v in walk(guard))
counter=[]
for name,mutant in [('filter',f['newQuery']+' where false'),('partial row',f['newQuery'].replace('to_jsonb(a)','to_jsonb(a.id)')),('distinct',f['newQuery'].replace('string_agg(','string_agg(distinct ')),('delimiter',f['newQuery'].replace("'|'","','")),('order UUID',f['newQuery'].replace('a.event_sequence','a.id')),('limit',f['newQuery']+' limit 1'),('float order',f['newQuery'].replace('a.event_sequence','a.event_sequence::double precision')),('outer hash',f['newQuery'].replace('select md5(','select upper('))]:
 assert clean(parse(mutant))!=clean(newq);counter.append(name)
programmes=json.loads((HERE/'payload/PROGRAMMES.json').read_text());inputs=json.loads((HERE/'payload/OFFLINE_INPUTS.json').read_text())
result={'version':'QT_SEQUENCE_ORDER_PG17_SOURCE_PROOF_V1','postgresql':list(pglast.get_postgresql_version()),'pglast':pglast.__version__,'completePlAstInverseExcludingSourcePositions':True,'wholeRawInverse':True,'onlyDigestAstDelta':'string_agg aggregate ordering: whole-row MD5 -> actual a.event_sequence','completeRowHashArgumentUnchanged':True,'delimiterOuterMd5CoalesceAndNoFilterUnchanged':True,'twoFreshLocalDigestQueries':2,'metadataGuardBeforeFixtures':True,'metadataRelations':sorted('.'.join(x) for x in relations),'metadataAliasVariableConflicts':0,'counterexamples':counter,'programmes':{k:len(pglast.parse_sql(v['sql'])) for k,v in programmes.items()},'inputs':{k:{field:len(pglast.parse_sql(v['input'][field])) for field in ['sql','verification_sql']} for k,v in inputs.items()},'digestEncodingChanged':True,'nativePerformance':'NOT_PROVED','SQLExecuted':False,'native':'NOT_RUN'}
print(json.dumps(result))

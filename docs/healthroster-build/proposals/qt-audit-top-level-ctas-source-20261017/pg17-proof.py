"""PG17 source AST proof only. No SQL, EXPLAIN, native execution or physical-plan claim."""
from pathlib import Path
import json,sys,re,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
f=json.loads((HERE/'FORWARD.json').read_text());old=(HERE/'parent/qt-assertions.sql').read_text();new=(HERE/'payload/qt-assertions.sql').read_text()
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
def clean(n):
 if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len','lineno','stmtid'}}
 if isinstance(n,list):return [clean(v) for v in n]
 return n
def sql(s):return json.loads(pglast.parser.parse_sql_json(s))
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
def main(s):return 'do $qt_controls$'+s.split('do $qt_controls$')[1].split('end;$qt_controls$;')[0]+'end;$qt_controls$;'
original_query=re.search(r'execute \$qt_full_audit_digest\$(.*?)\$qt_full_audit_digest\$',old,re.S).group(1)
parsed=sql(new)['stmts'];ctas=[x['stmt']['CreateTableAsStmt'] for x in parsed if 'CreateTableAsStmt' in x['stmt']];assert len(ctas)==2
for phase,x in zip(['before','after'],ctas):
 assert x['objtype']=='OBJECT_TABLE' and not x.get('is_select_into',False) and not x.get('if_not_exists',False)
 assert x['into']['rel']['relname']=='hr_contact_qt_audit_'+phase and x['into']['rel']['relpersistence']=='t'
 assert x['into']['onCommit']=='ONCOMMIT_DROP' and not x['into'].get('skipData',False)
 q=copy.deepcopy(x['query']);assert q['SelectStmt']['targetList'][0]['ResTarget'].pop('name')=='digest'
 assert clean(q)==clean(sql(original_query)['stmts'][0]['stmt'])
assert len(sql(old)['stmts'])==3 and len(parsed)==7
assert clean(parsed[-1]['stmt'])==clean(sql('select value as programme_result from hr_contact_qt_result;')['stmts'][0]['stmt'])
assert list(parsed[-2]['stmt'])==['DoStmt'] and 'QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED' in parsed[-2]['stmt']['DoStmt']['args'][0]['DefElem']['arg']['String']['sval']
old_main,new_main=pl(main(old)),pl(main(new));removed=[]
clause=f['edits'][2]['before'][len(f['edits'][2]['after'])-len(' then'):-len(' then')]
def expected(n):
 if isinstance(n,dict):
  if 'PLpgSQL_stmt_if' in n and 'audit_order_index' in n['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query']:removed.append('guard');return None
  if 'PLpgSQL_stmt_dynexecute' in n and original_query in n['PLpgSQL_stmt_dynexecute']['query']['PLpgSQL_expr']['query']:removed.append('digest');return None
  return {k:expected(v) for k,v in n.items()}
 if isinstance(n,list):return [v for x in n if (v:=expected(x)) is not None]
 if isinstance(n,str):return n.replace(clause,'')
 return n
a=expected(old_main)
datums=a[0]['PLpgSQL_function']['datums']
assert datums[25]=={'PLpgSQL_row':{'refname':'(unnamed row)','lineno':0,'fields':[{'name':'audits_before','varno':22}]}} or clean(datums[25])=={'PLpgSQL_row':{'refname':'(unnamed row)','fields':[{'name':'audits_before','varno':22}]}}
datums.pop(25)
def remap_exception_datums(n):
 if isinstance(n,dict):return {k:(v-1 if k in {'sqlstate_varno','sqlerrm_varno'} and isinstance(v,int) and v>25 else remap_exception_datums(v)) for k,v in n.items()}
 if isinstance(n,list):return [remap_exception_datums(v) for v in n]
 return n
assert clean(remap_exception_datums(a))==clean(new_main);assert sorted(removed)==['digest','guard']
restored=new
for e in reversed([x for x in f['edits'] if x['target']=='qt-assertions.sql']):assert restored.count(e['after'])==1;restored=restored.replace(e['after'],e['before'])
assert restored==old
admission=pl('do $qt_audit_admission$'+new.split('do $qt_audit_admission$')[1].split('end;$qt_audit_admission$;')[0]+'end;$qt_audit_admission$;')
assert 'QT_AUDIT_EVENT_SEQUENCE_INDEX_UNAVAILABLE' in json.dumps(admission) and 'QT_AUDIT_DIGEST_TEMP_NAMESPACE_UNAVAILABLE' in json.dumps(admission)
assert new.index('QT_AUDIT_EVENT_SEQUENCE_INDEX_UNAVAILABLE')<new.index(f['ctas'][0])<new.index('insert into public.tms_users')<new.index(f['ctas'][1])<new.rindex('QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED')<new.index('select value as programme_result')
# Source tail added to the audit-covered interval: existing catalogue/role/user reads and one existing control-table INSERT.
tail=main(old).split("raise exception 'QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED';end if;",1)[1]
assert 'update public.' not in tail.lower() and 'delete from' not in tail.lower()
assert tail.lower().count('insert into')==1 and 'insert into hr_contact_qt_result' in tail
assert 'create temporary table hr_contact_qt_result(value jsonb not null) on commit drop;' in new
programmes=json.loads((HERE/'payload/PROGRAMMES.json').read_text());inputs=json.loads((HERE/'payload/OFFLINE_INPUTS.json').read_text())
result={'version':'QT_TOP_LEVEL_CTAS_PG17_SOURCE_PROOF_V1','postgresql':list(pglast.get_postgresql_version()),'pglast':pglast.__version__,'fullRawInverse':True,'mainPlAstOnlyRemovesMovedGuardAndTwoDigestSites':True,'twoCtasInnerFullSelectAstEqualToOriginal':True,'temporaryOnCommitDropTables':2,'programmeResultAfterNativeEquality':True,'indexGuardBeforeCtasAndFixtures':True,'auditIntervalTail':{'existingReadOnlyAclRoleCountChecks':True,'existingTemporaryResultInsert':1,'additionalBusinessWrites':0},'programmes':{k:len(pglast.parse_sql(v['sql'])) for k,v in programmes.items()},'inputs':{k:{field:len(pglast.parse_sql(v['input'][field])) for field in ['sql','verification_sql']} for k,v in inputs.items()},'ctasExpectedWireCompletion':'SELECT 1, empty fields and rows','ctasNativeAbi':'UNOBSERVED','nestedSpiSerialCause':'NOT_PROVED','parallelExecution':'NOT_PROVED','performance':'NOT_PROVED','SQLExecuted':False,'native':'NOT_RUN'}
print(json.dumps(result))

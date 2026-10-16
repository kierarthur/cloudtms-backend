import hashlib,json,pathlib,re,sys
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
if sys.platform=='win32':here=pathlib.Path('\\\\?\\'+str(here))
proposal=json.loads((here/'PROPOSAL.json').read_text(encoding='utf-8'))
def clean(text):return re.sub(r'^\\set\b.*$','',text,flags=re.M)
def strip(value):
 if isinstance(value,list):return [strip(x) for x in value]
 if isinstance(value,dict):return {k:strip(v) for k,v in value.items() if not k.endswith('location') and k!='stmt_len'}
 return value
rows=[]
for number,pin in enumerate(proposal['files']):
 before=(here/'inputs/before'/pin['file']).read_text(encoding='utf-8');after=(here/'forward'/pin['file']).read_text(encoding='utf-8')
 old=json.loads(parser.parse_sql_json(clean(before)));new=json.loads(parser.parse_sql_json(clean(after)))
 if number<2:
  locks=[stmt['stmt']['LockStmt'] for stmt in new['stmts'] if 'LockStmt' in stmt['stmt']]
  assert len(locks)==len(pin['lockSet'])
  for actual,wanted in zip(locks,pin['lockSet']):
   assert actual['nowait'] is True
   assert actual['mode']=={'SHARE':5,'ACCESS EXCLUSIVE':8}[wanted['mode']]
   assert actual['relations'][0]['RangeVar']['schemaname']+'.'+actual['relations'][0]['RangeVar']['relname']==wanted['relation']
  without={**new,'stmts':[stmt for stmt in new['stmts'] if 'LockStmt' not in stmt['stmt']]}
  assert strip(without)==strip(old)
  assert 'TransactionStmt' in new['stmts'][0]['stmt']
  assert all('LockStmt' in stmt['stmt'] for stmt in new['stmts'][1:1+len(locks)])
  rows.append({'file':pin['file'],'newLockStatements':len(locks),'allNowait':True,'beforeAllOriginalChecks':True,'allOriginalStatementsAstEqual':True})
 else:
  a="select distinct(value->>'candidate_id')::uuid candidate_id,(value->>'client_id')::uuid client_id from jsonb_array_elements(facts->'pairs'||p_extra_pairs)order by 1,2"
  b=a.replace("facts->'pairs'||p_extra_pairs","(facts->'pairs')||p_extra_pairs")
  assert strip(json.loads(parser.parse_sql_json(a)))==strip(json.loads(parser.parse_sql_json(b)))
  assert before.count("facts->'pairs'||p_extra_pairs")==1
  assert after.count("(facts->'pairs')||p_extra_pairs")==1
  rows.append({'file':pin['file'],'fullBeforeAndAfterSqlGrammar':True,'entireForQuerySqlAstEqualIgnoringLocations':True,'exactOneParenthesisChange':True})
out={'version':'CONTACT_MAPPED_LOCK_GROUPING_PG17_SOURCE_CHECK_V1','qualification':'OFFLINE_GRAMMAR_AST_ONLY','library':pglast.__version__,'postgres':parser.get_postgresql_version(),'rows':rows,'databaseCalls':0,'native':'NOT_RUN'}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps(out))

"""Offline PostgreSQL17 grammar proof; no database connection or credentials."""
import json,pathlib,sys,hashlib
sys.path.insert(0,r'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
prior=json.loads((here.parent/'v6-contact-local-metadata-mcp-adapter-20261016/OFFLINE_INPUTS.json').read_text())
packet=json.loads((here/'OFFLINE_PROGRAMS.json').read_text())
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ('location','stmt_location','stmt_len')}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
def ast(sql):return [clean(s['stmt']) for s in json.loads(parser.parse_sql_json(sql))['stmts']]
sha=lambda s:hashlib.sha256(s.encode()).hexdigest()
assert pglast.get_postgresql_version()[0]==17
rows=[]
for c in packet['cases']:
 old=next(x for x in prior['cases'] if x['caseId']==c['caseId'])
 assert c['input']==old['input'] and c['receipt']==old['receipt']
 assert ast(c['input']['sql'])==ast(old['input']['sql'])
 assert len(ast(c['input']['sql']))==c['receipt']['indices']['expectedFailedStatement']
 assert len(json.loads(parser.parse_plpgsql_json(c['input']['sql'])))==1
 assert c['canonicalFixtureProof']==old['canonicalFixtureProof']
 assert c['afterInput']==c['beforeInput'][:-len('rollback;\n')]+c['canonicalFixtureProof']+'\nrollback;\n'
 assert ast(c['afterInput'])==ast(c['beforeInput'])[:-1]+ast(c['canonicalFixtureProof'])+ast('rollback;')
 assert len(ast(c['beforeInput']))==6 and len(ast(c['afterInput']))==7
 assert len(json.loads(parser.parse_plpgsql_json(c['beforeInput'])))==1
 assert len(json.loads(parser.parse_plpgsql_json(c['afterInput'])))==1
 rows.append({'caseId':c['caseId'],'firstBFinalLockIndex':c['receipt']['indices']['expectedFailedStatement'],
  'wholeFirstBInputAndReceiptUnchanged':True,'beforeStatements':6,'afterStatements':7,'beforeOutputFrames':2,'afterOutputFrames':3,
  'canonicalSnapshotAndOriginalFixtureSqlUnchanged':True,'fixtureSqlSha256':sha(c['canonicalFixtureProof']),
  'afterInputSha256':sha(c['afterInput']),'exactAppendAndASTReverse':True})
result={'scope':'OFFLINE_POSTGRES17_SOURCE_GRAMMAR_ONLY','postgresql':pglast.get_postgresql_version(),'pglast':pglast.__version__,
 'cases':rows,'canonicalSqlChanged':False,'databaseCalls':0,'native':'NOT_RUN','PartB':'NOT_RUN'}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'status':'PG17_SIX_CANONICAL_THIRD_FRAME_PROGRAMS_PASS','cases':len(rows),'native':'NOT_RUN'}))

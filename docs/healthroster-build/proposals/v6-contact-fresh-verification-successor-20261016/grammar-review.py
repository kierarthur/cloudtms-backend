"""Offline PG17 parse and source reverse; does not connect to a database."""
import json,pathlib,sys,hashlib
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
old=json.loads((here.parent/'v6-contact-local-metadata-mcp-adapter-20261016/OFFLINE_INPUTS.json').read_text())
packet=json.loads((here/'OFFLINE_QUALIFICATION_INPUTS.json').read_text())
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ('location','stmt_location','stmt_len')}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
def ast(sql):return [clean(s['stmt']) for s in json.loads(parser.parse_sql_json(sql))['stmts']]
sha=lambda s:hashlib.sha256(s.encode()).hexdigest()
rows=[]
for n,c in enumerate(packet['cases']):
 predecessor=old['cases'][n]
 assert c['caseId']==predecessor['caseId']
 assert c['sql']==c['verification_sql']==predecessor['offlineSecondProofSql']
 assert sha(c['sql'])==c['oldMainSqlSha256']
 assert c['sql'].startswith(c['canonicalFixtureProof']+'\n')
 assert c['canonicalFixtureProof']==predecessor['canonicalFixtureProof']
 assert len(ast(c['sql']))==len(ast(c['verification_sql']))==2
 assert ast(c['sql'])==ast(predecessor['offlineSecondProofSql'])
 b=predecessor['input']['sql'];original=predecessor['reverse']['originalSql'];r=predecessor['receipt'];removed=r['original']['removedStatement']-1
 assert ast(b)==ast(original)[:removed]+ast(original)[removed+1:]
 assert len(ast(b))==r['indices']['expectedFailedStatement']
 assert len(json.loads(parser.parse_plpgsql_json(b)))==1
 rows.append({'caseId':c['caseId'],'firstBFinalLockIndex':r['indices']['expectedFailedStatement'],
  'firstBAllRetainedStatementASTUnchanged':True,'secondMainStatements':2,'secondFreshVerificationStatements':2,
  'canonicalFixtureAndHeldAPairRawIdentical':True,'secondMainAndFreshASTIdentical':True,
  'secondMainSha256':sha(c['sql']),'secondFreshVerificationSha256':sha(c['verification_sql'])})
result={'scope':'OFFLINE_POSTGRES17_SOURCE_GRAMMAR_ONLY','postgresql':pglast.get_postgresql_version(),'pglast':pglast.__version__,
 'cases':rows,'canonicalSqlChanged':False,'databaseCalls':0,'native':'NOT_RUN','PartB':'NOT_RUN'}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'status':'PG17_OFFLINE_SIX_CASES_PASS','cases':len(rows),'native':'NOT_RUN'}))

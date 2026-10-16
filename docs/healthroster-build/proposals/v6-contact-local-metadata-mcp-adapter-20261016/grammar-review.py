"""PG17 offline raw/AST reverse proof; no DB connection or stub functions."""
import hashlib,json,pathlib,sys
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
packet=json.loads((here/'OFFLINE_INPUTS.json').read_text())
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ('location','stmt_location','stmt_len')}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
def ast(sql):return [clean(v['stmt']) for v in json.loads(parser.parse_sql_json(sql))['stmts']]
rows=[]
for e in packet['cases']:
 r=e['receipt'];sql=e['input']['sql'];old=e['reverse']['originalSql'];removed=r['original']['removedStatement']-1
 old_ast=ast(old);new_ast=ast(sql)
 assert new_ast==old_ast[:removed]+old_ast[removed+1:]
 assert len(new_ast)==r['statements']==r['indices']['expectedFailedStatement']
 assert sql[:r['original']['removedStart']]+e['reverse']['removedStatementSql']+sql[r['original']['removedStart']:]==old
 assert ast(e['input']['verification_sql'])[0]==ast(e['reverse']['originalVerificationSql'])[0]
 assert ast(e['input']['verification_sql'])[2]==ast(e['reverse']['originalVerificationSql'])[2]
 assert len(ast(e['input']['verification_sql']))==3
 assert len(ast(e['offlineSecondProofSql']))==2
 assert e['offlineSecondProofSql'].startswith(e['canonicalFixtureProof']+'\n')
 assert len(json.loads(parser.parse_plpgsql_json(sql)))==1
 rows.append({'caseId':e['caseId'],'statements':len(new_ast),'expectedFinalLockIndex':r['indices']['expectedFailedStatement'],
  'wholeOriginalRawReverse':True,'allRetainedStatementsASTIdentical':True,'allRetainedStatementsRawIdentical':True,
  'smallFreshStatements':3,'freshOriginalFirstLastASTIdentical':True,'secondGenuineFixtureProofStatements':2,'doGuardParsed':True})
out={'scope':'OFFLINE_POSTGRES17_GRAMMAR_AND_RAW_AST_REVERSE_ONLY','pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version(),
 'cases':rows,'native':'NOT_RUN','fixedMetadataReader':'Unchanged7fda; six complete fixed read-only programs previously grammar checked; source-pinned'}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'scope':out['scope'],'cases':len(rows),'native':'NOT_RUN'}))

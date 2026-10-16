import hashlib,json,pathlib,sys
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
if sys.platform=='win32':here=pathlib.Path('\\\\?\\'+str(here))
data=json.loads((here/'GRAMMAR_INPUTS.json').read_text(encoding='utf-8'))
def strip(v):
 if isinstance(v,list):return[strip(x)for x in v]
 if isinstance(v,dict):return{k:strip(x)for k,x in v.items()if not k.endswith('location')and k!='stmt_len'}
 return v
def parse(s):return strip(json.loads(parser.parse_sql_json(s)))
def inner(tree):
 top=tree['stmts'][0]['stmt']['SelectStmt'];ctes=top['withClause']['ctes'];first=ctes[0]['CommonTableExpr'];assert first['ctename']=='contact_canonical_frame';assert first['ctematerialized']=='CTEMaterializeAlways';return first['ctequery']
proof=[]
for case in data['cases']:
 original=parse(case['query']);wrapped=parse(case['wrappedQuery']);assert inner(wrapped)==original['stmts'][0]['stmt'];main=parse(case['sql']);fresh=parse(case['verificationSql']);assert len(main['stmts'])==case['statements'];assert len(fresh['stmts'])==3
 proof.append({'case':case['id'],'statements':len(main['stmts']),'freshStatements':len(fresh['stmts']),'wholeOriginalInnerAstEqual':True,'materializedCanonicalQueryOnce':True})
fixture=data['fixture'];assert inner(parse(fixture['wrappedQuery']))==parse(fixture['query'])['stmts'][0]['stmt']
out={'version':'CONTACT_MCP_FRAME_INDEPENDENT_PG17_GRAMMAR_REVIEW_V1','qualification':'OFFLINE_PG17_GRAMMAR_AND_COMPLETE_INNER_AST_ONLY','pglast':pglast.__version__,'postgres':parser.get_postgresql_version(),'cases':proof,'wholeFixtureInnerAstEqual':True,'databaseCalls':0,'native':'NOT_RUN'}
(here/'GRAMMAR_REVIEW.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'sixCases':len(proof),'snapshotInnerAstEqual':True,'fixtureInnerAstEqual':True,'native':'NOT_RUN'}))

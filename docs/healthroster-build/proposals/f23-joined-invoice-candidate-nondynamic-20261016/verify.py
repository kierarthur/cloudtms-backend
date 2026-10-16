"""Offline receipt/PG17 parse checks only. Never connects to a database."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path(sys.argv[1]);COMMIT='c1ebb691feb6a11324060e0c28210e2bd735624a'
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
sha=lambda x:hashlib.sha256(x if isinstance(x,bytes)else x.encode()).hexdigest()
read=lambda p:json.loads((HERE/p).read_text(encoding='utf-8'))
frozen=lambda p:subprocess.check_output(['git','show',COMMIT+':'+p],cwd=ROOT)
D,R,P,C,M=map(read,['DISCOVERY.json','REVIEW.json','PROVENANCE.json','CORRECTION_RECEIPT.json','EXTENSION_MAP.json'])
checks=[]
def check(name,assertion):
 assert assertion,name
 checks.append({'name':name,'result':'PASS'})
check('All exact source commits agree',all(x['sourceCommit']==COMMIT for x in[D,R,P,C,M]))
check('Exact census38 diagnostics and finite37 points',len(D['rows'])==38 and len(M['points'])==37 and sum(p['newDiagnosticCount']for p in M['points'])==37)
for pin in M['artifacts']:check('Artifact digest '+pin['path'],sha((HERE/pin['path']).read_bytes())==pin['sha256'])
for pin in M['filePins']:
 original=frozen(pin['path']);check('Original joined file '+pin['path'],sha(original)==pin['originalSha256'])
 changed=original.decode('utf-8')
 if pin['correctionRequired']:changed=changed.replace(C['exactReplacement']['before'],C['exactReplacement']['after'])
 check('Exact corrected joined file '+pin['path'],sha(changed)==pin['afterRequiredCorrectionSha256'])
check('16 exact producer complete functions',P['counts']['exactProducerWholeFunctions']==16 and len(P['functions'])==16)
check('No old whole-function approval transferred',P['counts']['exactOldWholeFunctions']==0 and P['counts']['diagnosticsWithExactPriorKey']==14)
check('JSON hazard excluded from all extension points',all(p['hazard']!='JSON_CONCAT'for p in M['points'])and M['excluded'][0]['reviewId']==23)
review={r['id']:r for r in R['tupleRows']+R['aggregateRows']+R['uuidRows']}
for point in M['points']:
 for n in point['reviewIds']:
  r=review[n]
  check('Point signature/body/statement/key '+str(n),all(point[x]==r[y]for x,y in [('signature','signature'),('bodySha256','bodySha256'),('completeFunctionSha256','routineSha256'),('statementSha256','statementSha256'),('keySha256','keySha256')]))
check('26 exact one-relation declared whole-row proofs',len(R['tupleRows'])==26 and all(r['proof']['verdict']=='FINITE_SOURCE_DECLARED_TUPLE_TYPE_PROVED'for r in R['tupleRows']))
check('64 scalar input expressions59 old plus5fresh',sum(r['proof']['aggregateCount']for r in R['aggregateRows'])==64 and sum(not a['priorExpressionEqual']for r in R['aggregateRows']for a in r['proof']['aggregates'])==5)
check('Four UUID statements preserve narrow guards',len(R['uuidRows'])==4 and [r['id']for r in R['uuidRows']]==[6,16,17,29])
original=frozen(C['target']).decode('utf-8');start=original.index('create or replace function private.invoice_daily_rate_actual_revision_assert_v1(');end=original.index('$function$;',start)+len('$function$;')
before=original[start:end]+'\n';after=(HERE/C['complete_successor']).read_text(encoding='utf-8')
check('Complete predecessor source hash',sha(before)==C['complete_before_sha256'])
check('Complete successor source hash',sha(after)==C['complete_successor_sha256'])
check('Only exact grouping change in complete successor',after.replace(C['exactReplacement']['after'],C['exactReplacement']['before'])==before)
check('One CREATE statement parses under PG17',len(json.loads(parser.parse_sql_json(after))['stmts'])==1)
def expression_ast(s):
 e=re.search(r'\brefs\s*:=\s*(.*?);',s,re.S)[1]
 tree=json.loads(parser.parse_sql_json('select '+e))
 def clean(x):
  if isinstance(x,dict):return{k:clean(v)for k,v in x.items()if k not in('location','stmt_location','stmt_len')}
  if isinstance(x,list):return[clean(v)for v in x]
  return x
 return clean(tree)
check('Original and explicitly grouped expression PG17 AST equal',expression_ast(before)==expression_ast(after))
check('Expression AST equals exact receipt digest',sha(json.dumps(expression_ast(before),sort_keys=True,separators=(',',':')))==C['expression_ast_sha256'])
check('No permissions or unrelated SQL added',after.count('create or replace function')==1 and not re.search(r'\b(grant|revoke|alter\s+function)\b',after,re.I))
result={'sourceCommit':COMMIT,'qualification':'OFFLINE_SOURCE_PROVENANCE_TYPE_CARDINALITY_AND_PG17_GRAMMAR_ONLY','parser':{'pglastVersion':pglast.__version__,'postgresqlVersion':pglast.get_postgresql_version()},'checks':checks,'passed':len(checks),'native':'NOT_RUN','dbConnection':'NOT_OPENED','rootFilesEdited':False,'genericFencesEdited':False}
(HERE/'CHECKS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'passed':len(checks),'checksSha256':sha((HERE/'CHECKS.json').read_bytes()),'native':'NOT_RUN'},indent=2))

# PG17 grammar/AST proof; no database or native catalogue execution.
from pathlib import Path
import sys,json,copy
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parser
P=Path(__file__).resolve().parent/'source'
def clean(x):
 if isinstance(x,list):return [clean(v) for v in x]
 if isinstance(x,dict):return {k:clean(v) for k,v in x.items() if k not in ('location','stmt_location','stmt_len')}
 return x
def parse(sql):return clean(json.loads(parser.parse_sql_json(sql)))
def text(n):return (P/(n+'.sql')).read_text(encoding='utf8')
def tree(n,fields=False):
 sql=text(n)
 if fields:sql='select '+sql
 else:
  for k in ['hr_contact_audit_bag','hr_contact_other_bag']:
   assert sql.count(":'"+k+"'")==1;sql=sql.replace(":'"+k+"'","'[]'")
 return parse(sql)
def row(node):
 found=[]
 def walk(x):
  if isinstance(x,dict):
   if 'FuncCall' in x:
    f=x['FuncCall'];names=[a.get('String',{}).get('sval') for a in f.get('funcname',[])]
    if names==['jsonb_agg'] and f.get('args') and 'batch_receipt_id' in json.dumps(f['args'][0]) and 'keyHash' in json.dumps(f['args'][0]):found.append((f['args'],0))
   for v in x.values():walk(v)
  elif isinstance(x,list):
   for v in x:walk(v)
 walk(node);assert len(found)==1,len(found);return found[0]
def get(n):a,i=row(n);return a[i]
def replace(n,value):n=copy.deepcopy(n);a,i=row(n);a[i]=copy.deepcopy(value);return n
def pairs(n):
 f=n['FuncCall'];assert [x['String']['sval'] for x in f['funcname']]==['jsonb_build_object'];a=f['args'];assert len(a)%2==0
 return {a[i]['A_Const']['sval']['sval']:a[i+1] for i in range(0,len(a),2)}
ref=tree('REFERENCE_FIELDS',True);before=tree('BEFORE_FIELDS',True);after=tree('AFTER_FIELDS',True);oldrow=get(ref);old=pairs(oldrow)
assert list(pairs(get(before)))==['keyHash','rowHash'];assert pairs(get(before))=={k:old[k] for k in ['keyHash','rowHash']}
a=get(after)['A_Expr'];assert a['name']==[{'String':{'sval':'||'}}]
assert pairs(a['lexpr'])=={k:old[k] for k in ['keyHash','rowHash','xmin','xactStatus']}
case=a['rexpr']['CaseExpr'];assert len(case['args'])==1 and 'arg' not in case
arm=case['args'][0]['CaseWhen'];assert pairs(arm['result'])=={k:old[k] for k in ['nativeClaimProved','createdAt','updatedAt','completedAt']}
assert case['defresult']==parse("select '{}'::jsonb")['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']
assert arm['expr']==parse("select (r.xmin::text::numeric >= 500::numeric or r.xmin::text = any(array['499']::text[]))")['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']
assert 'CoalesceExpr' in pairs(arm['result'])['nativeClaimProved']
for x in [before,after]:assert replace(x,oldrow)==ref
for n in ['BEFORE_FINAL','AFTER_FINAL']:assert replace(tree(n),oldrow)==tree('REFERENCE_FINAL')
# Every original outbox AST, all complete hash/count branches and sort keys remain exact.
expected=after
negative=[text('AFTER_FIELDS').replace('>= 500::numeric','>= 501::numeric'),text('AFTER_FIELDS').replace("array['499']::text[]","array[]::text[]"),text('AFTER_FIELDS').replace("else '{}'::jsonb","else jsonb_build_object('nativeClaimProved',true)"),text('AFTER_FIELDS').replace('case when (r.xmin','case when not (r.xmin',1),text('AFTER_FIELDS').replace("'rowHash'","'otherHash'",1),text('AFTER_FIELDS').replace('coalesce((', 'true or coalesce((',1),text('AFTER_FIELDS')+' where false',text('AFTER_FIELDS').replace('order by r.batch_receipt_id','order by r.created_at_utc')]
for sql in negative:assert parse('select '+sql)!=expected
print(json.dumps({'status':'PASS_GRAMMAR_AST_ONLY','fieldTrees':3,'wholeFinalTrees':3,'originalEligiblePAndTimestampsExact':True,'allAfterXminStatusExact':True,'outboxAndAllOtherASTExact':True,'negativeASTs':8,'psqlBags':'TWO_GRAMMAR_ONLY_PLACEHOLDERS_NOT_NATIVE_DATA','native':'NOT_RUN'}))

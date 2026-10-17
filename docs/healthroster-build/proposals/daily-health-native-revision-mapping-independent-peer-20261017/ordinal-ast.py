"""Actual Health SELECT/guard AST with controlled rows; no SQL/native execution."""
from pathlib import Path
import sys,json,copy,hashlib
HERE=Path(__file__).resolve().parent
sys.path.insert(0,r'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
raw=(HERE/'frozen/health/10_revision_apply.sqlinc').read_bytes();sql=raw.decode()
fn=pglast.parse_plpgsql(sql)[0]['PLpgSQL_function'];body=fn['action']['PLpgSQL_stmt_block']['body']
selected=[(i,n['PLpgSQL_stmt_execsql']) for i,n in enumerate(body) if 'PLpgSQL_stmt_execsql' in n and 'select v.revision_no' in str(n)]
assert len(selected)==1
at,statement=selected[0];assert statement['into'] and statement['strict']
assert statement['target']['PLpgSQL_row']['fields'][0]['name']=='accepted_revision_no'
query=statement['sqlstmt']['PLpgSQL_expr']['query'];select=pglast.parse_sql(query)[0].stmt
guard=body[at+1]['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'];predicate=pglast.parse_sql('select '+guard)[0].stmt.targetList[0].val
assert 'accepted_revision_no bigint' in sql
publisher=next(i for i,n in enumerate(body) if 'publication:=private.bpay_next_daily_rate_correction_publish_v1' in str(n))
receipt=next(i for i,n in enumerate(body) if "receipt:=jsonb_build_object" in str(n))
assert publisher<at<receipt and "'placement_revision',accepted_revision_no::text" in str(body[receipt])
def evaluate(node,c):
    if isinstance(node,ast.ColumnRef):
        value=c
        for field in node.fields:value=value[field.sval]
        return value
    if isinstance(node,ast.A_Const):
        if node.isnull:return None
        if isinstance(node.val,ast.String):return node.val.sval
        if isinstance(node.val,ast.Integer):return node.val.ival
        raise AssertionError(type(node.val))
    if isinstance(node,ast.NullTest):return (evaluate(node.arg,c) is None)==(node.nulltesttype.name=='IS_NULL')
    if isinstance(node,ast.BoolExpr):
        values=[evaluate(n,c) for n in node.args]
        if node.boolop.name=='AND_EXPR':return False if False in values else None if None in values else True
        if node.boolop.name=='OR_EXPR':return True if True in values else None if None in values else False
        raise AssertionError(node.boolop.name)
    if isinstance(node,ast.A_Expr):
        left,right=evaluate(node.lexpr,c),evaluate(node.rexpr,c)
        if left is None or right is None:return None
        op=node.name[0].sval
        if op=='=':return left==right
        if op=='<=':return left<=right
        raise AssertionError(op)
    raise AssertionError(type(node))
base={'native_apply':{'revision_id':'revA','work_id':'workA','financial':'finA'},'o':{'from_timesheet_id':'tsA','candidate_id':'candidateA'},'m':{'from_version':3,'original_booking_id':'bookingA'},
 'v':{'id':'revA','work_id':'workA','revision_no':1,'financial_snapshot_id':'finA','physical_timesheet_id':'tsA','physical_timesheet_version':3,'source_kind':'ORDINARY','approved_at_utc':'approved','sealed_at_utc':'sealed'},
 'w':{'id':'workA','current_revision_id':'revA','current_revision_no':1,'original_timesheet_id':'tsA','booking_id':'bookingA','candidate_id':'candidateA','contract_id':None,'origin_kind':'DAILY','work_kind':'ORDINARY','approval_state':'APPROVED'}}
join=select.fromClause[0].quals
def select_one(rows):
    matched=[r for r in rows if evaluate(join,r) is True and evaluate(select.whereClause,r) is True]
    if len(matched)!=1:return 'STRICT_CARDINALITY_REFUSAL',None
    value=evaluate(select.targetList[0].val,matched[0])
    if evaluate(predicate,{'accepted_revision_no':value}) is True:return 'ORDINAL_GUARD_REFUSAL',None
    return 'ACCEPTED_SOURCE_ASSOCIATION',str(value)
results=[]
for n in [1,9007199254740993,9223372036854775807]:
    c=copy.deepcopy(base);c['v']['revision_no']=n;c['w']['current_revision_no']=n
    status,value=select_one([c]);assert status=='ACCEPTED_SOURCE_ASSOCIATION' and value==str(n)
    results.append({'case':'canonical_native_int63_'+str(n),'status':status,'ordinal':value})
for owner,key,value in [('v','id','other'),('w','id','other'),('v','financial_snapshot_id','other'),('w','current_revision_id','other'),('w','current_revision_no',2),('v','physical_timesheet_id','other'),('v','physical_timesheet_version',4),('w','original_timesheet_id','other'),('w','booking_id','other'),('w','candidate_id','other'),('w','contract_id','contract'),('w','origin_kind','WEEKLY'),('w','work_kind','EXCEPTIONAL'),('w','approval_state','PENDING'),('v','source_kind','SOURCE'),('v','approved_at_utc',None),('v','sealed_at_utc',None)]:
    c=copy.deepcopy(base);c[owner][key]=value;status,ordinal=select_one([c]);assert status=='STRICT_CARDINALITY_REFUSAL' and ordinal is None
    results.append({'case':'wrong_'+owner+'_'+key,'status':status})
for name,rows in [('missing',[]),('ambiguous',[base,base])]:
    status,value=select_one(rows);assert status=='STRICT_CARDINALITY_REFUSAL';results.append({'case':name,'status':status})
for n in [0,-1]:
    c=copy.deepcopy(base);c['v']['revision_no']=n;c['w']['current_revision_no']=n
    status,value=select_one([c]);assert status=='ORDINAL_GUARD_REFUSAL';results.append({'case':'nonpositive_'+str(n),'status':status})
out={'sourceSha256':hashlib.sha256(raw).hexdigest(),'grammarStatements':len(pglast.parse_sql(sql)),'plpgsqlBodies':1,'actualSelectAstStrict':True,'publisherBeforeOrdinalBeforeReceipt':True,'controlledAstCases':results,'native':'NOT_RUN','limit':'Controlled rows evaluate the actual Health join/guard AST; no Bank implementation or native publication is proved.'}
(HERE/'ORDINAL_AST_RESULTS.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'cases':len(results),'grammar':'PG17','native':'NOT_RUN'}))

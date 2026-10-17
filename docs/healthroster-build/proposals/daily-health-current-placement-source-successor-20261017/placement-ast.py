"""Controlled evaluation of the authored Health selector/guards/projection AST.
No SQL, native producer, Bank implementation or database is executed.
"""
from pathlib import Path
import sys,json,copy,hashlib,uuid
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
BASE=Path(__file__).resolve().parent
raw=(BASE/'health/20_current_placement.sqlinc').read_bytes()
fn=pglast.parse_plpgsql(raw.decode())[0]['PLpgSQL_function']
body=fn['action']['PLpgSQL_stmt_block']['body']
while 'PLpgSQL_stmt_block' in body[0]:body=body[0]['PLpgSQL_stmt_block']['body']
select_stmt=next(n['PLpgSQL_stmt_execsql'] for n in body if 'PLpgSQL_stmt_execsql' in n and "'approval_refs',f.rate_source_refs_json" in str(n))
assert select_stmt['into'] and select_stmt['strict']
select=pglast.parse_sql(select_stmt['sqlstmt']['PLpgSQL_expr']['query'])[0].stmt
conditions=[pglast.parse_sql('select '+n['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val for n in body if 'PLpgSQL_stmt_if' in n]
placement_assignment=next(n['PLpgSQL_stmt_assign'] for n in body if 'PLpgSQL_stmt_assign' in n and "placement:=jsonb_build_object" in str(n))
expr=placement_assignment['expr']['PLpgSQL_expr']['query'].split(':=',1)[1]
projection=pglast.parse_sql('select '+expr)[0].stmt.targetList[0].val
def evaluate(n,c):
    if isinstance(n,ast.ColumnRef):
        value=c
        for f in n.fields:value=value[f.sval]
        return value
    if isinstance(n,ast.A_Const):
        if n.isnull:return None
        if isinstance(n.val,ast.String):return n.val.sval
        if isinstance(n.val,ast.Integer):return n.val.ival
        if isinstance(n.val,ast.Boolean):return n.val.boolval
        raise AssertionError(type(n.val))
    if isinstance(n,ast.TypeCast):
        value=evaluate(n.arg,c);kind=n.typeName.names[-1].sval
        if value is None:return None
        if kind=='text':return str(value)
        if kind in ['uuid']:return str(uuid.UUID(value))
        if kind in ['int4','int8']:return int(value)
        raise AssertionError(kind)
    if isinstance(n,ast.NullTest):return (evaluate(n.arg,c) is None)==(n.nulltesttype.name=='IS_NULL')
    if isinstance(n,ast.CoalesceExpr):return next((x for x in (evaluate(a,c) for a in n.args) if x is not None),None)
    if isinstance(n,ast.BoolExpr):
        values=[evaluate(a,c) for a in n.args]
        if n.boolop.name=='AND_EXPR':return False if False in values else None if None in values else True
        if n.boolop.name=='OR_EXPR':return True if True in values else None if None in values else False
        if n.boolop.name=='NOT_EXPR':return None if values[0] is None else not values[0]
        raise AssertionError(n.boolop.name)
    if isinstance(n,ast.A_Expr):
        left=evaluate(n.lexpr,c);op=n.name[0].sval
        if n.kind.name=='AEXPR_IN':
            values=[evaluate(a,c) for a in n.rexpr]
            return left not in values if op=='<>' else left in values
        right=evaluate(n.rexpr,c)
        if op in ['->','->>','#>>']:
            if op=='#>>' and isinstance(right,str):right=right.strip('{}').split(',')
            keys=right if isinstance(right,list) else [right]
            for key in keys:left=left.get(key) if isinstance(left,dict) else None
            return left if op=='->' or left is None or isinstance(left,str) else str(left)
        if n.kind.name=='AEXPR_DISTINCT':return left!=right
        if n.kind.name=='AEXPR_NOT_DISTINCT':return left==right
        if left is None or right is None:return None
        if op=='=':return left==right
        if op=='<>':return left!=right
        if op=='>':return left>right
        raise AssertionError((n.kind.name,op))
    if isinstance(n,ast.A_ArrayExpr):return [evaluate(a,c) for a in n.elements]
    if isinstance(n,ast.FuncCall):
        name=n.funcname[-1].sval;args=[evaluate(a,c) for a in n.args or []]
        if name=='jsonb_build_object':return dict(zip(args[::2],args[1::2]))
        if name=='jsonb_typeof':return 'object' if isinstance(args[0],dict) else 'string' if isinstance(args[0],str) else None
        if name=='pg_input_is_valid':
            if args[0] is None:return None
            try:uuid.UUID(args[0]);return True
            except (ValueError,TypeError,AttributeError):return False
        raise AssertionError(name)
    if isinstance(n,ast.SubLink):
        assert n.subLinkType.name=='EXISTS_SUBLINK'
        # Member association: evaluate the exact correlated source WHERE against
        # supplied member rows, without substituting its predicate.
        q=n.subselect;table=q.fromClause[0]
        assert isinstance(table,ast.RangeVar) and table.relname=='timesheet_contract_move_members'
        return any(evaluate(q.whereClause,{**c,table.alias.aliasname:m}) is True for m in c['memberRows'])
    raise AssertionError(type(n))
def joins(n,c):
    if isinstance(n,ast.RangeVar):return True
    assert isinstance(n,ast.JoinExpr)
    return joins(n.larg,c) and joins(n.rarg,c) and evaluate(n.quals,c) is True
def selected(rows):
    rows=[c for c in rows if joins(select.fromClause[0],c) and evaluate(select.whereClause,c) is True]
    if len(rows)!=1:raise ValueError('STRICT_CARDINALITY_REFUSAL')
    return evaluate(select.targetList[0].val,rows[0])
U=lambda n:str(uuid.UUID(int=n))
def fixture(ordinal=10,current=1):
    physical,work,candidate,client=map(U,[1,2,3,4]);op,review,financial,revision,receipt,actor=map(U,[10+current,20+current,30+current,40+current,50+current,60+current])
    refs={'approval_purpose':'RATE_CORRECTION','correction_operation_id':op,'accepted_review_id':review,'approved_by_user_id':actor}
    return {'o':{'from_timesheet_id':physical,'candidate_id':candidate,'actual_client_id':client,'placement_receipt_json':{'work_id':work}},
      'w':{'id':work,'current_revision_id':revision,'current_revision_no':ordinal,'original_timesheet_id':physical,'booking_id':'DAILY:opaque-booking-001','candidate_id':candidate,'contract_id':None,'origin_kind':'DAILY','work_kind':'ORDINARY','approval_state':'APPROVED'},
      'v':{'id':revision,'work_id':work,'revision_no':ordinal,'financial_snapshot_id':financial,'physical_timesheet_id':physical,'physical_timesheet_version':3,'source_kind':'ORDINARY','approved_at_utc':'approved','sealed_at_utc':'sealed'},
      't':{'timesheet_id':physical,'booking_id':'DAILY:opaque-booking-001','version':3,'sheet_scope':'DAILY','contract_id':None,'is_current':True,'archived_at_utc':None,'revoked_at':None},
      'f':{'id':financial,'timesheet_id':physical,'timesheet_version':3,'is_current':True,'is_stale':False,'candidate_id':candidate,'client_id':client,'basis':'SELF_REPORTED','authorised_at_utc':'approved','rate_source_refs_json':refs},
      'basis':{'state':'COMPLETE'},'current_basis':{'state':'COMPLETE'},'current_operation':op,'current_receipt':receipt,
      'current_o':{'id':op,'from_timesheet_id':physical,'family_root_id':physical,'candidate_id':candidate,'actual_client_id':client,'accepted_review_id':review,'created_by_user_id':actor,'placement_receipt_json':{'receipt_id':receipt,'work_id':work,'revision_id':revision,'current_financial_id':financial,'current_timesheet_id':physical}},
      'memberRows':[{'operation_id':op,'from_timesheet_id':physical,'to_timesheet_id':physical,'to_contract_id':None,'from_contract_id':None,'to_financial_id':financial,'to_version':3,'original_booking_id':'DAILY:opaque-booking-001'}]}
results=[];public_cases=[]
def run(c):
    c=copy.deepcopy(c);c['current_binding']=selected([c])
    # First six exact IFs are committed, provenance, canonical operation,
    # current operation/member receipt, canonical receipt, then source recheck.
    for i,condition in enumerate(conditions[:5]):
        if evaluate(condition,c) is True:raise ValueError('SOURCE_GUARD_'+str(i))
    return evaluate(projection,c)
original_receipt={'placement_revision':'1','work_id':U(2),'revision_id':U(41),'current_financial_id':U(31),'receipt_id':U(51)}
retained=copy.deepcopy(original_receipt)
for name,n,step in [('A_current',10,1),('A_after_B',11,2),('A_after_C',12,3),('A_after_new_A',13,4),('large_int63',9007199254740993,5),('max_int63',9223372036854775807,6)]:
    c=fixture(n,step);p=run(c);assert p['placement_revision']==str(n) and p['receipt_id']==c['current_receipt']
    assert p['receipt_id']!=original_receipt['receipt_id'] or step==1
    assert original_receipt==retained
    results.append({'case':name,'outcome':'CURRENT_HEALTH_PLACEMENT','placement':p,'acceptedReceiptUnchanged':True});public_cases.append({'case':name,'placement':p})
for field in ['placement_revision']:
    c=fixture();c['current_o']['placement_receipt_json'][field]='wrong-old-operation-billing-scalar'
    assert run(c)['placement_revision']=='10'
    del c['current_o']['placement_receipt_json'][field]
    assert run(c)['placement_revision']=='10'
    results.append({'case':'historical_receipt_scalar_missing_or_obsolete_ignored','outcome':'CURRENT_NATIVE_ORDINAL'})
mutations=[('w','current_revision_id',U(999)),('w','current_revision_no',999),('w','booking_id','wrong-booking'),('w','candidate_id',U(999)),('w','origin_kind','WEEKLY'),('w','approval_state','PENDING'),('v','work_id',U(999)),('v','financial_snapshot_id',U(999)),('v','revision_no',0),('v','revision_no',-1),('v','approved_at_utc',None),('v','sealed_at_utc',None),('t','timesheet_id',U(999)),('t','contract_id',U(999)),('t','is_current',False),('t','archived_at_utc','archived'),('t','revoked_at','revoked'),('f','is_current',False),('f','is_stale',True),('f','candidate_id',U(999)),('f','client_id',U(999)),('f','timesheet_version',4),('f','basis','SOURCE'),('f','authorised_at_utc',None)]
for owner,key,value in mutations:
    c=fixture();c[owner][key]=value
    try:run(c);raise AssertionError('unexpected acceptance '+owner+'.'+key)
    except ValueError as e:results.append({'case':'refuse_'+owner+'_'+key,'outcome':str(e)})
for owner,key,value in [('rate_source_refs_json','approval_purpose','ORDINARY'),('rate_source_refs_json','correction_operation_id',None),('rate_source_refs_json','correction_operation_id','malformed'),('rate_source_refs_json','accepted_review_id',U(999)),('rate_source_refs_json','approved_by_user_id',U(999))]:
    c=fixture();c['f'][owner][key]=value
    try:run(c);raise AssertionError('unexpected provenance acceptance '+key)
    except ValueError as e:results.append({'case':'refuse_provenance_'+key+'_'+str(value),'outcome':str(e)})
for key,value in [('state','REVIEWED')]:
    c=fixture();c['current_basis'][key]=value
    try:run(c);raise AssertionError('unexpected state acceptance')
    except ValueError as e:results.append({'case':'refuse_current_'+key,'outcome':str(e)})
for key,value in [('work_id',U(999)),('revision_id',U(999)),('current_financial_id',U(999)),('current_timesheet_id',U(999)),('receipt_id','malformed')]:
    c=fixture();c['current_o']['placement_receipt_json'][key]=value
    try:run(c);raise AssertionError('unexpected receipt acceptance '+key)
    except ValueError as e:results.append({'case':'refuse_receipt_'+key,'outcome':str(e)})
for key,value in [('to_financial_id',U(999)),('original_booking_id','different-booking'),('to_version',4)]:
    c=fixture();c['memberRows'][0][key]=value
    try:run(c);raise AssertionError('unexpected member acceptance '+key)
    except ValueError as e:results.append({'case':'refuse_member_'+key,'outcome':str(e)})
for rows in [[],[fixture(),fixture()]]:
    try:selected(rows);raise AssertionError('unexpected cardinality')
    except ValueError as e:results.append({'case':'strict_cardinality_'+str(len(rows)),'outcome':str(e)})
(BASE/'PLACEMENT_AST_RESULTS.json').write_bytes((json.dumps({'version':'CONTROLLED_ACTUAL_HEALTH_SQL_AST_ONLY_V1','sourceSha256':hashlib.sha256(raw).hexdigest(),'parser':pglast.__version__,'postgres':pglast.get_postgresql_version(),'cases':results,'publicDecoderCases':public_cases,'native':'NOT_RUN','BankingProducer':'NOT_PROVIDED','limit':'Controlled source inputs evaluate actual Health selector and five guards/projection. The existing native basis and final multi-snapshot recheck require separate native proof.'},indent=2)+'\n').encode())
print(json.dumps({'cases':len(results),'publicCases':len(public_cases),'native':'NOT_RUN'}))

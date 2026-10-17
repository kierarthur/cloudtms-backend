"""Derive the finite F1 source replay from the immutable original test engine."""
from pathlib import Path
import subprocess
BASE=Path(__file__).resolve().parent
repo='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026'
prefix='docs/healthroster-build/proposals/daily-health-current-placement-source-successor-20261017/'
def saved(file):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,'show','040bb062b9b0fdf3fa6bb45e400494d65929f40a:'+prefix+file]).decode()
s=saved('placement-ast.py')
s=s.replace("conditions[:5]","conditions[:-1]")
s=s.replace("# First six exact IFs are committed, provenance, canonical operation,\n    # current operation/member receipt, canonical receipt, then source recheck.","# Evaluate every exact direct guard except the final multi-snapshot/native\n    # basis EXISTS recheck, which remains a separate native obligation.")
s=s.replace("if kind in ['int4','int8']:return int(value)","if kind in ['int4','int8']:return int(value)")
s=s.replace("if isinstance(n,ast.CoalesceExpr):", "if isinstance(n,ast.BooleanTest):\n        value=evaluate(n.arg,c)\n        if n.booltesttype.name=='IS_NOT_TRUE':return value is not True\n        raise AssertionError(n.booltesttype.name)\n    if isinstance(n,ast.CoalesceExpr):")
s=s.replace("if n.kind.name=='AEXPR_DISTINCT':return left!=right", "if n.kind.name=='AEXPR_NULLIF':return None if left==right else left\n        if n.kind.name=='AEXPR_DISTINCT':return left!=right")
s=s.replace("if name=='jsonb_typeof':", "if name=='upper':return None if args[0] is None else args[0].upper()\n        if name=='btrim':return None if args[0] is None else args[0].strip(' ')\n        if name=='jsonb_typeof':")
s=s.replace("'revoked_at':None}","'revoked_at':None,'line_type':'HOURS','is_adjustment':False,'parent_timesheet_id':None,'correction_id':None,'adjustment_origin':None,'correction_kind':None,'authorised_at_server':'approved','worked_start_iso':'2026-10-01T09:00:00Z','worked_end_iso':'2026-10-01T17:00:00Z','job_title_norm':'rn','band':'5'}")
s=s.replace("'rate_source_refs_json':refs}","'rate_source_refs_json':refs,'role':'RN','band':'5'}")
s=s.replace("'current_operation':op,'current_receipt':receipt,", "'current_operation':op,'current_receipt':receipt,\n      'native_context':{'effective_timesheet_id':physical,'out_candidate':{'id':candidate},'out_client_id':client,'out_cur_fin':{'id':financial}},\n      'native_checked':{'effective_timesheet_id':physical,'out_candidate':{'id':candidate},'out_client_id':client,'out_cur_fin':{'id':financial}},")
marker="(BASE/'PLACEMENT_AST_RESULTS.json').write_bytes"
at=s.index(marker)
extra='''
# F1 source counterexamples use the actual old selector and guards as well.
old_raw=(BASE/'reference/parent/health/20_current_placement.sqlinc').read_bytes()
old_fn=pglast.parse_plpgsql(old_raw.decode())[0]['PLpgSQL_function'];old_body=old_fn['action']['PLpgSQL_stmt_block']['body']
while 'PLpgSQL_stmt_block' in old_body[0]:old_body=old_body[0]['PLpgSQL_stmt_block']['body']
old_select=pglast.parse_sql(next(n['PLpgSQL_stmt_execsql']['sqlstmt']['PLpgSQL_expr']['query'] for n in old_body if 'PLpgSQL_stmt_execsql' in n and "'approval_refs',f.rate_source_refs_json" in str(n)))[0].stmt
old_conditions=[pglast.parse_sql('select '+n['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val for n in old_body if 'PLpgSQL_stmt_if' in n]
def old_accepts(c):
    c=copy.deepcopy(c)
    if not joins(old_select.fromClause[0],c) or evaluate(old_select.whereClause,c) is not True:return False
    c['current_binding']=evaluate(old_select.targetList[0].val,c)
    return not any(evaluate(q,c) is True for q in old_conditions[:-1])
counterexamples=[]
changes=[('native_context','effective_timesheet_id',U(999)),('native_context','out_candidate',{'id':U(999)}),('native_context','out_client_id',U(999)),('native_context','out_cur_fin',{'id':U(999)}),('native_checked','effective_timesheet_id',U(999)),('native_checked','out_candidate',{'id':U(999)}),('native_checked','out_client_id',U(999)),('native_checked','out_cur_fin',{'id':U(999)}),('t','line_type','EXPENSE'),('t','is_adjustment',True),('t','parent_timesheet_id',U(999)),('t','correction_id',U(999)),('t','adjustment_origin','adjusted'),('t','correction_kind','CORRECTION'),('t','authorised_at_server',None),('t','worked_start_iso',None),('t','worked_end_iso','2026-10-01T08:00:00Z'),('t','job_title_norm','hca'),('t','band','6')]
for owner,key,value in changes:
    c=fixture();c[owner][key]=value
    assert old_accepts(c),(owner,key,'old counterexample not reproduced')
    try:run(c);raise AssertionError('F1 counterexample unexpectedly accepted '+owner+'.'+key)
    except ValueError as e:
        counterexamples.append({'case':owner+'.'+key,'original040bb':'ACCEPTS_CONTROLLED_SOURCE_ROWS','forward':'REFUSED','reason':str(e)})
        results.append({'case':'F1_'+owner+'_'+key,'outcome':str(e)})
'''
s=s[:at]+extra+s[at:]
s=s.replace("'cases':results,'publicDecoderCases'", "'cases':results,'F1Counterexamples':counterexamples,'publicDecoderCases'")
s=s.replace("selector/first five guards/projection", "selector/non-final direct guards/projection")
s=s.replace("print(json.dumps({'cases':len(results),'publicCases':len(public_cases),'native':'NOT_RUN'}))", "print(json.dumps({'cases':len(results),'F1Counterexamples':len(counterexamples),'publicCases':len(public_cases),'native':'NOT_RUN'}))")
(BASE/'placement-ast.py').write_text(s)
g=saved('grammar-and-types.py')
g=g.replace("BASE/'reference/SCHEMA_TYPES.json'","BASE/'reference/040bb/reference/SCHEMA_TYPES.json'")
g=g.replace("ddl_file='reference/root/", "ddl_file='reference/040bb/reference/root/")
g=g.replace("alias={'w'", "native_file='reference/root/supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql'\nnative_bytes=(BASE/native_file).read_bytes()\nnative_decl=pglast.parse_sql(native_bytes.decode())[0].stmt\nnative_fields={p.name:'.'.join(n.sval for n in p.argType.names) for p in native_decl.parameters if p.mode.name=='FUNC_PARAM_TABLE'}\nassert all(n in native_fields for n in ['effective_timesheet_id','out_candidate','out_client_id','out_cur_fin'])\nalias={'w'")
g=g.replace("if owner in alias:\n", "if owner in ['native_context','native_checked']:\n                assert column in native_fields,(owner,column)\n                refs.add(('public.tsfin_load_context_batch RETURN TABLE',column,native_fields[column]))\n            if owner in alias:\n")
g=g.replace("'aliasScopes':alias_scopes,", "'aliasScopes':alias_scopes,'actualNativeReturnTable':{'file':native_file,'sha256':sha(native_bytes),'fields':native_fields},")
g=g.replace("native_decl=pglast.parse_sql(native_bytes.decode())[0].stmt", "native_decl=pglast.parse_sql(native_bytes.decode())[0].stmt\nnative_body=next(o.arg[0].sval for o in native_decl.options if o.defname=='as')\nnative_body_ast=pglast.parse_sql(native_body)\nassert len(native_body_ast)==1 and isinstance(native_body_ast[0].stmt,ast.SelectStmt)")
g=g.replace("result={'version':", "native_write_types=(ast.InsertStmt,ast.UpdateStmt,ast.DeleteStmt,ast.MergeStmt,ast.CopyStmt)\nassert not any(isinstance(n,native_write_types) for n in ast_walk(native_body_ast))\nresult={'version':")
g=g.replace("'fields':native_fields}", "'fields':native_fields,'sqlBodyStatements':len(native_body_ast),'sqlBodyDirectWrites':0,'nestedExistingReadDependencies':'UNCHANGED_NOT_NEW_ABI_NOT_NATIVE_PROVED'}")
(BASE/'grammar-and-types.py').write_text(g)
print('Two finite test engines derived from exact Git040bb; no SQL/native execution.')

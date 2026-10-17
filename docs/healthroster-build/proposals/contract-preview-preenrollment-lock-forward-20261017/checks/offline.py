"""Source/PG17/FK conflict-premise proof only. Never executes SQL."""
from pathlib import Path
import copy
import sys, importlib.util, json, re, subprocess
from unittest.mock import patch
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent.parent
BASE = HERE.parent/'contract-preview-current-health-20261017'
REPO = HERE.parents[3]
ROOT = REPO.parent/'healthroster-install2-14102026'
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
spec = importlib.util.spec_from_file_location('corrected_contract_assembly',HERE/'assemble.py')
assembly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assembly)
checks = []
def check(name, fn):
    fn()
    checks.append(name)
state = assembly.verify()
checks.append('all-current-pins-full-template-scope-reverses-and-closed-four-owner-assembly')
original_read = Path.read_bytes
def refuses(name, file, transform):
    def changed(path):
        raw = original_read(path)
        return transform(raw) if path == HERE/file else raw
    with patch.object(Path,'read_bytes',changed):
        try:
            assembly.verify()
        except (AssertionError,ValueError,KeyError):
            checks.append(name)
            return
        raise AssertionError('Drift accepted: '+name)

refuses('FK-enrollment-before-boundary-refuses','templates/20_preview.sql.tmpl',
 lambda b:b.replace(b' @@BANK_PREVIEW@@',b' insert into private.timesheet_contract_move_operations(id) values(operation);\n @@BANK_PREVIEW@@'))
refuses('false-lock-scope-refuses','fragments/21_scope_locked_from_native_owner.plpgsql',lambda b:b.replace(b'constant boolean:=true',b'constant boolean:=false'))
refuses('missing-scope-splice-refuses','fragments/21_locked_preview_enrollment.plpgsql',lambda b:b.replace(b'perform 1 from public.timesheets_financials',b'perform 2 from public.timesheets_financials'))
refuses('early-review-members-refuses','fragments/21_locked_preview_enrollment.plpgsql',lambda b:b'perform private.timesheet_contract_move_review_members_v1(o.id,before_scope);\n'+b)
refuses('extra-native-owner-change-refuses','reference/private__timesheet_contract_move_family_lock_v1.sqlinc',lambda b:b.replace(b'for update;',b'for share;',1))
refuses('pretend-available-owner-flag-refuses','OWNER_BLOCKS.json',lambda b:b.replace(b'"UNPROVIDED"',b'"AVAILABLE"'))
refuses('fifth-owner-slot-refuses','OWNER_BLOCKS.json',lambda b:json.dumps({**json.loads(b),'bindings':{**json.loads(b)['bindings'],'BANK_PRELOCK':{'status':'UNPROVIDED'}}}).encode())
refuses('scope-derivation-baseline-refresh-refuses','SCOPE_BLOCK_DERIVATION.json',lambda b:b.replace(b'"CONSTANT_TRUE"',b'"FALSE"').replace(b'"afterOffset": 0',b'"afterOffset": 1',1))

preview = assembly.text(HERE/'templates/20_preview.sql.tmpl')
fragment = assembly.text(HERE/'fragments/21_locked_preview_enrollment.plpgsql')
# Refusal is substituted only in this in-memory grammar input. No owner prefix,
# financial result or runnable SQL is emitted or claimed by this parser proof.
grammar = preview.replace('@@BANK_PREVIEW@@',fragment+"\n raise exception 'UNPROVIDED_OWNER_GRAMMAR_ONLY' using errcode='55000';")
parsed = pglast.parse_sql(grammar)
programs = pglast.parse_plpgsql(grammar)
assert len(programs)==1 and len(parsed)==2
checks.append('exact-corrected-complete-Preview-and-fixed-scope-enrollment-splice-PG17-grammar')

schema = (ROOT/'supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql').read_text()
schema = re.sub(r'^\\[^\r\n]*','',schema,flags=re.M)
tables = [row.stmt for row in pglast.parse_sql(schema) if isinstance(row.stmt,ast.CreateStmt)]
operation = next(row for row in tables if row.relation.relname=='timesheet_contract_move_operations')
candidate = next(row for row in operation.tableElts if isinstance(row,ast.ColumnDef) and row.colname=='candidate_id')
fk = next(row for row in candidate.constraints if row.contype.name=='CONSTR_FOREIGN')
assert fk.pktable.relname=='candidates' and not fk.deferrable and not fk.initdeferred
checks.append('actual-schema-Candidate-FK-is-immediate-no-schema-change')

# The lock conflict premise comes from actual source ordering, not a native race.
scope = assembly.text(HERE/'fragments/21_scope_locked_from_native_owner.plpgsql')
family = assembly.text(HERE/'reference/private__timesheet_contract_move_family_lock_v1.sqlinc')
assert "if p_lock then placement:=private.timesheet_contract_move_family_lock_v1(o.from_timesheet_id);" in scope
assert 'perform 1 from public.candidates c where c.id=candidate for update;' in family
assert fragment.index(scope) < fragment.index('insert into private.timesheet_contract_move_operations')
assert scope.index('timesheet_contract_move_family_lock_v1') < scope.index('perform 1 from public.contracts v where v.id') < scope.index('perform 1 from public.timesheets_financials')
checks.append('complete-genuine-Health-Candidate-Contract-Timesheet-TSFIN-lock-scope-precedes-all-FK-enrollment')

# Evaluate the actual new post-wait guard AST. Native hash/permission results
# are supplied synthetic boundary values: neither algorithm nor native access
# authority is replaced or claimed by these correlation vectors.
guard_text=fragment[fragment.index(' if o.id'):fragment.index(" then\n  raise exception 'REVIEW_STALE'",fragment.index(' if o.id'))]
guard=pglast.parse_sql('select '+guard_text.removeprefix(' if '))[0].stmt.targetList[0].val
assert isinstance(guard,ast.BoolExpr) and guard.boolop.name=='OR_EXPR' and len(guard.args)==15
def evaluate(node,context):
    if isinstance(node,ast.A_Const):
        if node.isnull:return None
        return node.val.sval if isinstance(node.val,ast.String) else node.val.boolval if isinstance(node.val,ast.Boolean) else node.val.ival
    if isinstance(node,ast.ColumnRef):
        value=context
        for part in node.fields:value=value.get(part.sval) if isinstance(value,dict) else None
        return value
    if isinstance(node,ast.TypeCast):return evaluate(node.arg,context)
    if isinstance(node,ast.BoolExpr):
        values=[evaluate(arg,context) for arg in node.args]
        return any(values) if node.boolop.name=='OR_EXPR' else all(values) if node.boolop.name=='AND_EXPR' else not values[0]
    if isinstance(node,ast.A_Expr):
        left,right=evaluate(node.lexpr,context),evaluate(node.rexpr,context)
        if node.kind.name=='AEXPR_DISTINCT':return left!=right
        name=node.name[0].sval
        if name in ('->','->>'):return left.get(right) if isinstance(left,dict) else None
        if name=='=':return left is not None and right is not None and left==right
        raise TypeError(name)
    if isinstance(node,ast.FuncCall):
        name='.'.join(p.sval for p in node.funcname);args=[evaluate(arg,context) for arg in node.args]
        if name in ('to_jsonb','encode'):return args[0]
        if name=='private.timesheet_contract_move_row_hash_v1':return context['native_hash_tokens'].get(args[0]['fixture_kind'])
        if name=='private.weekly_source_pay_actor_allowed_v1':return context['current_wp_pp']
        if name=='jsonb_array_elements':return args[0]
        raise TypeError(name)
    if isinstance(node,ast.SubLink):
        select=node.subselect;source=select.fromClause[0]
        if isinstance(source,ast.RangeVar):
            assert source.schemaname=='public' and source.relname=='tms_users'
            rows=context['current_users'];alias=source.alias.aliasname
        else:
            assert isinstance(source,ast.RangeFunction)
            rows=evaluate(source.functions[0][0],context);alias=source.alias.aliasname
        matches=[{**context,alias:row} for row in rows if evaluate(select.whereClause,{**context,alias:row})]
        return bool(matches) if node.subLinkType.name=='EXISTS_SUBLINK' else evaluate(select.targetList[0].val,matches[0]) if matches else None
    raise TypeError(type(node))
ctx=dict(operation='operation',actor='actor',reason='Correcting the Contract',actor_hash='user-hash',current_wp_pp=True,
         source=dict(timesheet_id='source',contract_id='old',fixture_kind='source'),
         destination=dict(id='destination',fixture_kind='destination'),original_contract=dict(candidate_id='candidate',client_id='client'),
         o=dict(id='operation',purpose='CONTRACT_CORRECTION',from_timesheet_id='source',from_contract_id='old',to_contract_id='destination',
                candidate_id='candidate',actual_client_id='client',created_by_user_id='actor',reason='Correcting the Contract',family_root_id='root'),
         before_scope=dict(operation_id='operation',family_root_id='root',destination_contract_sha256='destination-hash',
                           members=[dict(from_timesheet_id='source',source_row_sha256='source-hash')]),
         current_users=[dict(id='actor',is_active=True,fixture_kind='user')],
         native_hash_tokens={'source':'source-hash','destination':'destination-hash','user':'user-hash'})
assert evaluate(guard,ctx) is False
checks.append('actual-post-wait-guard-AST-exact-current-boundary-allowed')
for field in ['id','purpose','from_timesheet_id','from_contract_id','to_contract_id','candidate_id','actual_client_id','created_by_user_id','reason','family_root_id']:
    changed=copy.deepcopy(ctx);changed['o'][field]='changed'
    assert evaluate(guard,changed) is True
    checks.append('actual-post-wait-guard-AST-wrong-operation-identity:'+field)
for field in ['operation_id','family_root_id','destination_contract_sha256']:
    changed=copy.deepcopy(ctx);changed['before_scope'][field]='changed'
    assert evaluate(guard,changed) is True
    checks.append('actual-post-wait-guard-AST-wrong-locked-scope:'+field)
for kind in ['source_row','missing_member','current_actor_hash','revoked_actor','inactive_actor','unknown_permission']:
    changed=copy.deepcopy(ctx)
    if kind=='source_row':changed['native_hash_tokens']['source']='changed'
    elif kind=='missing_member':changed['before_scope']['members']=[]
    elif kind=='current_actor_hash':changed['native_hash_tokens']['user']='changed'
    elif kind=='revoked_actor':changed['current_wp_pp']=False
    elif kind=='inactive_actor':changed['current_users'][0]['is_active']=False
    else:changed['current_wp_pp']=None
    assert evaluate(guard,changed) is True
    checks.append('actual-post-wait-guard-AST-refuses:'+kind)
def may_grant(held, actor, mode):
    return all(owner==actor or mode==other=='KEY_SHARE' for owner,other in held)
# Original counterexample: compatible immediate FK checks, then both upgrades wait.
old=[('A','KEY_SHARE'),('B','KEY_SHARE')]
assert not may_grant(old,'A','UPDATE') and not may_grant(old,'B','UPDATE')
# Corrected Health boundary: the first Candidate UPDATE excludes the other
# before its FK check. This removes F1's two-KEY-SHARE upgrade premise only.
new=[('A','UPDATE')]
assert may_grant(new,'A','KEY_SHARE') and not may_grant(new,'B','UPDATE')
checks.append('two-distinct-request-F1-wait-graph-premise-removed-by-source-order-model-only')

attempt=subprocess.run([sys.executable,str(HERE/'assemble.py'),'--emit',str(HERE/'forbidden-runtime.sql')],cwd=REPO,capture_output=True)
assert attempt.returncode==2 and not (HERE/'forbidden-runtime.sql').exists()
checks.append('runtime-emission-remains-refused-before-write')
protocol=subprocess.run(['node',str(BASE/'protocol-vectors.mjs')],cwd=REPO,capture_output=True,text=True,encoding='utf8')
assert protocol.returncode==0,protocol.stderr
assert json.loads(protocol.stdout)['assertions']==109
checks.append('actual-unchanged-V3-protocol109-preserved')
worker=subprocess.run(['node','--test','--test-reporter=tap','tests/contract-correction-v3.test.mjs'],cwd=ROOT,capture_output=True,text=True,encoding='utf8')
assert worker.returncode==0 and '# pass 12' in worker.stdout
checks.append('actual-current-Worker12-preserved')
result=dict(passed=len(checks),failed=0,checks=checks,assembly=state,grammarOnly=dict(parser=pglast.__version__,PostgreSQLGrammar='17.7',statements=len(parsed),programs=len(programs)),
            F1='FK_UPGRADE_PREMISE_REMOVED_IN_FIXED_HEALTH_SOURCE',BankPrefixClosure='NOT_PROVIDED',
            deadlockFreedom='NOT_PROVED',native='NOT_RUN',races='NOT_RUN',runtimeSQLFilesEmitted=0,original43FilesUnchanged=True)
result['postWaitGuardASTBranches']=15
result['nativeHashPermissionValues']='SYNTHETIC_BOUNDARY_TOKENS_NO_NATIVE_AUTHORITY_PROOF'
(HERE/'OFFLINE_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps({'passed':len(checks),'grammarPrograms':len(programs),'protocol':109,'currentWorker':12,'native':'NOT_RUN'}))

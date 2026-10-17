"""Finite tests of actual source predicates and complete owner/metadata boundaries.
The evaluator covers only these parsed read-admission predicates, never SQL execution.
"""
from pathlib import Path
import copy,json,re,sys,unittest
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
P=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
def doc(p):return json.loads((P/p).read_bytes())
S=doc('PAYLOAD_SELECTION.json');A=doc('ASSEMBLY.json');B={x['target']:(P/x['source']).read_bytes().decode('utf-8') for x in S['targets']}
def src(name):return B[next(v for k,v in A['ownerPages'].items() if name in k)]
def dicts(v):
 if isinstance(v,dict):
  yield v
  for x in v.values():yield from dicts(x)
 elif isinstance(v,list):
  for x in v:yield from dicts(x)
GROUP=pglast.parse_plpgsql(src('check_only_query_groups'))[0]
ROWS=pglast.parse_plpgsql(src('check_only_query_rows'))[0]
def statement(program,needle):
 found=[x['PLpgSQL_stmt_execsql']['sqlstmt']['PLpgSQL_expr']['query'] for x in dicts(program) if 'PLpgSQL_stmt_execsql' in x and needle in x['PLpgSQL_stmt_execsql']['sqlstmt']['PLpgSQL_expr']['query']]
 assert len(found)==1,(needle,len(found));return pglast.parse_sql(found[0])[0].stmt
def condition(program,needle):
 found=[x['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'] for x in dicts(program) if 'PLpgSQL_stmt_if' in x and needle in x['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query']]
 assert len(found)==1,(needle,len(found));return pglast.parse_sql('select '+found[0])[0].stmt.targetList[0].val
def tri_and(xs):return False if False in xs else None if None in xs else True
def tri_or(xs):return True if True in xs else None if None in xs else False
def value(n,c):
 if isinstance(n,ast.A_Const):return None if n.isnull else n.val.sval if isinstance(n.val,ast.String) else n.val.ival if isinstance(n.val,ast.Integer) else n.val.boolval
 if isinstance(n,ast.ColumnRef):
  fields=[f.sval for f in n.fields];v=c
  for f in fields:v=v.get(f) if isinstance(v,dict) else None
  return v
 if isinstance(n,ast.TypeCast):
  v=value(n.arg,c);t=n.typeName.names[-1].sval
  return int(v) if v is not None and t in ['int4','int8','integer','bigint'] else v
 if isinstance(n,ast.BoolExpr):
  xs=[value(x,c) for x in n.args];return tri_and(xs) if n.boolop.name=='AND_EXPR' else tri_or(xs) if n.boolop.name=='OR_EXPR' else None if xs[0] is None else not xs[0]
 if isinstance(n,ast.NullTest):return (value(n.arg,c) is None)==(n.nulltesttype.name=='IS_NULL')
 if isinstance(n,ast.A_Expr):
  op=n.name[0].sval;l=value(n.lexpr,c)
  if n.kind.name=='AEXPR_IN':
   xs=[None if l is None or (r:=value(x,c)) is None else l==r for x in n.rexpr];r=tri_or(xs);return (None if r is None else not r) if op=='<>' else r
  r=value(n.rexpr,c)
  if op in ['->','->>']:
   v=l.get(r) if isinstance(l,dict) else None
   return (str(v).lower() if isinstance(v,bool) else str(v) if v is not None else None) if op=='->>' and not isinstance(v,(dict,list)) else v
  if n.kind.name=='AEXPR_DISTINCT':return l!=r
  if n.kind.name=='AEXPR_NOT_DISTINCT':return l==r
  if n.kind.name=='AEXPR_OP_ANY':return tri_or([None if l is None or x is None else l==x for x in r]) if r is not None else None
  if op=='=':return None if l is None or r is None else l==r
  if op in ['<>','!=']:return None if l is None or r is None else l!=r
 raise AssertionError(('UNSUPPORTED_FINITE_SOURCE_PREDICATE',n))
PREVIEW=statement(GROUP,'from public.import_review_states s').whereClause
DECISION=statement(GROUP,'select d.*').whereClause
CURSOR=condition(ROWS,"v_cursor->>'read_revision' is distinct from v_revision")
ADMISSION=pglast.parse_plpgsql('\n'.join(x for x in B[S['admission']].splitlines() if not x.startswith('\\')))[0]
NAMESPACE=condition(ADMISSION,'routine.proname=expected_name').subselect
def preview():return {'s':{'import_id':'import','preview_generation':9,'state_version':17,'preview_fingerprint':'fingerprint','superseded_at_utc':None,'abandoned_at_utc':None},'preview':{'import_id':'import','preview_generation':9,'state_version':17,'preview_fingerprint':'fingerprint'}}
def decision(kind='EMAIL_ISSUE'):
 information=kind=='ADVISORY'
 return {'d':{'action_id':'act','import_id':'import','preview_generation':9,'client_id':'client','is_current':True,'evidence_fingerprint':'evidence','action_kind':kind,'timesheet_id':None if information else 'ts','hr_row_id':'hr','summary_json':{'authority_mode':'VALIDATION_ONLY'}},'native_issue':{'action_id':'act','evidence_fingerprint':'evidence','action_kind':kind},'state':{'import_id':'import','preview_generation':9},'p_client_id':'client','raw':{'item_kind':'SOURCE_INFORMATION' if information else 'ACTUAL_TIMESHEET','timesheet_id':None if information else 'ts','hr_row_id':'hr'}}
class Tests(unittest.TestCase):
 def test_current_preview_exact_positive(self):self.assertIs(value(PREVIEW,preview()),True)
 def test_current_preview_every_identity_and_lifecycle_mutation(self):
  for k in ['import_id','preview_generation','state_version','preview_fingerprint','superseded_at_utc','abandoned_at_utc']:
   for v in [None,'changed'] if k not in ['superseded_at_utc','abandoned_at_utc'] else ['changed']:
    x=preview();x['s'][k]=v;self.assertIsNot(value(PREVIEW,x),True,(k,v))
 def test_all_three_genuine_decision_arms(self):
  for kind in ['EMAIL_ISSUE','EMAIL_REMINDER','ADVISORY']:self.assertIs(value(DECISION,decision(kind)),True,kind)
 def test_each_native_decision_binding_is_required(self):
  for kind in ['EMAIL_ISSUE','EMAIL_REMINDER','ADVISORY']:
   for key in ['action_id','import_id','preview_generation','client_id','evidence_fingerprint','action_kind','is_current']:
    x=decision(kind);x['d'][key]=False if key=='is_current' else None;self.assertIsNot(value(DECISION,x),True,(kind,key))
 def test_subject_alias_and_action_enum_refusals(self):
  for kind in ['EMAIL_ISSUE','EMAIL_REMINDER','ADVISORY']:
   x=decision(kind);x['d']['timesheet_id']='wrong';self.assertIsNot(value(DECISION,x),True)
  for kind in ['ADVISORY','EMAIL_ISSUE']:
   x=decision(kind);x['raw']['item_kind']='OTHER';self.assertIsNot(value(DECISION,x),True)
  for old in ['EMAIL_WEEKLY_ISSUES','EMAIL_DAILY_ISSUES','EMAIL_MISSING_FROM_IMPORT']:
   x=decision();x['d']['action_kind']=old;x['native_issue']['action_kind']=old;self.assertIsNot(value(DECISION,x),True)
 def test_advisory_never_fabricates_timesheet_or_authority(self):
  for key,v in [('hr_row_id','wrong'),('summary_json',{'authority_mode':'SOURCE_AUTHORITY'}),('timesheet_id','guessed')]:
   x=decision('ADVISORY');x['d'][key]=v;self.assertIsNot(value(DECISION,x),True)
 def test_cursor_currentness_actual_null_semantics(self):
  for cursor,revision,refuse in [(None,'new',False),({'read_revision':'a'},'a',False),({'read_revision':'a'},'b',True),({},'a',True),({'read_revision':None},'a',True)]:self.assertIs(value(CURSOR,{'v_cursor':cursor,'v_revision':revision}),refuse)
 def test_source_authority_and_redaction_blocks_are_retained(self):
  group=src('check_only_query_groups');rows=src('check_only_query_rows')
  self.assertIn("if view='ADMIN' then",group);self.assertIn("groups jsonb:='[]'::jsonb",group)
  for exact in ["'query_import_id',case when v_admin", "'query_preview',case when v_admin", "'query_evidence',case when v_admin", "'native_issues',case when v_admin"]:self.assertIn(exact,rows)
  self.assertIn("'source_outreach_eligible',false,'bulk_actions','{}'::jsonb",group)
  self.assertIn("'accept_eligible',false,'actions','[]'::jsonb",group)
 def test_three_successors_and_four_new_names_only(self):
  inverse=doc('SOURCE_OWNER_REVERSES.json')['rows'];self.assertEqual(len(inverse),4)
  self.assertEqual(len([x for x in inverse if '_acl_' not in x['signature']]),3)
  self.assertEqual(len(A['newNames']),4);self.assertEqual(len(S['targets']),17)
 def test_literal_full_parent_reversals(self):
  for row in doc('FULL_PARENT_FORWARD.json')['rows']:
   self.assertEqual(B[row['target']],row['after']);self.assertEqual(row['after'].replace(row['after'],row['before'],1),row['before'])
 def test_exact_three_hunk_gate_inverse_preserves_every_original_map(self):
  f=doc('F23_BRIDGE_FORWARD.json');self.assertEqual(len(f['hunks']),3);s=B[f['target']]
  for h in reversed(f['hunks']):self.assertEqual(s.count(h['after']),1);s=s.replace(h['after'],h['before'],1)
  self.assertEqual(s,(P/'parents'/f['target']).read_bytes().decode('utf-8'))
 def test_classifier_and_verifier_exact_literal_delta(self):
  expected=set(doc('EXPECTED_SECURITY_DELTA.json')['newPrivateLiteralSignatures']);self.assertEqual(len(expected),4)
  old=(P/'parents/supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql').read_text(encoding='utf-8');new=B['supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql']
  for s in expected:self.assertNotIn("'"+s+"'",old);self.assertEqual(new.count("'"+s+"'"),1)
  stripped=new
  for s in sorted(expected):stripped=stripped.replace("    '"+s+"',\n",'',1)
  self.assertEqual(stripped,old)
 def test_one_time_guard_is_not_in_repeatable(self):
  module=B[S['module']];guard=B[S['admission']]
  self.assertNotIn('pg_proc',module);self.assertNotIn('admission',module)
  self.assertIn("routine.proname=expected_name",guard);self.assertIn("namespace.nspname='private'",guard)
  self.assertNotIn('to_regprocedure',guard);self.assertIn('ONE_TIME_ADMISSION',guard)
 def test_actual_namewide_namespace_predicate_catches_overloads_and_procedures(self):
  for name in A['newNames']:
   for kind,args in [('f',''),('f','uuid,jsonb'),('p','text'),('a','uuid')]:
    c={'namespace':{'nspname':'private'},'routine':{'proname':name,'prokind':kind,'proargtypes':args},'expected_name':name}
    self.assertIs(value(NAMESPACE.whereClause,c),True,(name,kind,args))
   for schema,other in [('public',name),('private',name+'_other')]:
    c={'namespace':{'nspname':schema},'routine':{'proname':other},'expected_name':name};self.assertIs(value(NAMESPACE.whereClause,c),False)
 def test_actual_rowtype_selects_reject_wrong_relation_join_or_nonstar(self):
  checks=[h for row in doc('RAW_F23_CENSUS.json') for h in row['hazards'] if h['hazard'].startswith('ROWTYPE_SELECT:')]
  self.assertEqual(len(checks),8)
  for h in checks:
   sig,kind,q=json.loads(h['key']);expected=kind.split(':',2)[2].replace(' ','').lower()
   def proved(text):
    n=pglast.parse_sql(text)[0].stmt
    return len(n.fromClause)==1 and isinstance(n.fromClause[0],ast.RangeVar) and n.fromClause[0].schemaname+'.'+n.fromClause[0].relname==expected and len(n.targetList)==1 and isinstance(n.targetList[0].val,ast.ColumnRef) and isinstance(n.targetList[0].val.fields[0],ast.A_Star)
   self.assertTrue(proved(q));self.assertFalse(proved(q.replace(expected,'public.candidates' if expected!='public.candidates' else 'public.clients')))
   self.assertFalse(proved(q.replace('select *','select 1',1)))
 def test_json_concat_real_producers_keep_jsonb_array_shape(self):
  group=src('check_only_query_groups');rows=src('check_only_query_rows')
  self.assertIn("rows jsonb:='[]'::jsonb",group);self.assertIn("'rows',v_rows",rows)
  self.assertIn("coalesce(jsonb_agg(value order by worker_sort,actual_work_date,item_sort),'[]'::jsonb)",rows)
  self.assertIn("'actions',jsonb_build_array(review_action)",group)
  self.assertIn("if reminder_action is not null then group_row:=jsonb_set(group_row,'{actions}',group_row->'actions'||jsonb_build_array(reminder_action),false);",group)
 def test_full_keyset_and_fail_closed_native_rechecks(self):
  group=src('check_only_query_groups')
  for exact in ["raw_revision is distinct from page->>'read_revision'", "cursor is not distinct from page->>'next_cursor'", "latest_decision.action_id is null", "latest_decision.action_id=any(native_actions)","WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_PARENT_AMBIGUOUS"]:self.assertIn(exact,group)
  self.assertNotIn('execute ',group.lower());self.assertNotIn('insert into',group.lower())
 def test_authored_non_financial_scope_and_native_pending_are_retained(self):
  s=src('actual_timesheet_submission');self.assertIn("v_count>1",s);self.assertIn("'CURRENT_FINANCIAL_IDENTITY_AMBIGUOUS'",s)
  self.assertIn("'CORRECTION_MEMBER_NOT_COUNTED'",s);self.assertIn("'IMMUTABLE_SUBMISSION_IDENTITY_UNPROVED'",s)
  signed=src('signed_submission_read_context');self.assertIn("'query_route_available',false",signed);self.assertIn("'actions_held',true",signed)
  for s in A['ownerPages'].values():self.assertNotIn('bpay_next_',B[s])
 def test_watched_path_report_does_not_supply_pin_translation(self):
  overlap=doc('WATCHED_PATH_OVERLAP.json');self.assertEqual(overlap['qt']['rows'],63);self.assertEqual(overlap['qt']['overlap'],[])
  self.assertEqual({x['file'] for x in overlap['r11']['overlap']},{'docs/bpay-next/final-definer-closures.json','supabase/release/current-release.json'})
  self.assertFalse(overlap['currentContract']['selected'])
if __name__=='__main__':unittest.main()

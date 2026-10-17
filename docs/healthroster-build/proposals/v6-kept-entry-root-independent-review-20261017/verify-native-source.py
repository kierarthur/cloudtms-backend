"""Independent PG17 grammar and fixed Health security source assertions; no DB."""
from pathlib import Path
import hashlib,json,re,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[3].parent/'healthroster-install2-14102026'
D='docs/healthroster-build/v6-owner-delivery-20261016/'
def raw(p):return Path(chr(92)*2+'?'+chr(92)+str(p.resolve())).read_bytes()
def text(p):return raw(p).decode()
def sha(b):return hashlib.sha256(b).hexdigest()
def obj(p):return json.loads(raw(ROOT/p))
kept=obj(D+'KEPT_PAY_REKEY_ROOT_MOUNT_20261017.json')
entry=obj(D+'CORRECTION_ENTRY_ROOT_MOUNT_20261017.json')
verifier='supabase/verification/17102026_0144_kept_pay_and_correction_entry_security.sql'
acl=obj(D+'KEPT_ENTRY_MANDATORY_ACL_VERIFIER_20261017.json')['target']
files=sorted({r['target'] for r in kept['mountFiles']+entry['payloads'] if r['target'].endswith(('.sql','.sqlinc'))}|{verifier,acl})
grammar=[];owners={};ddl_triggers={};ddl_indexes={}
for file in files:
 source=text(ROOT/file)
 clean=re.sub(r'^\s*\\[^\n]*','',source,flags=re.M)
 parsed=pglast.parse_sql(clean);bodies=0
 for node in parsed:
  stmt=node.stmt
  if isinstance(stmt,ast.CreateFunctionStmt):
   name='.'.join(x.sval for x in stmt.funcname)
   types=[('.'.join(x.sval for x in a.argType.names)).replace('pg_catalog.','') for a in (stmt.parameters or [])]
   signature=name+'('+','.join(types)+')'
   owners[signature]=source
   if any(o.defname=='language' and o.arg.sval=='plpgsql' for o in stmt.options):
    fragment=clean[node.stmt_location:node.stmt_location+node.stmt_len] if node.stmt_len else clean[node.stmt_location:]
    pglast.parse_plpgsql(fragment);bodies+=1
  elif isinstance(stmt,ast.DoStmt):
   fragment=clean[node.stmt_location:node.stmt_location+node.stmt_len] if node.stmt_len else clean[node.stmt_location:]
   pglast.parse_plpgsql(fragment);bodies+=1
  elif isinstance(stmt,ast.CreateTrigStmt):ddl_triggers[stmt.trigname]=stmt
  elif isinstance(stmt,ast.IndexStmt):ddl_indexes[stmt.idxname]=stmt
 grammar.append({'file':file,'sha256':sha(raw(ROOT/file)),'statements':len(parsed),'plpgsqlBodies':bodies})
# This expected set is written independently, rather than inferred from the verifier.
fixed=[
 ('private.weekly_source_identical_rekey_prior_v1(uuid)',False,'pg_catalog,pg_temp'),
 ('private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid)',False,'pg_catalog,pg_temp'),
 ('private.weekly_source_kept_as_paid_facts_v1(uuid,text)',False,'pg_catalog,pg_temp'),
 ('private.weekly_source_kept_as_paid_context_v1(uuid,uuid)',False,'pg_catalog,pg_temp'),
 ('private.weekly_source_invoice_credited_targets_admit_v1(uuid,jsonb,uuid)',False,'pg_catalog,pg_temp'),
 ('private.weekly_source_keep_ack_native_guard_v1()',False,'pg_catalog,pg_temp'),
 ('private.timesheet_correction_entry_subject_v1(uuid,uuid)',False,'pg_catalog,public,private,pg_temp'),
 ('private.timesheet_correction_entry_v1(uuid,uuid)',False,'pg_catalog,public,private,pg_temp'),
 ('public.weekly_source_keep_as_paid_decide_atomic_v1(jsonb)',True,'pg_catalog,pg_temp'),
 ('public.timesheet_correction_entry_v1(jsonb)',True,'pg_catalog,public,private,pg_temp')]
verify=text(ROOT/verifier)
actual=[(m[0],m[1]=='true',m[2]) for m in re.findall(r"\('((?:private|public)\.[a-z_0-9]+\([^']*\))',(false|true),'([^']+)'\)",verify)]
assert actual==fixed
for signature,service,search in fixed:
 source=owners[signature]
 assert re.search(r'\bsecurity\s+definer\b',source,re.I)
 paths=re.findall(r'\bset\s+search_path\s*(?:=|to)\s*([^\n]+)',source,re.I)
 assert search in [x.replace(' ','').replace("'",'').rstrip(';') for x in paths],signature
assert "array['anon','authenticated']" in verify
assert "has_function_privilege('service_role',fn,'EXECUTE') is distinct from expected.service_execute" in verify
assert "acl.grantee<>actual.proowner" in verify
assert "has_any_column_privilege(role_name,relation,'SELECT,INSERT,UPDATE,REFERENCES')" in verify
# Actual DDL names, timing/event bits and target guards must match each fixed assertion.
triggers=[
 ('zzzz_weekly_source_keep_ack_native_guard',7,'before insert','private.weekly_source_keep_ack_native_guard_v1()'),
 ('zzzz_weekly_source_keep_ack_update_guard',19,'before update','private.weekly_source_keep_ack_native_guard_v1()'),
 ('zzzz_weekly_source_keep_ack_delete_guard',11,'before delete','private.weekly_source_keep_ack_native_guard_v1()'),
 ('zzzz_weekly_source_keep_ack_truncate_guard',34,'before truncate','private.weekly_source_keep_ack_native_guard_v1()'),
 ('weekly_source_kept_as_paid_decisions_immutable',27,'before update or delete','private._weekly_source_immutable_record_guard_v1()'),
 ('weekly_source_kept_as_paid_decisions_truncate_immutable',34,'before truncate','private._weekly_source_immutable_record_guard_v1()')]
all_sql='\n'.join(text(ROOT/f) for f in files)
for name,bits,event,function in triggers:
 assert re.search(r"'"+name+r"',"+str(bits)+r",'"+re.escape(function)+r"'",verify)
 declaration=ddl_triggers[name]
 assert declaration.timing|declaration.events|int(declaration.row)==bits,name
 assert '.'.join(x.sval for x in declaration.funcname)+'()'==function
 assert declaration.args is None and not declaration.isconstraint
 assert declaration.relation.schemaname=='public'
 assert declaration.relation.relname==('audit_events' if name.startswith('zzzz_') else 'weekly_source_kept_as_paid_decisions')
for name in ['weekly_source_kept_as_paid_decisions_identity','weekly_source_keep_ack_request_identity','weekly_source_keep_ack_event_sequence']:
 declaration=ddl_indexes[name]
 assert declaration.unique and declaration.relation.schemaname=='public'
 assert (declaration.whereClause is not None)==('keep_ack' in name)
 assert "'public."+name+"'" in verify
# Genuine packet AST proof, suppressing only its exact existing evidence write.
packet=ROOT/'docs/healthroster-build/proposals/kept-entry-f23-classification-20261017'
original=Path.write_text;suppressed=[]
def no_write(path,value,*args,**kwargs):
 assert path==packet/'AST_PROOF.json'
 assert value.encode()==raw(path)
 suppressed.append(str(path));return len(value)
Path.write_text=no_write
try:exec(compile(text(packet/'ast-proof.py'),str(packet/'ast-proof.py'),'exec'),{'__file__':str(packet/'ast-proof.py'),'__name__':'__main__'})
finally:Path.write_text=original
assert len(suppressed)==1
result={'parser':'pglast '+pglast.__version__,'postgresGrammar':'17.7','files':grammar,
 'totals':{'files':len(grammar),'statements':sum(x['statements']for x in grammar),'plpgsqlBodies':sum(x['plpgsqlBodies']for x in grammar)},
 'independentSecurityExpectedSet':{'private':8,'service':2,'immutable':1,'triggerAssertions':6,'uniqueIndexAssertions':3},
 'genuineExpressionAndEightColumnDdlAst':'PASS','otherWorktreeWrites':0,'native':'NOT_RUN','catalogue':'NOT_RUN'}
(HERE/'GRAMMAR_SECURITY_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print('PASS:',result['totals'],'fixed independent 8-private/2-service headers, six trigger source declarations and three unique-index declarations')

"""Source-only finite composition. Never installs, executes SQL, or edits root.

Every winning source is explicitly recorded in REVIEWED_SELECTION.json. The
inventory pass never writes a guessed winner or updates a shared registry.
"""
from pathlib import Path
import sys,subprocess,json,hashlib,re
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
from pglast import parse_sql,scan
from pglast.ast import CreateFunctionStmt
HERE=Path(__file__).resolve().parent
REPO=HERE.parents[3]
PREFIX='docs/healthroster-build/proposals/'
PACKAGES=[
 ('96c5a7ec4655481499f53d3e2c59361ede102a74','candidate-contract-move-native-20261016'),
 ('c47530caa7e4c7e2412a5d6b03b76bbb7f6dc8bb','candidate-contract-move-lifecycle-20261016'),
 ('529ecc404c29c923b1fcfca3d764e9dc9d6762ad','candidate-move-paper-draft-20261016'),
 ('c03c010dfabe8280fcbb36779fa210e7e5242a02','candidate-move-native-reset-20261016'),
 ('a10f080f2272155c423991ffc81de8670d3c756b','candidate-move-qr-paper-20261016'),
 ('975376fd97855248c76b0ccbe027eed9b50f25ff','candidate-native-pending-20261016'),
 ('2b5f58431df26561b1423d40c48bef15209494fd','candidate-move-expense-preparation-20261016'),
 ('11afc9c69c85b6babef0b0e7b5230bc2e0bd3417','candidate-current-body-witness-20261016')]
def sha(b):return hashlib.sha256(b).hexdigest()
def blob(commit,path):return subprocess.check_output(['git','show',commit+':'+path],cwd=REPO)
def writejson(name,value):(HERE/name).write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8',newline='\n')
def signature(stmt):
 name='.'.join(x.sval for x in stmt.funcname)
 args=[]
 for p in stmt.parameters or []:
  if str(p.mode) in ('o','t'):continue
  names=[x.sval for x in p.argType.names]
  if names[0]=='pg_catalog':names=names[1:]
  typ='.'.join(names)
  typ={'int4':'integer','bool':'boolean','int8':'bigint'}.get(typ,typ)
  if p.argType.arrayBounds:typ+='[]'*len(p.argType.arrayBounds)
  args.append(typ)
 return name+'('+','.join(args)+')'
def units():
 result=[];packages=[]
 for commit,package in PACKAGES:
  base=PREFIX+package+'/'
  recipe_raw=blob(commit,base+'MOUNT_RECIPE.json');recipe=json.loads(recipe_raw)
  ordered=recipe.get('mount',{}).get('orderedSqlPayloads')
  if ordered is None:ordered=[x['payload'] for x in recipe['sql']] if 'sql' in recipe else [recipe['payload']]
  packages.append({'commit':commit,'package':package,'recipePath':base+'MOUNT_RECIPE.json','recipeSha256':sha(recipe_raw)})
  for file in ordered:
   raw=blob(commit,base+file)
   text=raw.decode('utf-8')
   for i,part in enumerate(parse_sql(text)):
    # This Python pglast API returns Python character indices. Convert both
    # boundaries to physical UTF-8 bytes; QR source contains multibyte comments.
    char_start=part.stmt_location;char_end=char_start+part.stmt_len if part.stmt_len else len(text)
    if text[char_end:char_end+1]==';':char_end+=1
    start=len(text[:char_start].encode('utf-8'));end=len(text[:char_end].encode('utf-8'))
    unit={'commit':commit,'path':base+file,'fileSha256':sha(raw),'statementIndex':i,'startByte':start,'endByteExclusive':end,'spanSha256':sha(raw[start:end]),'tag':type(part.stmt).__name__}
    if isinstance(part.stmt,CreateFunctionStmt):unit['signature']=signature(part.stmt)
    unit['_raw']=raw[start:end];result.append(unit)
 return result,packages
def public(u):return {k:v for k,v in u.items() if not k.startswith('_')}
def run():
 all_units,packages=units()
 if '--inventory' in sys.argv:
  occurrences={}
  for u in all_units:
   if 'signature' in u:occurrences.setdefault(u['signature'],[]).append(public(u))
  effects=[public(u)|{'preview':u['_raw'].decode('utf-8')[:240]} for u in all_units if 'signature' not in u and u['tag'] not in ['AlterOwnerStmt','AlterFunctionStmt','GrantStmt','NotifyStmt']]
  writejson('INVENTORY.json',{'packages':packages,'functionOccurrences':occurrences,'nonFunctionEffects':effects})
  print(f'{len(occurrences)} function signatures, {sum(map(len,occurrences.values()))} complete definitions, {len(effects)} non-function effects inventoried; NO winners inferred.')
  return
 selection=json.loads((HERE/'REVIEWED_SELECTION.json').read_text(encoding='utf-8'))
 selected={x['signature']:x for x in selection['functions']}
 if set(selected)!={u['signature'] for u in all_units if 'signature' in u}:raise RuntimeError('SELECTION_CENSUS_MISMATCH')
 out=[];functions=[];effects=[];skipped=[];canonical=[]
 for u in all_units:
  if 'signature' in u:
   s=selected[u['signature']]
   if (u['commit'],u['path'],u['spanSha256'])!=(s['commit'],s['path'],s['spanSha256']):skipped.append(public(u));continue
   emitted=u['_raw']
   if len(parse_sql(emitted.decode('utf-8')))!=1:raise RuntimeError('COMPLETE_FUNCTION_SPAN_REQUIRED')
   if b'\r' in emitted:
    if u['path']!='docs/healthroster-build/proposals/candidate-move-qr-paper-20261016/forward/03_captured_paper_source_lock.sqlinc':raise RuntimeError('UNREVIEWED_NONCANONICAL_FUNCTION:'+u['path'])
    if b'\r' in emitted.replace(b'\r\n',b''):raise RuntimeError('BARE_CR')
    a=emitted.index(b'as $function$')+len(b'as $function$');z=emitted.index(b'$function$;',a);body=emitted[a:z]
    literals=[t for t in scan(body.decode('utf-8')) if t.name in('SCONST','USCONST')]
    if any(b'\r' in body[t.start:t.end+1] for t in literals):raise RuntimeError('CR_IN_NATIVE_LITERAL')
    emitted=emitted.replace(b'\r\n',b'\n')
    canonical.append(public(u)|{'transformation':'CRLF-to-LF only; no bare CR, all actual PL/SQL string tokens checked, no CR within literal','literalTokensChecked':len(literals),'canonicalCompleteSha256':sha(emitted)})
   header=f"-- Exact complete source {u['signature']}\n-- {u['commit']}:{u['path']} bytes[{u['startByte']}:{u['endByteExclusive']}] SHA256 {u['spanSha256']}\n".encode()
   out.append(header+emitted.strip()+b'\n');functions.append(public(u)|{'canonicalCompleteSha256':sha(emitted)})
  elif u['tag'] in ['CreateTrigStmt','DropStmt']:
   if b'\r' in u['_raw']:raise RuntimeError('NONCANONICAL_SELECTED_EFFECT:'+u['path'])
   effects.append(u)
  elif u['tag'] not in ['AlterOwnerStmt','AlterFunctionStmt','GrantStmt','NotifyStmt']:
   raise RuntimeError('UNREVIEWED_EFFECT:'+str(public(u)))
 if len(functions)!=len(selected):raise RuntimeError('WINNER_NOT_UNIQUE')
 (HERE/'forward').mkdir(exist_ok=True)
 (HERE/'forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc').write_bytes(b'\n'.join(out))
 effect_raw=b'\n'.join(u['_raw'].strip()+b'\n' for u in effects)
 (HERE/'forward/CANDIDATE_EFFECTS.sqlinc').write_bytes(effect_raw)
 acl='-- Explicit final Candidate closure. Original service RPCs only; private helpers owner-only.\n'
 for u in functions:
  sig=u['signature'];acl+=f'alter function {sig} owner to postgres;\nrevoke all on function {sig} from public,anon,authenticated,service_role;\n'
  if sig.startswith('public.'):acl+=f'grant execute on function {sig} to service_role;\n'
 # Deferred trigger function is supplied only by the exact975 migration body.
 sig='private.timesheet_contract_native_pending_commit_guard_v1()'
 acl+=f'alter function {sig} owner to postgres;\nrevoke all on function {sig} from public,anon,authenticated,service_role;\n'
 acl+="notify pgrst, 'reload schema';\n"
 (HERE/'forward/CANDIDATE_FINAL_ACL.sqlinc').write_text(acl,encoding='utf-8',newline='\n')
 writejson('JOINED_SQL_MAP.json',{'status':'SOURCE_COMPOSITION_NATIVE_NOT_RUN','packages':packages,'functions':functions,'effects':[public(u) for u in effects],'supersededDefinitionsExcluded':skipped,'newlineCanonicalization':canonical,'effectAudit':'Only exact existing96c5 summary invalidation trigger drop/create. No business DML/DO/CALL/transaction/psql meta introduced. Complete975 one-time nullable body separately required.'})
 print(f'{len(functions)} unique complete final functions, {len(effects)} exact non-function effects, {len(skipped)} superseded definitions excluded. No SQL execution.')
if __name__=='__main__':run()

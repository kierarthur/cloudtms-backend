"""Offline exact source closure only; never opens a database or network route."""
from pathlib import Path
import sys,json,hashlib
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
from pglast import parser,stream
base=Path(__file__).resolve().parent
recipe=json.loads((base/'MOUNT_RECIPE.json').read_text(encoding='utf8'))
root=Path(recipe['sourceInputs']['root'])
sha=lambda b:hashlib.sha256(b).hexdigest()
for f in recipe['sourceInputs']['files']+[recipe['brokerInput']]:
 if sha((root/f['path']).read_bytes())!=f['sha256']:raise RuntimeError('Saved root input changed: '+f['path'])
functions=[]
for file in sorted((base/'forward').glob('*.sqlinc')):
 if file.name=='13_acl.sqlinc':continue
 for x in parser.parse_sql(file.read_text(encoding='utf8')):
  n=x.stmt
  if type(n).__name__!='CreateFunctionStmt':continue
  name='.'.join(v.sval for v in n.funcname)
  signature=name+'('+','.join(stream.RawStream()(a.argType) for a in n.parameters or [])+')'
  functions.append({'signature':signature,'path':file.relative_to(base).as_posix(),'exposure':'service_role' if name.startswith('public.') else 'owner_only'})
acl='-- Exact finite ACL closure: native business RPCs retain existing service-only grants.\n'
for f in functions:
 acl+='alter function '+f['signature']+' owner to postgres;\n'
 acl+='revoke all on function '+f['signature']+' from public,anon,authenticated,service_role;\n'
 if f['exposure']=='service_role':acl+='grant execute on function '+f['signature']+' to service_role;\n'
(base/'forward/13_acl.sqlinc').write_text(acl,encoding='utf8',newline='\n')
recipe['functions']=functions
recipe['mount']['orderedSqlPayloads']=[f.relative_to(base).as_posix() for f in sorted((base/'forward').glob('*.sqlinc'))]
recipe['verification']['nodeTests']=15
recipe['verification']['offlineGrammar']='13 SQL payloads plus pure fact scalar assertions; actual %ROWTYPE catalogue field checks'
recipe['payloads']=[{'path':f.relative_to(base).as_posix(),'sha256':sha(f.read_bytes()),'bytes':f.stat().st_size} for f in sorted(base.rglob('*')) if f.is_file() and f.name not in ['MOUNT_RECIPE.json','PUBLISHED.md']]
(base/'MOUNT_RECIPE.json').write_text(json.dumps(recipe,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps({'payloads':len(recipe['payloads']),'functions':len(functions),'sha256':sha((base/'MOUNT_RECIPE.json').read_bytes()),'native':'NOT_RUN'}))

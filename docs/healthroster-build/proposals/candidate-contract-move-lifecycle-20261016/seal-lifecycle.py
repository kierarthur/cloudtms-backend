"""Offline source/ACL closure; never opens a database or network route."""
from pathlib import Path
import sys,json,hashlib
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
from pglast import parser,stream
b=Path(__file__).resolve().parent;r=json.loads((b/'MOUNT_RECIPE.json').read_text(encoding='utf-8'));root=Path(r['sourceInputs']['root'])
sha=lambda raw:hashlib.sha256(raw).hexdigest()
for f in r['sourceInputs']['files']:
 assert sha((root/f['path']).read_bytes())==f['sha256'],'ROOT_INPUT_CHANGED:'+f['path']
assert sha((b/r['schema7d']['path']).resolve().read_bytes())==r['schema7d']['sha256'],'7D_SCHEMA_CHANGED'
functions=[]
for file in r['mount']['orderedSqlPayloads']:
 if file.endswith('07_acl.sqlinc'):continue
 for item in parser.parse_sql((b/file).read_text(encoding='utf-8')):
  n=item.stmt
  if type(n).__name__!='CreateFunctionStmt':continue
  name='.'.join(v.sval for v in n.funcname)
  signature=name+'('+','.join(stream.RawStream()(a.argType) for a in n.parameters or [])+')'
  functions.append({'signature':signature,'path':file,'exposure':'service_role' if name.startswith('public.') else 'owner_only'})
assert len(functions)==8
acl='-- Final finite native business/private ACL. No browser grants.\n'
for f in functions:
 acl+='alter function '+f['signature']+' owner to postgres;\nrevoke all on function '+f['signature']+' from public,anon,authenticated,service_role;\n'
 if f['exposure']=='service_role':acl+='grant execute on function '+f['signature']+' to service_role;\n'
(b/'forward/07_acl.sqlinc').write_text(acl,encoding='utf-8',newline='\n')
r['functions']=functions
r['payloads']=[{'path':f.relative_to(b).as_posix(),'sha256':sha(f.read_bytes()),'bytes':f.stat().st_size} for f in sorted(b.rglob('*')) if f.is_file() and f.name not in('MOUNT_RECIPE.json','PUBLISHED.md')]
(b/'MOUNT_RECIPE.json').write_text(json.dumps(r,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'payloads':len(r['payloads']),'functions':len(functions),'manifestSha256':sha((b/'MOUNT_RECIPE.json').read_bytes()),'native':'NOT_RUN'}))

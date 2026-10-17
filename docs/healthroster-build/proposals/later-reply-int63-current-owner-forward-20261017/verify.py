"""Read-only current-source/full-inverse verifier. Does not run release or SQL."""
from pathlib import Path
import json,hashlib,subprocess,sys
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
MONEY=HERE.parents[3]
sha=lambda b:hashlib.sha256(b).hexdigest()
def git(root,*args):return subprocess.check_output(['git','-C',str(root),*args])
owner=json.loads((HERE/'CURRENT_OWNER.json').read_text())
forward=json.loads((HERE/'FULL_FORWARD.json').read_text())
for row in owner['inputs']:
 raw=(HERE/row['reference']).read_bytes()
 assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
 assert (ROOT/row['path']).read_bytes()==raw,row['path']
 for spec in [':'+row['path'],'HEAD:'+row['path'],owner['sourceRootCommit']+':'+row['path']]:
  assert git(ROOT,'show',spec)==raw,spec
old=(HERE/'reference/later.sqlinc').read_bytes();new=(HERE/forward['payload']).read_bytes()
assert len(old)==forward['beforeBytes'] and sha(old)==forward['beforeSha256']
assert len(new)==forward['afterBytes'] and sha(new)==forward['afterSha256']
candidate=old.decode()
for edit in forward['edits']:
 assert candidate.count(edit['before'])==edit['expected']
 candidate=candidate.replace(edit['before'],edit['after'])
assert candidate.encode()==new
inverse=new.decode()
for edit in reversed(forward['edits']):
 assert inverse.count(edit['after'])==edit['expected']
 inverse=inverse.replace(edit['after'],edit['before'])
assert inverse.encode()==old
assert sum(x['expected'] for x in forward['edits'])==7
signature='public.weekly_source_later_change_decide_atomic_v1(jsonb)'
registry=json.loads((ROOT/'docs/bpay-next/final-definer-closures.json').read_bytes())
actual=[c for c in registry['chains'] if c['signature']==signature]
assert actual==[owner['registeredCurrentChain']]
assert actual[0]['finalPhysical']==owner['target']
assert (HERE/'reference/root.sql').read_text().count('\\ir 17102026_0141_weekly_source_later_accept_health/10_complete_later_change_decide.sqlinc')==1
acl=(HERE/'reference/acl.sqlinc').read_text()
expected=[f'alter function {signature} owner to postgres;',
 f'revoke all on function {signature} from public,anon,authenticated,service_role;',
 f'grant execute on function {signature} to service_role;']
for line in expected:assert acl.splitlines().count(line)==1,line
assert owner['newSignatures']==owner['newPrivileges']==owner['metadataWrites']==0
assert forward['mounted'] is False and forward['futureManagedScaffoldRequired'] is True
count=0
if (HERE/'MANIFEST.json').exists():
 mf=json.loads((HERE/'MANIFEST.json').read_text())
 for row in mf['files']:
  raw=(HERE/row['file']).read_bytes();assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
  if '--committed' in sys.argv:
   rel=(HERE/row['file']).relative_to(MONEY).as_posix()
   assert git(MONEY,'show',':'+rel)==raw and git(MONEY,'show','HEAD:'+rel)==raw
  count+=1
print(json.dumps({'verdict':'PASS','currentWholeOwnerAndCallerInputs':len(owner['inputs']),
 'forwardInverse':'PASS','castOccurrences':7,'currentRegistryChainUnchanged':True,
 'independentExistingACLStatements':3,'manifestRows':count,'metadataChanges':0,'native':'NOT_RUN'}))

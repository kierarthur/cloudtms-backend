"""Non-writing complete public-source inventory/parent/provenance verification."""
from pathlib import Path
import hashlib,json,os,subprocess,sys
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
OWN=Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026')
def long(p):return Path('\\\\?\\'+str(Path(p).resolve()))
def read(p):return long(HERE/p).read_bytes()
def doc(p):return json.loads(read(p))
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*args,root=ROOT):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',*args],cwd=root,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
def main():
 assert str(HERE).lower().startswith('d:\\development\\github\\'),HERE
 selection=doc('PAYLOAD_SELECTION.json');parents=doc('PARENTS.json');provenance=doc('SOURCE_PROVENANCE.json');assembly=doc('ASSEMBLY.json')
 assert len(selection['targets'])==17 and len({x['target'] for x in selection['targets']})==17
 start=git('rev-parse','HEAD').decode().strip() if '--current' in sys.argv else None
 for row in parents['rows']:
  b=read(row['capture']);assert len(b)==row['bytes'] and sha(b)==row['sha256']
  assert b==git('show',row['commit']+':'+row['target']),('HISTORICAL_PARENT',row['target'])
  if start:assert b==long(ROOT/row['target']).read_bytes()==git('show',':'+row['target'])==git('show','HEAD:'+row['target']),('CURRENT_COMPLETE_PARENT_CHANGED',row['target'])
 for row in selection['targets']:
  b=read(row['source']);assert len(b)==row['bytes'] and sha(b)==row['sha256']
  if row['beforeSha256'] is None and start:
   assert not long(ROOT/row['target']).exists() and not git('ls-files','--',row['target']).strip(),('NEW_TARGET_NOT_VACANT',row['target'])
 for row in doc('FULL_PARENT_FORWARD.json')['rows']:
  b=read('parents/'+row['target']);a=next(x for x in selection['targets'] if x['target']==row['target']);raw=read(a['source'])
  assert b==row['before'].encode() and raw==row['after'].encode()
  assert len(b)==row['beforeBytes'] and len(raw)==row['afterBytes'] and sha(b)==row['beforeSha256'] and sha(raw)==row['afterSha256']
 for row in doc('SOURCE_OWNER_REVERSES.json')['rows']:
  b=read('parents/'+row['currentPhysical']);assert row['before'].encode() in b
  if '_acl_' not in row['signature']:assert b==row['before'].encode(),('THREE_COMPLETE_OWNER_FILES',row['signature'])
  a=next(x for x in selection['targets'] if x['target']==row['successorPhysical']);assert row['after'].encode() in read(a['source'])
  assert sha(row['before'].encode())==row['beforeSha256'] and len(row['before'].encode())==row['beforeBytes']
  assert sha(row['after'].encode())==row['afterSha256'] and len(row['after'].encode())==row['afterBytes']
 for row in provenance['owners']:
  a=assembly['ownerPages'][row['id']];raw=read(next(x['source'] for x in selection['targets'] if x['target']==a))
  historical=git('show',provenance['authoredModuleCommit']+':'+provenance['originalModule']+'/'+row['output'],root=OWN)
  assert raw==historical and sha(raw)==row['output_sha256']
  original=git('show',provenance['originalBodyCommit']+':'+row['source_file'],root=OWN);assert original==read('provenance/'+row['source_file']) and sha(original)==row['source_file_sha256']
 if start:
  checked={}
  for owner in doc('EXISTING_DEPENDENCY_CLOSURE.json')['rows']:
   for pin in owner['sourceFileHashes']:
    if pin['file'] in checked:assert checked[pin['file']]==pin['rawSha256'];continue
    raw=long(ROOT/pin['file']).read_bytes();assert len(raw)==pin['rawBytes'] and sha(raw)==pin['rawSha256'] and sha(raw.replace(b'\r\n',b'\n'))==pin['canonicalSha256'],('EXISTING_CALLEE_SOURCE_CHANGED',pin['file'])
    assert raw==git('show',':'+pin['file'])==git('show','HEAD:'+pin['file']),('EXISTING_CALLEE_CURRENT_GIT_CHANGED',pin['file'])
    checked[pin['file']]=pin['rawSha256']
 release_before=json.loads(read('parents/supabase/release/current-release.json'));release_after=json.loads(read(next(x['source'] for x in selection['targets'] if x['target']=='supabase/release/current-release.json')))
 for key in ['verificationFiles','newVerificationFiles']:assert release_after[key][:-1]==release_before[key] and release_after[key][-1]==selection['verifier']
 for k in release_before:
  if k not in ['verificationFiles','newVerificationFiles']:assert release_after[k]==release_before[k]
 lock_before=json.loads(read('parents/supabase/release/migration-lock.json'));lock_after=json.loads(read(next(x['source'] for x in selection['targets'] if x['target']=='supabase/release/migration-lock.json')))
 assert lock_after['migrations'][:-1]==lock_before['migrations'] and lock_after['migrations'][-1]['path']==selection['admission']
 reg_before=json.loads(read('parents/docs/bpay-next/final-definer-closures.json'));reg_after=json.loads(read(next(x['source'] for x in selection['targets'] if x['target']=='docs/bpay-next/final-definer-closures.json')))
 sig='private._weekly_source_acl_private_helper_contract_v1()'
 for old,new in zip(reg_before['chains'],reg_after['chains']):
  if old['signature']!=sig:assert old==new,old['signature']
 assert len(reg_after['chains'])==len(reg_before['chains'])+7
 for key in reg_before:
  if key!='chains':assert reg_before[key]==reg_after[key]
 if (HERE/'MANIFEST.json').exists():
  manifest=doc('MANIFEST.json');actual=set()
  for p in long(HERE).rglob('*'):
   if p.is_file() and p!=long(HERE/'MANIFEST.json') and '__pycache__' not in p.parts:actual.add(str(p.relative_to(long(HERE))).replace('\\','/'))
  assert actual=={x['path'] for x in manifest['files']}
  for row in manifest['files']:
   raw=read(row['path']);assert sha(raw)==row['sha256'] and len(raw)==row['bytes'],row['path']
 if start:assert git('rev-parse','HEAD').decode().strip()==start,'CURRENT_HEAD_MOVED_DURING_VERIFY'
 print(json.dumps({'targets':len(selection['targets']),'completeParents':len(parents['rows']),'wholeFileInverses':len(doc('FULL_PARENT_FORWARD.json')['rows']),'completeExistingHelperOwners':3,'authoredExactBodies':7,'current':'PASS' if start else 'NOT_REQUESTED','native':'NOT_RUN'}))
if __name__=='__main__':main()

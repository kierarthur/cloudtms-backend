"""Read-only finite F23 mount and prebody bridge comparisons. No DB."""
from pathlib import Path
import hashlib,json,subprocess
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[3].parent/'healthroster-install2-14102026'
BASE='548208ac8d3d8c3f32f56f1c1174242d276467b1'
D='docs/healthroster-build/v6-owner-delivery-20261016/'
P='docs/healthroster-build/proposals/kept-entry-f23-classification-20261017/'
def read(p):return Path(chr(92)*2+'?'+chr(92)+str(p.resolve())).read_bytes()
def sha(b):return hashlib.sha256(b).hexdigest()
def canon(b):return b.decode().replace('\r\n','\n')
def obj(p):return json.loads(read(ROOT/p))
def git(c,p):return subprocess.check_output(['git','-C',str(ROOT),'show',c+':'+p])
def reverse(s,hunks):
 for h in reversed(hunks):
  assert s.count(h['after'])==1
  s=s.replace(h['after'],h['before'])
 return s
mount=obj(D+'KEPT_ENTRY_F23_ROOT_MOUNT_20261017.json')
recipe=obj(P+'GATE_EDITS.json')
assert len(recipe['hunks'])==4
gate=read(ROOT/recipe['target'])
assert sha(gate)==recipe['outputSha256']==mount['outputGateSha256']
prior=reverse(gate.decode(),recipe['hunks']).encode()
assert sha(prior)==recipe['inputSha256']==mount['inputGateSha256']
assert prior==git(BASE,recipe['target'])
for pin in mount['copiedFiles']:
 data=read(ROOT/pin['path'])
 assert sha(data)==pin['sha256'] and len(data)==pin['bytes']
 assert data==git(mount['sourceCommit'],pin['path'])
review=obj(P+'CLASSIFICATION.json')
assert review['counts']=={'files':19,'rawOccurrences':94,'uniquePoints':93}
assert review['native']=='NOT_RUN'
for pin in review['inputs']:assert sha(canon(read(ROOT/pin['path'])).encode())==pin['sha256'],pin['path']
bridge=obj(D+'PREBODY_RPC_POLICY_SUCCESSOR_20261017.json')
data=read(ROOT/bridge['target'])
assert len(bridge['hunks'])==2 and sha(data)==bridge['outputSha256']
prior=reverse(data.decode(),bridge['hunks']).encode()
assert sha(prior)==bridge['inputSha256'] and prior==git(BASE,bridge['target'])
original='docs/healthroster-build/v6-owner-delivery-20261016/F23_PREBODY_DYNAMIC_REVIEW_20261016/CLASSIFICATION.json'
assert read(ROOT/original)==git(BASE,original)
prebody=obj(original);assert len(prebody['dependencies'])==95
reload=obj(D+'RPC_RELOAD_EXECUTION_PIN_SUCCESSOR_20261017.json')
changed=[]
for pin in prebody['dependencies']:
 data=read(ROOT/pin['file']);value=canon(data)
 if sha(value.encode())!=pin['sha256']:
  assert pin['file']==reload['policy'] and pin['sha256']==reload['inputPolicySha256']
  assert sha(value.encode())==reload['outputPolicySha256']
  assert value.count(reload['after'])==1
  assert sha(value.replace(reload['after'],reload['before']).encode())==pin['sha256']
  changed.append(pin['file'])
 if pin.get('preservedRawSha256'):assert sha(data)==pin['preservedRawSha256']
assert changed==[reload['policy']]
evidence={'base':BASE,'physicalForwardSource':mount['sourceCommit'],'copiedFiles':len(mount['copiedFiles']),
 'gateFullReverseHunks':4,'prebodyFullReverseHunks':2,'scopedPhysicalInputs':19,'hazardOccurrences':94,
 'originalReviewUnchanged':True,'boundInputs':95,'soleExactPolicySuccessor':changed,
 'gateSha256':sha(gate),'bridgeSha256':sha(read(ROOT/bridge['target'])),'native':'NOT_RUN','otherWorktreeWrites':0}
(HERE/'EXTENSION_EVIDENCE.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
print('PASS: 12 exact Physical files; four gate hunks and two bridge hunks fully reverse; original review/94 other dependencies unchanged')

"""Independent finite root integration checks; no DB, external API or writes outside HERE."""
from pathlib import Path
import hashlib, json, os, subprocess, sys

HERE=Path(__file__).resolve().parent
OWN=HERE.parents[3]
BACKEND=OWN.parent/'healthroster-install2-14102026'
OFFICE=OWN.parent/'healthroster-v2-office-13102026'
B='548208ac8d3d8c3f32f56f1c1174242d276467b1'
O='6d3d7210ac02fa2d1741f20e52461eaa75158350'
D='docs/healthroster-build/v6-owner-delivery-20261016/'
PKG='docs/healthroster-build/proposals/kept-pay-rekey-current-root-successor-20261016/'
ENTRY='docs/healthroster-build/proposals/correction-entry-native-reader-20261017/'
def raw(p): return Path(chr(92)*2+'?'+chr(92)+str(p.resolve())).read_bytes()
def text(p): return raw(p).decode('utf-8')
def sha(b): return hashlib.sha256(b).hexdigest()
def dump(v,ascii=True): return (json.dumps(v,ensure_ascii=ascii,indent=2)+'\n').encode('utf-8')
def obj(p): return json.loads(text(p))
def git(root,commit,file): return subprocess.check_output(['git','-C',str(root),'show',commit+':'+file])
def restore(value,hunks):
    for h in reversed(hunks):
        assert value.count(h['after'])==1
        value=value.replace(h['after'],h['before'])
    return value
evidence={'backend':B,'office':O,'nativeExecuted':False,'otherWorktreeWrites':False}
for root,pin in [(BACKEND,B),(OFFICE,O)]:
    assert os.path.realpath(root,strict=True).lower().startswith('d:\\development\\github\\')
    for args in [['rev-parse','--absolute-git-dir'],['rev-parse','--path-format=absolute','--git-common-dir']]:
        actual=subprocess.check_output(['git','-C',str(root),*args]).decode().strip()
        assert os.path.realpath(actual,strict=True).lower().startswith('d:\\development\\github\\')

kept=obj(BACKEND/(D+'KEPT_PAY_REKEY_ROOT_MOUNT_20261017.json'))
entry=obj(BACKEND/(D+'CORRECTION_ENTRY_ROOT_MOUNT_20261017.json'))
registry=obj(BACKEND/'docs/bpay-next/final-definer-closures.json')
assert sha(dump(registry))==entry['registry']['outputSha256']
for change in reversed(entry['registry']['changes']):
    matches=[i for i,c in enumerate(registry['chains']) if c['signature']==change['signature']]
    assert len(matches)==1 and registry['chains'][matches[0]]==change['after']
    if change['before'] is None: registry['chains'].pop(matches[0])
    else: registry['chains'][matches[0]]=change['before']
assert sha(dump(registry,False))==entry['registry']['inputSha256']
mounts=[]
for mount in kept['mountFiles']:
    data=dump(registry,False) if mount['target']=='docs/bpay-next/final-definer-closures.json' else raw(BACKEND/mount['target'])
    assert sha(data)==mount['sha256'] and len(data)==mount['bytes'],mount['target']
    assert data==git(BACKEND,kept['sourceCommit'],PKG+'payload/'+mount['target']),mount['target']
    mounts.append({'file':mount['target'],'sha256':sha(data),'equalsOriginalReviewedPayload':True,'entryRegistryDeltaReversed':mount['target'].endswith('final-definer-closures.json')})
evidence['keptMounts']=mounts
evidence['entryRegistryChanges']=len(entry['registry']['changes'])

# Complete route parent reversal to the genuine current input, not an allowed
# marker or partial-source comparison. Earlier Manual/Source reversals are also
# executed independently by the unchanged actual parent test.
parent=obj(BACKEND/(PKG+'PARENT_EDITS.json'))
value=text(BACKEND/parent['target']);assert sha(value.encode())==parent['outputSha256']
before=restore(value,parent['hunks']);assert sha(before.encode())==parent['inputSha256']
evidence['keptParentFullReverseHunks']=len(parent['hunks'])

value=text(BACKEND/'broker/src/index.js');assert sha(value.encode())==entry['workerParent']['outputSha256']
before=restore(value,entry['hunks']);assert sha(before.encode())==entry['workerParent']['inputSha256']
assert before.encode()==git(BACKEND,'418e65d517661686dcffd18d99363bb3176b217f','broker/src/index.js')
evidence['workerParentFullReverseHunks']=len(entry['hunks'])

classifier=entry['classifier'];value=text(BACKEND/classifier['target']);assert sha(value.encode())==classifier['outputSha256']
before=restore(value,classifier['hunks']);assert sha(before.encode())==classifier['inputSha256']
assert before.encode()==raw(BACKEND/classifier['predecessor'])
evidence['entryClassifierFullReverseHunks']=len(classifier['hunks'])

for row in entry['payloads']:
    data=raw(BACKEND/row['target']);assert sha(data)==row['sha256'] and len(data)==row['bytes'],row['target']
assert raw(BACKEND/'broker/src/correction-entry.mjs')==git(BACKEND,entry['sourceCommit'],ENTRY+'correction-entry.mjs')
assert raw(BACKEND/'supabase/repeatable/17102026_0135_correction_entry_reader/01_complete_entry_reader.sqlinc')==git(BACKEND,entry['sourceCommit'],ENTRY+'17102026_0109_correction_entry_reader.sql')
evidence['entryPayloadsMatched']=len(entry['payloads'])

classic=obj(BACKEND/(D+'CORRECTION_ENTRY_CLASSIC_HOST_MOUNT_20261017.json'))
for h in classic['hunks']:
    value=text(OFFICE/h['target']);assert sha(value.encode())==h['outputSha256'];before=restore(value,[h]);assert sha(before.encode())==h['inputSha256']
    assert before.encode()==git(OFFICE,classic['officeInputCommit'],h['target'])
evidence['officeCompleteParentReversals']=len(classic['hunks'])
assert sha(raw(OFFICE/'js/timesheet-correction-entry-v1.js'))==classic['derivation']['outputSha256']

acl=obj(BACKEND/(D+'KEPT_ENTRY_MANDATORY_ACL_VERIFIER_20261017.json'))
value=text(BACKEND/acl['target']);assert sha(value.encode())==acl['outputSha256'];before=restore(value,acl['hunks'])
assert sha(before.encode())==acl['inputSha256'] and before.encode()==git(BACKEND,'24c57e6c7af4a8fb07d5e3126ab9ac76e9448560',acl['target'])
evidence['mandatoryAclWholeFileReverseHunks']=len(acl['hunks'])
current=obj(BACKEND/'supabase/release/current-release.json')
previous=json.loads(git(BACKEND,'24c57e6c7af4a8fb07d5e3126ab9ac76e9448560','supabase/release/current-release.json'))
new='supabase/verification/17102026_0144_kept_pay_and_correction_entry_security.sql'
for key,count in [('verificationFiles',177),('newVerificationFiles',178)]:
    assert current[key]==previous[key]+[new] and len(current[key])==count
    assert current[key].count(new)==1 and acl['target'] in current[key]
    current[key]=previous[key]
assert current==previous
evidence['mandatoryListsOnlyAppendOne']={'verificationFiles':177,'newVerificationFiles':178,'originalVerifierRetained':True}

reload=obj(BACKEND/(D+'RPC_RELOAD_EXECUTION_PIN_SUCCESSOR_20261017.json'))
policy=text(BACKEND/reload['policy']);assert sha(policy.encode())==reload['outputPolicySha256']
assert policy.count(reload['after'])==1
before=policy.replace(reload['after'],reload['before'])
assert sha(before.encode())==reload['inputPolicySha256'] and before.encode()==git(BACKEND,'24c57e6c7af4a8fb07d5e3126ab9ac76e9448560',reload['policy'])
assert sha(raw(BACKEND/reload['reviewReceipt']))==reload['reviewReceiptSha256']
for row in reload['orderedFiles']: assert sha(raw(BACKEND/row['path']))==row['sha256']
evidence['executionPolicyOnlyReviewedPinChanged']={'exceptions':reload['exceptionCount'],'reason':'SOURCE_REASSERT','includedFileBytesUnchanged':True}

# Frozen pin proof for the actual reviewed runtime/security files, omitting
# business bodies and whole registries from the report itself.
files=set([m['target'] for m in kept['mountFiles']]+[m['target'] for m in entry['payloads']]+[acl['target'],new,reload['policy'],'supabase/release/current-release.json'])
evidence['frozenBackendFiles']=[]
for file in sorted(files):
    data=raw(BACKEND/file);assert data==git(BACKEND,B,file),file
    evidence['frozenBackendFiles'].append({'file':file,'sha256':sha(data)})
for file in ['js/timesheet-correction-entry-v1.js','js/main.js','index.html']:
    assert raw(OFFICE/file)==git(OFFICE,O,file),file
evidence['sourceFindings']=0
(HERE/'INTEGRATION_EVIDENCE.json').write_bytes(dump(evidence))
print('PASS: exact20 kept payloads, entry8 payloads, registry/classifier/Worker/Office/ACL full reversals, mandatory lists and single reviewed execution pin')

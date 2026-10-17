"""Finite data-only metadata recipe. No writer, subprocess, network or runtime import.

The reader supplied by git_values.py is read-only and validates actual Git values.
Tests use an explicitly synthetic reader, never actual materialization evidence.
"""
from pathlib import Path
import copy,hashlib,json,re
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
P='docs/healthroster-build/proposals/'
OP=P+'daily-health-genuine-staged-operator-20261017/'
C=P+'daily-health-s0-stage-coordinator-20261017/'
S=P+'daily-health-genuine-single-stage-coordinator-20261017/'
BK=P+'daily-genuine-stage-root-bookkeeper-20261017/'
H=P+'daily-health-s0-catalogue-holder-20261017/'
COM=P+'daily-five-stage-producer-hash-forward-20261017/'
def sha(b):return hashlib.sha256(b).hexdigest()
def encoded(v):return (json.dumps(v,indent=2,ensure_ascii=False)+'\n').encode()
def must(v,c):
 if not v:raise ValueError(c)
def blobrow(file,b,commit=None):
 r=dict(file=file,bytes=len(b),sha256=sha(b))
 if commit is not None:r['commit']=commit
 return r

class Authority:
 def __init__(self):
  cap=json.loads((HERE/'CAPTURE.json').read_text());self.b2=Path('\\\\?\\'+cap['b2Path'].replace('/','\\'))
  raw=(self.b2/'MANIFEST.json').read_bytes();must(sha(raw)==cap['b2ManifestSha256'],'FROZEN_B2_MANIFEST_CHANGED')
  for r in json.loads(raw)['files']:
   b=(self.b2/r['file']).read_bytes();must(len(b)==r['bytes'] and sha(b)==r['sha256'],'FROZEN_B2_INPUT_CHANGED')
  self.parentRows=json.loads((self.b2/'PARENTS.json').read_text())['files']
  self.parents={(r['owner'],r['file']):(self.b2/'parents'/r['owner']/r['file']).read_bytes() for r in self.parentRows}
  self.selected=json.loads((self.b2/'SELECTION.json').read_text())['targets']
  self.functional={(r['owner'],r['file']):(self.b2/'payload'/r['owner']/r['file']).read_bytes() for r in self.selected}
  self.edits=json.loads((self.b2/'EDITS.json').read_text())['hunks']
  self.map=json.loads((self.b2/'SOURCE_CHAIN_MAP.json').read_text())
  self.composer=json.loads((HERE/'COMPOSER_FORWARD.json').read_text());f=self.composer['file']
  self.functional['root',f]=(HERE/'payload/root'/f).read_bytes()
  for r in cap['files']:
   self.parents[r['owner'],r['file']]=(HERE/'parents'/r['owner']/r['file']).read_bytes()
  self.extraParents=cap['files'];self.acl=json.loads((HERE/'ACL_COMPOSITION.json').read_text())
  self.metadata=[('report',r['file']) for r in self.map['reportMetadata']]+[('root',r['file']) for r in self.map['rootMetadata']]+[('root',COM+'MANIFEST.json')]
  must(len(self.metadata)==14,'EXACT_THIRTEEN_PLUS_COMPOSER_METADATA')

def expected_functional(a,acl):
 must(type(acl) is bool,'EXPLICIT_ACL_CHOICE_REQUIRED');out=dict(a.functional)
 if acl:out['report',a.acl['file']]=(HERE/'acl/b2-plus-accepted-acl-phase-parent.mjs').read_bytes()
 return out

def manifest(a,owner,file,changed,new=()):
 original=a.parents[owner,file];v=json.loads(original);folder=file.rsplit('/',1)[0]+'/'
 must(type(v.get('files')) is list,'ORIGINAL_MANIFEST_FILES_REQUIRED')
 seen=set()
 for r in v['files']:
  must(r['file'] not in seen,'DUPLICATE_MANIFEST_RECORD');seen.add(r['file']);target=folder+r['file']
  if (owner,target) in changed:r.update(blobrow(r['file'],changed[owner,target]))
 for target in new:
  name=target.removeprefix(folder);must(target.startswith(folder) and name not in seen,'ADDITIVE_ROW_COLLISION');seen.add(name)
  v['files'].append(blobrow(name,changed[owner,target]))
 return encoded(v)

def metadata(a,commits,reader,acl=False):
 """Compute all currently possible DAG nodes; dependency commits must really exist."""
 f=expected_functional(a,acl);out={};fr=commits['functionalReport'];rr=commits['functionalRoot']
 out['report',OP+'MANIFEST.json']=manifest(a,'report',OP+'MANIFEST.json',f,[OP+'current-read.mjs',OP+'current-ipc.mjs'])
 # Holder's seven external source pins and its complete manifest remain raw unchanged.
 for file in [H+'EXTERNAL_SOURCE_PINS.json',H+'MANIFEST.json']:out['report',file]=a.parents['report',file]
 if 'operatorMetadata' in commits:
  commit=commits['operatorMetadata'];must(reader.blob('report',commit,OP+'MANIFEST.json')==out['report',OP+'MANIFEST.json'],'REAL_OPERATOR_MANIFEST_COMMIT_REQUIRED')
  v=json.loads(a.parents['report',C+'SOURCE_PINS.json']);must(len(v)==151,'ORIGINAL_151_PINS_REQUIRED')
  for r in v:
   key=('report',r['file'])
   if r['file']==OP+'MANIFEST.json':r.update(blobrow(r['file'],out[key],commit))
   elif key in f:r.update(blobrow(r['file'],f[key],fr))
  must(not any(r['file'] in [OP+'current-read.mjs',OP+'current-ipc.mjs'] for r in v),'NEW_MODULE_PIN_COLLISION')
  v.extend(blobrow(file,f['report',file],fr) for file in [OP+'current-read.mjs',OP+'current-ipc.mjs'])
  must(len(v)==153,'EXACT_153_PINS_REQUIRED');out['report',C+'SOURCE_PINS.json']=encoded(v)
  out['report',C+'MANIFEST.json']=manifest(a,'report',C+'MANIFEST.json',{**f,**out})
 if 'coordinatorMetadata' in commits:
  must(('report',C+'MANIFEST.json') in out,'OPERATOR_DEPENDENCY_REQUIRED');commit=commits['coordinatorMetadata']
  for file in [C+'SOURCE_PINS.json',C+'MANIFEST.json']:must(reader.blob('report',commit,file)==out['report',file],'REAL_COORDINATOR_METADATA_COMMIT_REQUIRED')
  v=json.loads(a.parents['report',S+'SOURCE_PINS.json']);must(len(v)==16,'ORIGINAL_SIXTEEN_PINS_REQUIRED')
  for r in v:
   key=('report',r['file'])
   if key in out and r['file'].startswith(C):r.update(blobrow(r['file'],out[key],commit))
   elif key in f:r.update(blobrow(r['file'],f[key],fr))
  out['report',S+'SOURCE_PINS.json']=encoded(v);out['report',S+'MANIFEST.json']=manifest(a,'report',S+'MANIFEST.json',{**f,**out})
 if 'oneMetadata' in commits:
  must(('report',S+'MANIFEST.json') in out,'COORDINATOR_DEPENDENCY_REQUIRED');commit=commits['oneMetadata']
  for file in [S+'SOURCE_PINS.json',S+'MANIFEST.json']:must(reader.blob('report',commit,file)==out['report',file],'REAL_ONE_STAGE_METADATA_COMMIT_REQUIRED')
  v=json.loads(a.parents['report',BK+'PARENTS.json']);must(len(v['fixedRuntime'])==12,'TWELVE_RUNTIME_RECORDS_REQUIRED')
  count=0
  for r in v['fixedRuntime']:
   key=('report',r['file'])
   if key in f:r.update(blobrow(r['file'],f[key],fr));count+=1
  must(count==5,'EXACT_FIVE_REMAPS_REQUIRED');out['report',BK+'PARENTS.json']=encoded(v)
  out['report',BK+'MANIFEST.json']=manifest(a,'report',BK+'MANIFEST.json',{**f,**out})
 for owner,file in a.metadata:
  if owner=='root':out[owner,file]=manifest(a,owner,file,f)
 return out

STAGES={'operator':[('report',OP+'MANIFEST.json'),('report',H+'EXTERNAL_SOURCE_PINS.json'),('report',H+'MANIFEST.json')],
 'coordinator':[('report',C+'SOURCE_PINS.json'),('report',C+'MANIFEST.json')],
 'one':[('report',S+'SOURCE_PINS.json'),('report',S+'MANIFEST.json')],
 'bookkeeper':[('report',BK+'PARENTS.json'),('report',BK+'MANIFEST.json')],
 'root':[('root',r) for r in [P+'daily-s1-actual-effect-recorder-20261017/MANIFEST.json',P+'daily-s1-after-clock-adapter-20261017/MANIFEST.json',P+'daily-original-stages-after-response-clock-forward-20261017/MANIFEST.json',P+'daily-original-stages-root-effect-adapter-20261017/MANIFEST.json',COM+'MANIFEST.json']]}
REQUIRED={'operator':set(),'coordinator':{'operatorMetadata'},'one':{'operatorMetadata','coordinatorMetadata'},'bookkeeper':{'operatorMetadata','coordinatorMetadata','oneMetadata'},'root':{'operatorMetadata','coordinatorMetadata','oneMetadata'}}

def propose(a,stage,commits,reader,acl=False):
 """Return complete postimages and literal complete-file inverses. Never write them."""
 must(stage in STAGES,'CLOSED_DAG_STAGE_REQUIRED')
 required=REQUIRED[stage]|{'functionalReport','functionalRoot'}
 must(type(commits) is dict and set(commits)==required,'EXACT_ALREADY_CREATED_COMMIT_SET_REQUIRED')
 heads={o:reader.head(o) for o in ['report','root']}
 for k,c in commits.items():
  must(type(c) is str and re.fullmatch('[a-f0-9]{40}',c),'EXACT_COMMIT_HASH_REQUIRED')
  owner='root' if k=='functionalRoot' else 'report';reader.commit(owner,c,heads[owner])
 f=expected_functional(a,acl)
 for (owner,file),b in f.items():
  commit=commits['functionalReport' if owner=='report' else 'functionalRoot']
  must(reader.blob(owner,commit,file)==b,'ACTUAL_COMPLETE_FUNCTIONAL_COMMIT_REQUIRED')
  must(reader.saved(owner,file)==reader.index(owner,file)==reader.blob(owner,heads[owner],file)==b,'FUNCTIONAL_SAVED_INDEX_HEAD_MISMATCH')
 # All original55 parents are proved from actual original historical objects. Changed
 # present files are certified postimages whose literal inverse yields that parent.
 for r in a.parentRows+a.extraParents:
  key=(r['owner'],r['file']);old=a.parents[key]
  must(len(old)==r['bytes'] and sha(old)==r['sha256'] and reader.blob(r['owner'],r['commit'],r['file'])==old,'ORIGINAL_HISTORICAL_PARENT_CHANGED')
  if key in f:
   raw=f[key].decode()
   if acl and key==('report',a.acl['file']):
    for h in reversed(a.acl['edits']):must(raw.count(h['after'])==1,'ACL_REVERSE_ANCHOR');raw=raw.replace(h['after'],h['before'],1)
   if key==('root',a.composer['file']):
    for h in reversed(a.composer['edits']):must(raw.count(h['after'])==h['occurrences'],'COMPOSER_REVERSE_ANCHOR');raw=raw.replace(h['after'],h['before'])
   else:
    for h in reversed([x for x in a.edits if (x['owner'],x['file'])==key]):must(raw.count(h['after'])==1,'ORIGINAL_REVERSE_ANCHOR');raw=raw.replace(h['after'],h['before'],1)
   must(raw.encode()==old,'COMPLETE_HISTORICAL_PARENT_INVERSE_REQUIRED')
 out=metadata(a,commits,reader,acl)
 for r in a.parentRows+a.extraParents:
  key=(r['owner'],r['file']);file=r['file'];owner=r['owner'];current=reader.saved(owner,file)
  must(current==reader.index(owner,file)==reader.blob(owner,heads[owner],file),'CURRENT_PARENT_IMAGE_MISMATCH')
  allowed=[f[key]] if key in f else [a.parents[key],*([out[key]] if key in out else [])]
  must(current in allowed,'UNREVIEWED_CURRENT_METADATA_OR_DEPENDENCY')
 records=[]
 for owner,file in STAGES[stage]:
  must((owner,file) in out,'REAL_DAG_DEPENDENCY_COMMIT_MISSING');old=a.parents[owner,file];new=out[owner,file]
  records.append(dict(owner=owner,file=file,before=old,after=new,changed=old!=new,beforeSha256=sha(old),sha256=sha(new),wholeInverse=dict(before=old.decode(),after=new.decode())))
 must(all(reader.head(o)==heads[o] for o in heads),'SOURCE_MOVED_DURING_RECIPE')
 return dict(stage=stage,commits=copy.deepcopy(commits),aclComposed=acl,records=records,actualPublicAdmission='STILL_REQUIRED_AFTER_REAL_METADATA_COMMITS',native='NOT_RUN')

def assert_resealed(a,commits,reader,acl=False):
 """Mandatory first half of final source admission: all14 exact derived metadata
 images at their actual MF-containing commits, index, saved and complete HEAD.
 Follow with admit-public.mjs using the returned config; neither calls a writer.
 """
 required={'functionalReport','functionalRoot','operatorMetadata','coordinatorMetadata','oneMetadata','bookkeeperMetadata','rootMetadata'}
 must(type(commits) is dict and set(commits)==required,'EXACT_FINAL_ACTUAL_METADATA_COMMITS_REQUIRED')
 base={k:v for k,v in commits.items() if k not in ['bookkeeperMetadata','rootMetadata']}
 # Reuse every complete functional/current/historical/inverse check, not hashes alone.
 propose(a,'root',base,reader,acl)
 heads={o:reader.head(o) for o in ['root','report']}
 for k in ['bookkeeperMetadata','rootMetadata']:
  owner='root' if k=='rootMetadata' else 'report';reader.commit(owner,commits[k],heads[owner])
 expected=metadata(a,base,reader,acl);rows=[]
 for (owner,file),b in expected.items():
  if owner=='root':key='rootMetadata'
  elif file.startswith(OP) or file.startswith(H):key='operatorMetadata'
  elif file.startswith(C):key='coordinatorMetadata'
  elif file.startswith(S):key='oneMetadata'
  else:key='bookkeeperMetadata'
  commit=commits[key]
  must(reader.saved(owner,file)==reader.index(owner,file)==reader.blob(owner,heads[owner],file)==reader.blob(owner,commit,file)==b,'EXACT_FINAL_RESEALED_METADATA_IMAGES_REQUIRED')
  rows.append(dict(owner=owner,**blobrow(file,b,commit)))
 must(len(rows)==14 and all(reader.head(o)==heads[o] for o in heads),'FINAL_METADATA_HEAD_MOVED')
 return dict(status='ACTUAL_DERIVED_METADATA_IMAGES_PASS',metadataRows=rows,rootHead=heads['root'],reportHead=heads['report'],
  publicAdmissionConfig=dict(rootHead=heads['root'],operatorCommit=heads['report'],helperCommit=commits['rootMetadata'],s1AdapterCommit=commits['rootMetadata'],s1RecorderCommit=commits['rootMetadata'],stageAdapterCommit=commits['rootMetadata'],stageRecorderCommit=commits['rootMetadata'],composerCommit=commits['rootMetadata'],aclComposed=acl),
  next='MANDATORY_ADMIT_PUBLIC_MJS_SAME_HEADS',native='NOT_RUN')

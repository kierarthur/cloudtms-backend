from capture import HERE,OP,S0,ONE,BOOK,HOLDER,P,dump,sha
import json
def main():
 selection=json.loads((HERE/'SELECTION.json').read_text())['targets'];selected={r['file']:r for r in selection if r['owner']=='report'}
 metadata=[OP+'MANIFEST.json',S0+'SOURCE_PINS.json',S0+'MANIFEST.json',ONE+'SOURCE_PINS.json',ONE+'MANIFEST.json',BOOK+'PARENTS.json',BOOK+'MANIFEST.json',HOLDER+'EXTERNAL_SOURCE_PINS.json',HOLDER+'MANIFEST.json']
 downstream={OP+'MANIFEST.json',S0+'SOURCE_PINS.json',S0+'MANIFEST.json',ONE+'SOURCE_PINS.json',ONE+'MANIFEST.json',BOOK+'PARENTS.json'}
 rows=[]
 for file in metadata:
  old=json.loads((HERE/'parents/report'/file).read_text());matches=[]
  def walk(v,parts=()):
   if isinstance(v,list):
    for i,x in enumerate(v):walk(x,parts+(str(i),))
   elif isinstance(v,dict):
    if isinstance(v.get('file'),str):
     target=v['file'] if v['file'].startswith('docs/') else str(PathLike(file).parent+'/'+v['file'])
     if target in selected or target in downstream:matches.append(dict(pointer='/'+ '/'.join(parts),oldRecord=v,target=target,postimage=selected.get(target),successorCommit=None,requires='REAL_REVIEWED_COMMIT_AND_COMPLETE_METADATA_REVERSE'))
    for k,x in v.items():walk(x,parts+(k,))
  walk(old);b=(HERE/'parents/report'/file).read_bytes();rows.append(dict(file=file,bytes=len(b),sha256=sha(b),affectedRecords=matches))
 fixed=json.loads((HERE/'parents/report'/BOOK/'PARENTS.json').read_text())['fixedRuntime'];assert len(fixed)==12
 rootMeta=[P+'daily-s1-actual-effect-recorder-20261017/MANIFEST.json',P+'daily-s1-after-clock-adapter-20261017/MANIFEST.json',P+'daily-original-stages-after-response-clock-forward-20261017/MANIFEST.json',P+'daily-original-stages-root-effect-adapter-20261017/MANIFEST.json']
 roots=[]
 for f in rootMeta:
  old=json.loads((HERE/'parents/root'/f).read_text());direct=[]
  for i,r in enumerate(old['files']):
   target=f.rsplit('/',1)[0]+'/'+r['file'];new=next((x for x in selection if x['owner']=='root' and x['file']==target),None)
   if new:direct.append(dict(pointer='/files/'+str(i),oldRecord=r,target=target,postimage=new))
  roots.append(dict(file=f,affectedRecords=direct))
 result=dict(version='B2_EXACT_SOURCE_CHAIN_UPDATE_MAP_V1',status='ADMISSION_HELD_NO_FAKE_FUTURE_COMMIT',reportParent='6b5ebedecf95c8a1adfda5141bbf3eccf5294d23',reportMetadata=rows,rootMetadata=roots,originalTwelveRuntimeBindings=[dict(oldRecord=r,postimage=selected.get(r['file']),disposition='REMAP_TO_REAL_RUNTIME_OWNER_COMMIT' if r['file'] in selected else 'PRESERVE_EXACT_ORIGINAL_RECORD') for r in fixed],newOperatorRows=[r for r in selection if r['owner']=='report' and r.get('parent')=='ABSENT'],unchangedHolder=True,sourceGuards='UNCHANGED_FULL_CURRENT_SAVED_INDEX_HEAD_HISTORICAL',optionalAclComposition=dict(moneyPostimageSha256='e77e1753e43039a9862e40dc4ba3bc487480356a20805991a036d852f77e97a2',selected=False,required='COMPOSE_LITERAL_APPROVED_ACL_HUNK_THEN_REGENERATE_EXACT_PHASE_PARENT_HASH_AND_EVERY_AFFECTED_RECORD'),recipe='NEW_ACTUAL_REPORT_COMMIT_REQUIRES_GENUINE_S0_SAME_SOURCE_PREFIX_NO_REBINDING_OF_OLD_R9')
 dump('SOURCE_CHAIN_MAP.json',result)
 print(json.dumps(dict(metadata=len(rows)+len(roots),runtimeBindings=len(fixed),changedRuntimeBindings=sum(r['file'] in selected for r in fixed))))
class PathLike:
 def __init__(self,s):self.parent=s.rsplit('/',1)[0]
if __name__=='__main__':main()

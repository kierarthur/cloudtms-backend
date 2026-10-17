"""Actual historical B2 proof only. No current-image substitution/virtual Git."""
from pathlib import Path
import json,sys
HERE=Path(__file__).resolve().parent
ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'
REL='docs/healthroster-build/proposals/daily-b2-real-commit-reseal-source-20261017/'
def historical_baseline(a,base,reader,expected_functional,metadata,sha,must):
 commits=base['config']['metadataCommits'];heads={o:reader.head(o) for o in ['report','root']}
 for k,c in commits.items():reader.commit('root' if k in ['functionalRoot','rootMetadata'] else 'report',c,heads['root' if k in ['functionalRoot','rootMetadata'] else 'report'])
 for o in heads:reader.commit(o,base[o+'Commit'],heads[o])
 f=expected_functional(a,False);out=metadata(a,commits,reader,False)
 # Literal original complete-parent inverse algorithm retained. Real original Git objects only.
 for r in a.parentRows+a.extraParents:
  key=(r['owner'],r['file']);old=a.parents[key]
  must(len(old)==r['bytes'] and sha(old)==r['sha256'] and reader.blob(r['owner'],r['commit'],r['file'])==old,'ORIGINAL_HISTORICAL_PARENT_CHANGED')
  if key in f:
   raw=f[key].decode()
   if key==('root',a.composer['file']):
    for h in reversed(a.composer['edits']):must(raw.count(h['after'])==h['occurrences'],'COMPOSER_REVERSE_ANCHOR');raw=raw.replace(h['after'],h['before'])
   else:
    for h in reversed([x for x in a.edits if (x['owner'],x['file'])==key]):must(raw.count(h['after'])==1,'ORIGINAL_REVERSE_ANCHOR');raw=raw.replace(h['after'],h['before'],1)
   must(raw.encode()==old,'COMPLETE_HISTORICAL_PARENT_INVERSE_REQUIRED')
 for (o,file),b in f.items():must(reader.blob(o,commits['functionalReport' if o=='report' else 'functionalRoot'],file)==b,'ORIGINAL_COMPLETE_FUNCTIONAL_COMMIT_REQUIRED')
 for (o,file),b in out.items():
  if o=='root':k='rootMetadata'
  elif file.startswith('docs/healthroster-build/proposals/daily-health-genuine-staged-operator-') or file.startswith('docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-'):k='operatorMetadata'
  elif file.startswith('docs/healthroster-build/proposals/daily-health-s0-stage-coordinator-'):k='coordinatorMetadata'
  elif file.startswith('docs/healthroster-build/proposals/daily-health-genuine-single-stage-coordinator-'):k='oneMetadata'
  else:k='bookkeeperMetadata'
  must(reader.blob(o,commits[k],file)==b,'ORIGINAL_REAL_METADATA_OWNER_COMMIT_REQUIRED')
 expected={**a.parents,**f,**out};must(len(a.parentRows)==55 and len(f)==19 and len(out)==14,'EXACT_ORIGINAL_B2_COUNTS_REQUIRED')
 rows=[]
 for (o,file),b in sorted(expected.items()):
  must(reader.blob(o,base[o+'Commit'],file)==b,'ORIGINAL_ACCEPTED_BASELINE_IMAGE_REQUIRED');rows.append(dict(owner=o,file=file,bytes=len(b),sha256=sha(b)))
 must(rows==base['rows'],'EXACT_CAPTURED_BASELINE_ROWS_REQUIRED');must(all(reader.head(o)==heads[o] for o in heads),'HISTORICAL_PROOF_HEAD_MOVED')
 return dict(status='ORIGINAL_55_PARENT_B2_HISTORICAL_PROOF_PASS',originalParents=55,functionalRows=19,metadataRows=14,metadataImages=[dict(owner=o,file=file,bytes=len(b),sha256=sha(b)) for (o,file),b in sorted(out.items())],baselineRows=rows,heads=heads,native='NOT_RUN')
def main():
 # All loaded sources are pinned/committed by the invoking fixed JS gate first.
 sys.path.insert(0,ROOT+'/'+REL)
 from reseal import Authority,expected_functional,metadata,sha,must
 from git_values import GitValues
 b=json.loads((HERE/'B2_BASELINE.json').read_text());print(json.dumps(historical_baseline(Authority(),b,GitValues(),expected_functional,metadata,sha,must)))
if __name__=='__main__':main()

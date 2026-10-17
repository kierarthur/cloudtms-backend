"""Exact full source forward/reverse diff, no database or native calls."""
from pathlib import Path
import json,hashlib,difflib
here=Path(__file__).resolve().parent;base=here.parent
sources=[('canonical-input-builder.mjs','v6-contact-six-lock-mcp-successor-20261016/input-builder.mjs'),('input-builder.mjs','v6-contact-local-metadata-mcp-adapter-20261016/input-builder.mjs'),('fixed-metadata-reader.mjs','v6-contact-large-frame-feasibility-20261016/fixed-metadata-reader.mjs'),('result-validator.mjs','v6-contact-fresh-verification-successor-20261016/result-validator.mjs'),('hold-case.mjs','v6-contact-fresh-verification-successor-20261016/hold-case.mjs')]
rows=[];diffs=[];sha=lambda s:hashlib.sha256(s.encode()).hexdigest()
for file,old in sources:
 a=(base/old).read_text();z=(here/file).read_text();aa=a.splitlines(keepends=True);zz=z.splitlines(keepends=True);hunks=[]
 for group in difflib.SequenceMatcher(None,aa,zz,autojunk=False).get_grouped_opcodes(4):
  x=''.join(aa[group[0][1]:group[-1][2]]);y=''.join(zz[group[0][3]:group[-1][4]])
  if not x or not y or a.count(x)!=1 or z.count(y)!=1:hunks=[{'before':a,'after':z}];break
  hunks.append({'before':x,'after':y})
 s=a
 for h in hunks:assert s.count(h['before'])==1;s=s.replace(h['before'],h['after'])
 assert s==z
 for h in reversed(hunks):assert s.count(h['after'])==1;s=s.replace(h['after'],h['before'])
 assert s==a
 rows.append({'file':file,'predecessor':'../'+old,'beforeSha256':sha(a),'afterSha256':sha(z),'hunks':hunks,'wholeForwardAndReverse':True})
 diffs.extend(difflib.unified_diff(aa,zz,fromfile=old,tofile=here.name+'/'+file,n=0))
(here/'DELTA.json').write_text(json.dumps({'scope':'EXACT_COMPLETE_SOURCE_FORWARD_AND_REVERSE','changes':rows,'canonicalLiveRestorationChanged':False,'firstBProgramChanged':False},indent=2)+'\n')
(here/'SOURCE.diff').write_text(''.join(diffs))
print(json.dumps({'sourceReverses':len(rows),'canonicalLiveRestorationChanged':False,'native':'NOT_RUN'}))

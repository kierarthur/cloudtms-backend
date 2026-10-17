from pathlib import Path
import json,hashlib,subprocess
H=Path(__file__).resolve().parent
R=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
F='125915e9f5f43aaa7e4eb54cd8b8355d134bb81a'
target='docs/healthroster-build/proposals/daily-b2-r11-s0-publish-stageplans-20261017.mjs'
fixture='docs/healthroster-build/proposals/daily-health-genuine-auth-r11-b2-source-recipe-20261017/HISTORICAL_SOURCE_FIXTURE.json'
manifest=fixture.rsplit('/',1)[0]+'/MANIFEST.json'
sha=lambda b:hashlib.sha256(b).hexdigest()
git=lambda *a:subprocess.check_output(['git','-C',str(R),*a])
head=git('rev-parse','HEAD').decode().strip()
git('merge-base','--is-ancestor',F,head)
rows=[]
for name,file in [('publisher.mjs',target),('HISTORICAL_SOURCE_FIXTURE.json',fixture),('RECIPE_MANIFEST.json',manifest),('auth-supersession.mjs',fixture.rsplit('/',1)[0]+'/auth-supersession.mjs')]:
 b=(R/file).read_bytes()
 for ref in [':',head+':',F+':']:assert b==git('show',ref+file),(name,ref)
 dest=H/'parent'/name;dest.parent.mkdir(exist_ok=True);assert not dest.exists();dest.write_bytes(b)
 rows.append(dict(file=file,captured='parent/'+name,bytes=len(b),sha256=sha(b),historicalCommit=F))
row=next(x for x in json.loads((R/manifest).read_bytes())['files'] if x['file']=='HISTORICAL_SOURCE_FIXTURE.json')
assert row['bytes']==2975604 and row['sha256']=='a183d1f68a321cc1c1cf805e52d0452d54829e7c5ae1d055c851f98b75eadb81'
assert rows[1]['bytes']==row['bytes'] and rows[1]['sha256']==row['sha256']
before=(H/'parent/publisher.mjs').read_bytes();text=before.decode()
edits=[
 ("const physical=f=>fs.realpathSync.native(f).replaceAll('\\\\','/');", "const physical=f=>fs.realpathSync.native(f).replaceAll('\\\\','/');\nconst LARGE_RECIPE_FIXTURE=Object.freeze({file:'"+fixture+"',bytes:2975604,sha256:'"+row['sha256']+"'});"),
 ("s.size>0&&s.size<=2e6,'BOUNDED_PHYSICAL_PUBLIC_REQUIRED'", "s.size>0&&(file===LARGE_RECIPE_FIXTURE.file?s.size===LARGE_RECIPE_FIXTURE.bytes:s.size<=2e6),'BOUNDED_PHYSICAL_PUBLIC_REQUIRED'"),
 ("const b=fs.readFileSync(f);if(historical)", "const b=fs.readFileSync(f);if(file===LARGE_RECIPE_FIXTURE.file)check(b.length===LARGE_RECIPE_FIXTURE.bytes&&hash(b)===LARGE_RECIPE_FIXTURE.sha256,'EXACT_LARGE_PUBLIC_RECIPE_REQUIRED');if(historical)")]
for a,b in edits:assert text.count(a)==1;text=text.replace(a,b)
after=text.encode();inverse=text
for a,b in reversed(edits):assert inverse.count(b)==1;inverse=inverse.replace(b,a)
assert inverse.encode()==before
(H/'payload').mkdir();(H/'payload/publisher.mjs').write_bytes(after)
(H/'FORWARD.json').write_text(json.dumps(dict(version='R11_EXACT_PUBLIC_RECIPE_FIXTURE_READER_FORWARD_V1',root=str(R).replace('\\','/'),capturedHead=head,parentCommit=F,target=target,beforeBytes=len(before),beforeSha256=sha(before),bytes=len(after),sha256=sha(after),edits=[dict(before=a,after=b) for a,b in edits],wholeInverse=dict(before=before.decode(),after=text),parentImages=rows,recipeFUnchanged=True,reportK4Unchanged=True,authReceiptUnchanged=True,defaultBytes=2000000,exactFixture=row),indent=2)+'\n')
assert git('rev-parse','HEAD').decode().strip()==head
print(json.dumps(dict(targets=1,edits=len(edits),completeParentBytes=len(before),postBytes=len(after),parentImages=len(rows),fixtureBytes=row['bytes'],native='NOT_RUN')))

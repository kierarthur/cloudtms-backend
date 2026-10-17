// Pure evidence linkage. The actual verifier supplies fixed captured source and real public Git.
import{createHash}from'node:crypto';
const sha=b=>createHash('sha256').update(b).digest('hex'),must=(v,c)=>{if(!v)throw Error(c)},P='docs/healthroster-build/proposals/';
const pairs=[['daily-genuine-root-s0-b2-r11-bookkeeper-20261017.mjs','daily-genuine-root-s0-b2-bookkeeper-20261017.mjs'],['daily-genuine-s0-b2-r11-tools-orchestration-20261017.js.txt','daily-genuine-s0-b2-tools-orchestration-20261017.js.txt'],['daily-b2-r11-s0-publish-stageplans-20261017.mjs','daily-b2-s0-publish-stageplans-20261017.mjs'],['daily-five-stage-b2-r11-source-plan-20261017/compose.mjs','daily-five-stage-producer-hash-forward-20261017/compose.mjs']].map(([a,b])=>[P+a,P+b]);
export function verifyParentEvidence(entry,recipe,parents,readCaptured){
 must(entry.targets.length===4&&recipe.files.length===2,'EXACT_SIX_COMPLETE_PARENT_CLAIMS_REQUIRED');const checks=[];
 for(const [i,r]of entry.targets.entries())must(r.file===pairs[i][0]&&r.parentFile===pairs[i][1],'EXACT_ENTRY_PARENT_PAIR_REQUIRED');
 for(const [i,r]of recipe.files.entries())must(r.file===['recipe.mjs','materialize.mjs'][i]&&r.parentFile===P+'daily-health-genuine-auth-r10-source-recipe-20261017/'+r.file,'EXACT_RECIPE_PARENT_PAIR_REQUIRED');
 for(const r of [...entry.targets,...recipe.files]){
  const rows=parents.filter(p=>p.root==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'&&p.file===r.parentFile);must(rows.length===1,'ONE_GENUINE_CAPTURED_PARENT_REQUIRED');const parent=rows[0],b=readCaptured(parent.captured),claimed=Buffer.from(r.wholeInverse.before);
  must(b.length===parent.bytes&&sha(b)===parent.sha256,'RAW_CAPTURED_PARENT_METADATA_REQUIRED');
  must(r.beforeBytes===parent.bytes&&r.beforeSha256===parent.sha256&&claimed.length===parent.bytes&&claimed.equals(b),'COMPLETE_BEFORE_MUST_EQUAL_CAPTURED_PARENT');
  if(r.parentReference)must(readCaptured(r.parentReference).equals(b),'RECIPE_PARENT_REFERENCE_REQUIRED');checks.push({...parent,expected:b});
 }
 return checks;
}
export function verifyActualParentEvidence(checks,{head,read,blob,ancestor}){
 const heads=new Map();for(const r of checks){if(!heads.has(r.root))heads.set(r.root,head(r.root));const h=heads.get(r.root);ancestor(r.root,r.commit,h);const saved=read(r.root,r.file);must(saved.equals(r.expected)&&[h,'',r.commit].every(ref=>blob(r.root,ref,r.file).equals(r.expected)),'COMPLETE_BEFORE_MUST_EQUAL_ACTUAL_SAVED_INDEX_HEAD_HISTORY');}
 for(const[root,h]of heads)must(head(root)===h,'ACTUAL_PARENT_HEAD_MOVED');return checks.length;
}

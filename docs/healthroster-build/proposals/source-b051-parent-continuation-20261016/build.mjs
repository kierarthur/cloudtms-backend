// Authoring only: saves a bounded recipe and a reviewable whole Source parent.
// Does not modify either root target. Index full bytes are reproducible in memory.
import fs from'node:fs';import path from'node:path';import{execFileSync}from'node:child_process';import{here,root,hash,createRecipe,compose,targets}from'./recipe.mjs';
const recipe=createRecipe(),outputs=compose(recipe),current=[];
for(const f of recipe.files){const same=hash(fs.readFileSync(path.join(root,f.target)))===f.preimageSha256;current.push({target:f.target,matchesOriginalCheckpoint:same});if(process.argv.includes('--require-current')&&!same)throw Error('CURRENT_SAVED_ROOT_PREIMAGE_CHANGED');execFileSync(process.execPath,['--input-type=module','--check'],{input:outputs[f.target],maxBuffer:1_000_000,windowsHide:true});}
fs.writeFileSync(path.join(here,'PARENT_EDITS.json'),JSON.stringify(recipe,null,2)+'\n');
fs.writeFileSync(path.join(here,'PROPOSED_ROUTES.js'),outputs[targets.routes]);
console.log(JSON.stringify({parents:recipe.files.length,anchoredEdits:recipe.files.reduce((n,f)=>n+f.edits.length,0),currentSavedRoot:current,composition:'ORIGINAL39AA_RECIPE_ONLY_ROOT_MUST_COMPOSE_LATER_DAILY',fullReverse:true,syntax:'PASS',rootEdits:0,native:'NOT_RUN'}));

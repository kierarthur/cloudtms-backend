// Bounded document-name reconciliation input, not an implementation or native acceptance test.
import fs from 'node:fs';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const planPath='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_PLAN_V2_CONSOLIDATION_13102026/HEALTHROSTER_SELFBILL_IMPLEMENTATION_PLAN_V2/03_IMPLEMENTATION_PLAN.md';
const censusPath='docs/healthroster-build/proposals/current-final-classification-union-20261014/CURRENT_FINAL_DECLARATION_OWNERS.json';
const sha=b=>createHash('sha256').update(b).digest('hex');
const planBytes=fs.readFileSync(planPath),censusBytes=fs.readFileSync(censusPath),text=planBytes.toString(),census=JSON.parse(censusBytes);
assert.equal(sha(planBytes),'b9f5b95393bf2f4c338da8110895dc7f80cb896f389aa0f27ea1c01468aef2d3');
const named=[...new Set([...text.matchAll(/\b(?:public|private)\.[a-z][a-z0-9_]*(?=\()/g)].map(m=>m[0]))].sort();
const currentNames=new Set(census.functions.map(row=>row.name));
const nonFunctionNames=new Set(['public.tms_users','public.weekly_source_rule_set_versions','public.weekly_source_self_bill_reports']);
const absent=named.filter(name=>!currentNames.has(name));
const candidates=absent.filter(name=>!nonFunctionNames.has(name)).map(name=>({name,primaryPlanLines:text.split(/\r?\n/).flatMap((line,i)=>line.includes(name)?[i+1]:[]),status:'RECONCILE_ACTUAL_SEMANTIC_OWNER_OR_IMPLEMENT_MISSING_SOURCE'}));
const rootCommit=execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim();
assert.equal(rootCommit,'61ce56ff5a0df2e4e4a85fdbb9efc25267d3c966');
const receipt={sourceOnly:true,status:'LITERAL_NAME_CANDIDATES_FOR_MANUAL_SOURCE_RECONCILIATION_NOT_COMPLETENESS_VERDICT',rootCommit,planPath,planSha256:sha(planBytes),declarationCensusPath:censusPath,declarationCensusSha256:sha(censusBytes),censusAuthority:'Exact published b5 finite declaration name inventory; current0709 classification stage only redefines six already named owners. No current full-body/census equivalence claim.',planCallableLookingNames:named.length,distinctCensusDeclarationNames:currentNames.size,nonFunctionDocumentNames:absent.filter(name=>nonFunctionNames.has(name)),candidates,scope:'Literal planned names may be historical, aliases, or dynamic declarations. Each candidate requires actual source/caller/owner-clause semantic reconciliation. An absent name alone is not an implementation defect. No native catalogue or functional test ran.',nativeCalls:0};
fs.writeFileSync('docs/healthroster-build/install2-composition/OWNER_PLAN_CALLABLE_NAME_CANDIDATES_14102026.json',JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({sourceOnly:true,planNames:named.length,censusNames:currentNames.size,nonFunctionDocumentNames:receipt.nonFunctionDocumentNames.length,candidates:candidates.length,nativeCalls:0}));

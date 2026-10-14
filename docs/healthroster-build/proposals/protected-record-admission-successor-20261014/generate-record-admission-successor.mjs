import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),base=path.join(out,'../protected-record-evidence-current-root-install2-forward'),sha=b=>createHash('sha256').update(b).digest('hex'),save=(f,v)=>fs.writeFileSync(path.join(out,f),JSON.stringify(v,null,2)+'\n');
const entryBefore=fs.readFileSync(path.join(base,'scripts/weekly-source-protected-record-evidence.mjs'),'utf8'),entryHunk={before:"||env.CLOUDTMS_PROTECTED_ENVIRONMENT!=='database-test'||env.CLOUDTMS_ENVIRONMENT!=='TEST'",after:"||env.CLOUDTMS_PROTECTED_ENVIRONMENT!=='database-test'||env.CLOUDTMS_ENVIRONMENT!=='TEST'||env.CLOUDTMS_RELEASE_MODE!=='UPGRADE'"};assert.equal(entryBefore.split(entryHunk.before).length,2);const entryAfter=entryBefore.replace(entryHunk.before,()=>entryHunk.after);assert.equal(entryAfter.replace(entryHunk.after,()=>entryHunk.before),entryBefore);fs.writeFileSync(path.join(out,'scripts/weekly-source-protected-record-evidence.mjs'),entryAfter);
const workflowBefore=fs.readFileSync(path.join(base,'workflow/database-release.yml'),'utf8'),hunks=[{before:'    steps:\n      - uses: actions/checkout@v6',after:`    steps:
      - name: Admit TEST-only recording before any protected target job
        if: inputs.phase == 'RECORD_EVIDENCE'
        env:
          RECORD_ENVIRONMENT: \${{ inputs.environment }}
          RECORD_MODE: \${{ inputs.mode }}
        shell: bash
        run: |
          set -euo pipefail
          [[ "$RECORD_ENVIRONMENT" == "TEST" && "$RECORD_MODE" == "UPGRADE" ]] || { echo "RECORD_EVIDENCE is restricted to TEST UPGRADE before protected target admission"; exit 1; }
      - uses: actions/checkout@v6`},{before:'    needs: source-gate\n    runs-on: ubuntu-latest',after:"    needs: source-gate\n    if: inputs.phase != 'RECORD_EVIDENCE' || (inputs.environment == 'TEST' && inputs.mode == 'UPGRADE')\n    runs-on: ubuntu-latest"}];
// The generic workflow has two step lists; qualify the first occurrence by its
// exact source-gate context and preserve every ordinary phase byte otherwise.
hunks[0].before='      commit: ${{ steps.commit.outputs.sha }}\n'+hunks[0].before;hunks[0].after='      commit: ${{ steps.commit.outputs.sha }}\n'+hunks[0].after;
let workflowAfter=workflowBefore;for(const h of hunks){assert.equal(workflowAfter.split(h.before).length,2);workflowAfter=workflowAfter.replace(h.before,()=>h.after);}let reverse=workflowAfter;for(const h of [...hunks].reverse())reverse=reverse.replace(h.after,()=>h.before);assert.equal(reverse,workflowBefore);fs.writeFileSync(path.join(out,'workflow/database-release.yml'),workflowAfter);
save('COMPLETE_PROTECTED_ENTRY_REVERSE.json',{source_only:true,entry28_manifest_sha256:sha(fs.readFileSync(path.join(base,'REVIEW_MANIFEST.json'))),entry_before_sha256:sha(entryBefore),entry_after_sha256:sha(entryAfter),entry_hunk:entryHunk,entry_entire_predecessor_reverse:true,workflow_before_sha256:sha(workflowBefore),workflow_after_sha256:sha(workflowAfter),workflow_hunks:hunks,workflow_entire_predecessor_reverse:true,ordinary_release_modes_phases_and_approvals_unchanged:true,invalid_recording_target_refuses_before_environment_secret_assignment:true,no_new_sql_private_abi_bundle_or_native_operation:true});
console.log(JSON.stringify({source_only:true,entry_sha256:sha(entryAfter),workflow_sha256:sha(workflowAfter),entire_predecessor_reverse:true}));

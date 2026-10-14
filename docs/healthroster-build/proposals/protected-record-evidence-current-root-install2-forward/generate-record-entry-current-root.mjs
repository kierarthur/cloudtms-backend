import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),managed=path.join(out,'../managed-qualification-current-root-install2-forward');
const current='a63455e878391cb3ee9732165e58c0db517f48a7',deps='fe7b3dfc2946cf334ba88681ae425105f864c42f',report='ae49914f8b5acbf00c7ab11c2bd1db849ffb5bd6';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,maxBuffer:32*1024*1024});
const blob=(c,f)=>git(['show',c+':'+f]),save=(f,v)=>fs.writeFileSync(path.join(out,f),JSON.stringify(v,null,2)+'\n'),write=(f,v)=>{fs.mkdirSync(path.dirname(path.join(out,f)),{recursive:true});fs.writeFileSync(path.join(out,f),v);};
const original=blob(current,'.github/workflows/database-release.yml').toString('utf8');assert.equal(blob(deps,'.github/workflows/database-release.yml').toString('utf8'),original);
const base=fs.readFileSync(path.join(managed,'workflow/database-release.yml'),'utf8');
const hunks=[
 {before:'options: [PLAN, APPLY, AUTO_PLAN, AUTO]',after:'options: [PLAN, APPLY, AUTO_PLAN, AUTO, RECORD_EVIDENCE]'},
 {before:'      customer_key:',after:`      record_actor_user_id:
        description: Actual active TEST Office admin UUID for RECORD_EVIDENCE
        required: false
        type: string
      record_artifact_slot:
        description: One existing receipt slot for RECORD_EVIDENCE
        required: false
        type: choice
        options: [weekly_source_protected_test_setup_authorizations, weekly_source_off_rule_test_controls, weekly_source_protected_partial_native_evidence, weekly_source_protected_activation_probe_authorizations, weekly_source_protected_capability_qualifications, weekly_source_protected_prerequisite_qualifications, weekly_source_report_fin_native_comparisons, weekly_source_report_fin_qualifications]
      record_reason:
        description: Purpose of recording committed evidence against the same VERIFIED installation
        required: false
        type: string
      customer_key:`},
 {before:'      CLOUDTMS_RELEASE_REQUESTED_APPROVAL: ${{ inputs.approval }}',after:`      CLOUDTMS_RELEASE_REQUESTED_APPROVAL: \${{ inputs.approval }}
      CLOUDTMS_PROTECTED_ENVIRONMENT: \${{ inputs.environment == 'TEST' && 'database-test' || 'database-live' }}
      CLOUDTMS_RECORD_INPUT_COMMIT: \${{ needs.source-gate.outputs.commit }}
      CLOUDTMS_RECORD_ACTOR_USER_ID: \${{ inputs.record_actor_user_id }}
      CLOUDTMS_RECORD_ARTIFACT_SLOT: \${{ inputs.record_artifact_slot }}
      CLOUDTMS_RECORD_REASON: \${{ inputs.record_reason }}`},
 {before:'          if [[ "${{ inputs.phase }}" == "AUTO" || "${{ inputs.phase }}" == "AUTO_PLAN" ]]; then',after:`          if [[ "\${{ inputs.phase }}" == "RECORD_EVIDENCE" ]]; then
            [[ "\${{ inputs.environment }}" == "TEST" && "\${{ inputs.mode }}" == "UPGRADE" && "$GITHUB_REPOSITORY" == "kierarthur/cloudtms-backend" ]] || { echo "Recording is restricted to protected TEST on the canonical repository"; exit 1; }
            test -n "\${CLOUDTMS_EXPECTED_DATABASE:-}" || { echo "Missing protected exact TEST database"; exit 1; }
            expected="RECORD_EVIDENCE TEST \${{ needs.source-gate.outputs.commit }} \${CLOUDTMS_RECORD_ARTIFACT_SLOT} \${CLOUDTMS_RECORD_ACTOR_USER_ID}"
            [[ "\${CLOUDTMS_RELEASE_REQUESTED_APPROVAL:-}" == "$expected" ]] || { echo "Exact RECORD_EVIDENCE approval does not match commit, slot and Office actor"; exit 1; }
          fi
          if [[ "\${{ inputs.phase }}" == "AUTO" || "\${{ inputs.phase }}" == "AUTO_PLAN" ]]; then`},
 {before:'      - name: Read-only release plan\n        run:',after:"      - name: Read-only release plan\n        if: inputs.phase != 'RECORD_EVIDENCE'\n        run:"},
 {before:'      - name: Preserve bounded automatic release evidence',after:`      - name: Record committed evidence against the same VERIFIED TEST installation
        if: inputs.phase == 'RECORD_EVIDENCE' && inputs.environment == 'TEST' && inputs.mode == 'UPGRADE'
        timeout-minutes: 10
        run: node scripts/weekly-source-protected-record-evidence.mjs record
      - name: Preserve exact same-installed recording receipt
        if: always() && inputs.phase == 'RECORD_EVIDENCE'
        uses: actions/upload-artifact@v4
        with:
          name: same-installed-recording-\${{ needs.source-gate.outputs.commit }}
          path: .codex-tmp/weekly-source-post-verified-record-receipt.json
          if-no-files-found: warn
      - name: Preserve bounded automatic release evidence`},
 {before:'            echo "- Authority: ${{ inputs.environment ==',after:'            echo "- Authority: ${{ inputs.environment =='}
];
// The unchanged summary must describe the separate recording approval instead
// of inheriting the standing installation phrase. No ordinary release policy changes.
hunks.pop();hunks.push({before:`            echo "- Authority: \${{ inputs.environment == 'TEST' && inputs.mode == 'UPGRADE' && 'standing managed TEST UPGRADE' || 'exact commit-bound approval' }}"`,after:`            echo "- Authority: \${{ inputs.phase == 'RECORD_EVIDENCE' && 'exact same-installed recording approval' || (inputs.environment == 'TEST' && inputs.mode == 'UPGRADE' && 'standing managed TEST UPGRADE' || 'exact commit-bound approval') }}"`});
let after=base;for(const h of hunks){assert.equal(after.split(h.before).length,2,'Nonunique workflow hunk');after=after.replace(h.before,()=>h.after);}let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,'Nonunique workflow reverse');reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,base);
write('workflow/database-release.yml',after);
const managedProof=JSON.parse(fs.readFileSync(path.join(managed,'COMPLETE_CURRENT_SOURCE_REVERSE.json'))).proofs.find(r=>r.destination==='.github/workflows/database-release.yml');
let originalReverse=reverse;for(const h of [...managedProof.hunks].reverse())originalReverse=originalReverse.replace(h.after,()=>h.before);assert.equal(originalReverse,original);
save('COMPLETE_CURRENT_WORKFLOW_REVERSE.json',{source_only:true,input_commit:current,input_sha256:sha(original),managed_base_sha256:sha(base),managed_base_manifest_sha256:sha(fs.readFileSync(path.join(managed,'REVIEW_MANIFEST.json'))),recording_hunks:hunks,managed_existing_hunks:managedProof.hunks,output_sha256:sha(after),entire_managed_base_reverse:true,entire_original_current_reverse:true,ordinary_apply_auto_and_plan_authority_preserved:true,recording_has_no_install_or_plan_call:true});
const pins=new Map();function dependency(file){if(pins.has(file))return;let sourceCommit=deps,sourceFile=file,b;
 if(file.startsWith('scripts/weekly-source-report-fin-')){sourceCommit=report;sourceFile='docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014/forward/'+file;}
 b=blob(sourceCommit,sourceFile);const imports=[...b.toString('utf8').matchAll(/(?:from\s*|import\s*\(\s*)['"](\.[^'"]+)['"]/g)].map(m=>m[1]);
 pins.set(file,{file,source_commit:sourceCommit,source_file:sourceFile,bytes:b.length,sha256:sha(b),imports});write('dependency-reference/'+file,b);
 for(const imported of imports){const target=path.posix.normalize(path.posix.join(path.posix.dirname(file),imported));if(/\.(?:mjs|js)$/.test(target))dependency(target);}
}
const entry=fs.readFileSync(path.join(out,'scripts/weekly-source-protected-record-evidence.mjs'),'utf8');for(const m of entry.matchAll(/(?:from\s*|import\s*\(\s*)['"](\.[^'"]+)['"]/g))dependency(path.posix.normalize(path.posix.join('scripts',m[1])));
save('CURRENT_NATIVE_DEPENDENCY_PINS.json',{source_only:true,dependencies:[...pins.values()],report_source_commit:report,report_final_collector_and_mapper_seal_still_required:true,relative_import_closure_complete:true,current_module_copies_are_test_input_references_only:true});
const sourcePins=[['supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql'],['supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql'],['supabase/migrations/22082026_1507_cloudtms_database_release_control_plane.sql'],['supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/04_13_complete_nullable_physical_qualification_scope.sqlinc'],['supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/27_POST_VERIFIED_EVIDENCE_RECORD.sqlinc']];
sourcePins.push(['supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/21_q45_actor_event_closed_dto_cross_session_00_complete_current_catalogue.sqlinc']);
const tree=git(['ls-tree','-r','--name-only',deps]).toString('utf8').split('\n');const resolved=sourcePins.map(([file])=>{assert.ok(tree.includes(file),'Missing actual native schema/owner input '+file);return file;});
save('SOURCE_SCHEMA_OWNER_READS.json',{source_only:true,commit:deps,files:[...new Set(resolved)].map(file=>{const b=blob(deps,file);return {file,bytes:b.length,sha256:sha(b)};}),private_money_owners_are_exact_fe7_mounted_0430_sources:true,private_ledger_row_json_has_no_guessed_column_projection:true,actual_function_owner_session_user_current_user_and_test_identity_verified_before_service_request_dialect:true,office_actor_validated_by_actual_native_admin_owner:true,turn_pid_from_actual_distinct_backend_and_existing_lock_namespace:true});
console.log(JSON.stringify({source_only:true,workflow_sha256:sha(after),dependencies:pins.size,entry_sha256:sha(entry),full_current_reverse:true}));

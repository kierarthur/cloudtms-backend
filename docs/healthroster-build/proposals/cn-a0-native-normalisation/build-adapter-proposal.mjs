import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root,'explicit controlling root required');
const nativePath='broker/src/timesheet-correction-orchestrator.mjs';
const input=fs.readFileSync(path.join(root,nativePath));const sha=value=>crypto.createHash('sha256').update(value).digest('hex');
assert.equal(sha(input),'e51be6f1005c2a71487efe308ea01bfaba9eb3c3767b3c632ca72f7c1343e118','current root CN guards changed');
let next=input.toString('utf8').replace(/\r\n/g,'\n');const hunks=[];
function change(anchor,replacement){assert.equal(next.split(anchor).length,2,'changed CN anchor '+anchor.slice(0,80));hunks.push({anchor,replacement});next=next.replace(anchor,replacement);}
change(' * buildReplacementSnapshot?:(input:{prepare:object,actor:CorrectionActor})=>Promise<object>,',
  ' * buildReplacementSnapshot?:(input:{prepare:object,actor:CorrectionActor})=>Promise<object>,\n * buildOriginalSnapshot?:(input:{prepare:object,actor:CorrectionActor})=>Promise<object>,');
change('function snapshotIdentity(snapshot, expected, { original = false } = {}) {',
  'function snapshotIdentity(snapshot, expected, { original = false, canonicalInput = false } = {}) {');
change('      snapshot.is_stale !== false || snapshot.has_rate_issue !== false || snapshot.has_pay_channel_issue !== false ||',
  '      (snapshot.is_stale !== false && !(canonicalInput && !OWN(snapshot, \'is_stale\'))) || snapshot.has_rate_issue !== false || snapshot.has_pay_channel_issue !== false ||');
change("      typeof r.original_authorisation_required !== 'boolean') fail('CORRECTION_RECEIPT_INVALID');",
  "      typeof r.original_authorisation_required !== 'boolean' ||\n      (OWN(r, 'original_materialised_in_unit') && typeof r.original_materialised_in_unit !== 'boolean')) fail('CORRECTION_RECEIPT_INVALID');");
change(`        !sameUuid(r.original_financial_id, a.original_financial_id) || r.original_financial_authority_hash !== a.original_financial_authority_hash ||
        r.original_authorisation_required !== prepare.original_authorisation_required) fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');`,
`        r.original_authorisation_required !== prepare.original_authorisation_required ||
        r.original_materialised_in_unit !== prepare.original_materialisation_required ||
        (!prepare.original_materialisation_required && (!sameUuid(r.original_financial_id, a.original_financial_id) ||
          r.original_financial_authority_hash !== a.original_financial_authority_hash)) ||
        (prepare.original_materialisation_required && r.original_authorisation_required !== true)) fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');`);
change(`  } else if (value.original_current_snapshot != null || a.original_financial_id != null ||
             value.original_authorisation_required !== true) fail('CORRECTION_PREPARE_AUTHORITY_INVALID');`,
`  } else if (value.original_current_snapshot != null || a.original_financial_id != null ||
             a.original_financial_authority_hash != null || frozen.financial_authority_hash != null ||
             frozen.financial_snapshot != null || t0.authorised_at_server != null ||
             value.original_authorisation_required !== true) fail('CORRECTION_PREPARE_AUTHORITY_INVALID');`);
change(`    if (prepare.original_materialisation_required) {
      // No claim that preview/calculator JSON equals the bounded writer's stored output.
      // Root must prove and add an exact native normalization contract before enabling A0 missing-finance.
      fail('CORRECTION_ORIGINAL_NORMALISATION_UNPROVED', 503);
    }`,
`    let original;
    if (prepare.original_materialisation_required) {
      if (typeof deps.buildOriginalSnapshot !== 'function') fail('CORRECTION_CANONICAL_ORIGINAL_PRODUCER_UNAVAILABLE', 503);
      original = cloneJson(await deps.buildOriginalSnapshot({ prepare, actor: a }));
      snapshotIdentity(original, {
        timesheetId: prepare.authority.original_timesheet_id, version: prepare.authority.original_timesheet_version,
        candidateId: prepare.candidate_id, clientId: prepare.client_id
      }, { canonicalInput: true });
    }`);
change(`    });
    const reversal = cloneFrozenCorrectionReversal(prepare.original_current_snapshot, {
      timesheetId: authority.planned_reversal_timesheet_id, version: 1,
      candidateId: prepare.candidate_id, clientId: prepare.client_id
    });
    const prepared = { prepare_fingerprint: prepare.prepare_fingerprint, replacement_snapshot: replacement, reversal_snapshot: reversal };`,
`    }, { canonicalInput: true });
    const prepared = { prepare_fingerprint: prepare.prepare_fingerprint, replacement_snapshot: replacement };
    if (prepare.original_materialisation_required) {
      prepared.original_snapshot = original;
      // No A0 reversal is calculated from preview input. The existing atomic
      // SQL writes/rereads actual original and signs that stored output.
    } else {
      prepared.reversal_snapshot = cloneFrozenCorrectionReversal(prepare.original_current_snapshot, {
        timesheetId: authority.planned_reversal_timesheet_id, version: 1,
        candidateId: prepare.candidate_id, clientId: prepare.client_id
      });
    }`);
const output=path.join(here,'timesheet-correction-orchestrator.mjs');fs.writeFileSync(output,next);
const diff=spawnSync('git',['diff','--no-index','--',path.join(root,nativePath),output],{encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});assert.ok([0,1].includes(diff.status),diff.stderr);
const lines=diff.stdout.split('\n');fs.writeFileSync(path.join(here,'ADAPTER_ROOT.diff'),`--- a/${nativePath}\n+++ b/${nativePath}\n${lines.slice(lines.findIndex(line=>line.startsWith('@@'))).join('\n')}`);
fs.writeFileSync(path.join(here,'ADAPTER_SOURCE_GUARD.json'),JSON.stringify({status:'PROPOSAL_UNMOUNTED',input:{path:nativePath,sha256:sha(input)},
  output:{path:'timesheet-correction-orchestrator.mjs',sha256:sha(Buffer.from(next))},hunks},null,2)+'\n');
console.log(JSON.stringify({status:'SOURCE_GENERATED',hunks:hunks.length,sha256:sha(Buffer.from(next))}));

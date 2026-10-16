import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const root=path.dirname(fileURLToPath(import.meta.url));
const read=p=>fs.readFileSync(path.join(root,p),'utf8');
test('complete source is pinned and uses no migration/public protocol or alternate owner',()=>{
 const m=JSON.parse(read('MOUNT_RECIPE.json'));
 assert.equal(m.qualification,'SOURCE_ONLY_NATIVE_NOT_RUN');assert.equal(m.physical.source,'c03c010dfabe8280fcbb36779fa210e7e5242a02');
 for(const p of m.payloads){assert.equal(crypto.createHash('sha256').update(read(p.path)).digest('hex'),p.sha256,p.path);assert.ok(scanManagedSql(read(p.path)).statements.length);}
 assert.equal(m.targets.length,2);
});
test('rotated target requires actual H103 and exact native binding while accepted member/receipt/component scope remains',()=>{
 const s=read('forward/025_claim_placement.sqlinc');
 assert.match(s,/m\.to_timesheet_id=t\.timesheet_id and m\.to_version=t\.version/);
 assert.match(s,/m\.to_timesheet_id is distinct from t\.timesheet_id\s+and private\.timesheet_contract_native_rotation_binding_v1\(to_jsonb\(p\),false\)/);
 assert.match(s,/private\.timesheet_contract_current_placement_v1\(t\.timesheet_id\)=jsonb_build_object/);
 for(const invariant of ['w.id=any(m.workflow_ids)','p_expense_component_id=any(m.expense_component_ids)',"o.placement_receipt_json->>'receipt_id'=p.placement_receipt_id::text",'p.current_contract_id is not distinct from t.contract_id','m.to_contract_id is not distinct from t.contract_id'])assert.ok(s.includes(invariant),invariant);
 assert.match(s,/if v_count>1 or exists/);assert.match(s,/new genuine workflow/i);
 assert.doesNotMatch(s,/max\s*\(\s*(?:version|.*version)|update\s+private\.timesheet_contract_move_members|insert into|service_role.*grant/i);
});
test('native claim lock keeps family-before-component ordering and exact owner recheck',()=>{
 const s=read('forward/028_claim_lock.sqlinc');
 assert.ok(s.indexOf('timesheet_contract_move_family_lock_v1')<s.indexOf('for update'));
 assert.match(s,/v_component\.workflow_id is distinct from v_initial_workflow_id/);
 assert.match(s,/v_component\.owning_timesheet_id is distinct from v_initial_owner_id/);
 assert.equal((s.match(/private\.timesheet_contract_claim_placement_v1\(/g)??[]).length,2);
 assert.match(s,/is distinct from v_timesheet_id/);assert.match(s,/errcode='40001'/);
 assert.doesNotMatch(s,/update\s+public\.|insert into|delete from|set_config|pg_advisory/i);
});
test('genuine native assertions preserve full business rows and do not fabricate approval/audit/provider acceptance',()=>{
 const s=read('fixtures/native-claim-rotation.sqlinc');
 assert.match(s,/GENUINE_NATIVE_CLAIM_ROTATION_INPUTS_REQUIRED/);
 assert.match(s,/before_rows is distinct from after_rows/);
 assert.match(s,/FOREIGN_COMPONENT/);assert.match(s,/INTERMEDIATE_AFTER_ROTATION/);
 assert.doesNotMatch(s,/insert into public\.|update public\.|delete from public\.|'SUCCESS'|set_config|clock_timestamp\(\)/i);
});

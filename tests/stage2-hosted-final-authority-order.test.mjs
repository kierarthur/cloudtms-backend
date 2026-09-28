import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {inventory,repoRoot} from '../scripts/cloudtms-db-release-lib.mjs';
import path from 'node:path';

// Stage 2 successor (v5.1) source-order regression. Hosted TEST re-runs ten changed
// candidate/expense/invoice closures in the Stage 2 UPGRADE. Their older pages of four
// routines must be superseded by the unchanged canonical authorities, and the one routine
// shared with Banking Pay Stage 2 must keep 0202/0209 as its single final authority.
const REASSERT='supabase/repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql';
const CANONICAL=[
 ['supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql','c0c355e4119ebf66e3d5e5bf56b57bb6c0b771dd275007f2440f4a47639be162'],
 ['supabase/repeatable/22092026_1226_client_initial_settings_baseline_v1.sql','273b0d5cfa4cacc4f1d60d3a4682315aabd1c58a7c08d2b42e0c164f34f6264d'],
 ['supabase/repeatable/24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql','96d33ee8880cf721e8dc45bf4d51e782e5bd30cb5423843b1174cb22f30712bf'],
];
const text=file=>readFileSync(path.join(repoRoot,file),'utf8').replace(/--[^\n]*/g,'');
const defines=(root,name)=>root.paths.some(p=>new RegExp(`create\\s+or\\s+replace\\s+function\\s+${name.replace('.','\\.')}\\s*\\(`,'i').test(text(p)));
const touchesAcl=(root,name)=>root.paths.some(p=>new RegExp(`\\b(?:grant|revoke)\\b[^;]*\\bon\\s+function\\s+${name.replace('.','\\.')}\\s*\\(`,'i').test(text(p)));

test('Stage 2 hosted final-authority reassertion replays only the three unchanged canonical files, in source order, last',()=>{
 const all=inventory().repeatables;
 const index=all.findIndex(x=>x.path===REASSERT);
 assert.ok(index>=0);
 const root=all[index];
 assert.deepEqual(root.paths,[REASSERT,...CANONICAL.map(([file])=>file)]);
 for(const [file,hash] of CANONICAL)assert.equal(all.find(x=>x.path===file).sha256,hash,file);
 const dated=all.filter(x=>/\/\d{8}_\d{4}_/.test(x.path));
 assert.equal(dated.at(-1).path,REASSERT,'reassertion must be the last dated repeatable');
});

test('Each reasserted routine has the reassertion closure as its last definer',()=>{
 const all=inventory().repeatables;
 for(const name of ['public.candidate_app_timesheet_page_v1','public.client_create_with_settings_v1',
  'public.weekly_source_message_dispatch_claim_v1','public.weekly_source_message_dispatch_submission_start_atomic_v1']){
  const definers=all.filter(root=>defines(root,name));
  assert.equal(definers.at(-1).path,REASSERT,name);
 }
});

test('The shared duplicate-expense review keeps 0202 definition and 0209 access list as single final authority',()=>{
 const all=inventory().repeatables;
 const name='private._timesheet_duplicate_expense_review_v1';
 const definers=all.filter(root=>defines(root,name)).map(root=>root.path);
 const aclRoots=all.filter(root=>touchesAcl(root,name)).map(root=>root.path);
 assert.equal(definers.at(-1),'supabase/repeatable/26092026_0202_banking_pay_stage2_source_authorisation_v1.sql');
 assert.equal(aclRoots.at(-1),'supabase/repeatable/26092026_0209_banking_pay_stage2_grants_v1.sql');
 assert.ok(!definers.includes(REASSERT)&&!aclRoots.includes(REASSERT));
 const i0202=all.findIndex(x=>x.path===definers.at(-1));
 for(const older of definers.slice(0,-1))assert.ok(all.findIndex(x=>x.path===older)<i0202,older);
});

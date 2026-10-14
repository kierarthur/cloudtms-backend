import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.join(path.dirname(fileURLToPath(import.meta.url)),'issue-support-construction'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);
const input='before/01_private__invoice_issue_validate_batch.sql.txt',output='14_complete_native_issue_validator_with_agency_requirements.sqlinc',reverse='ISSUE_VALIDATOR_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,output))){assert.ok(process.argv.includes('--refresh-unfrozen'));assert.equal(sha(fs.readFileSync(path.join(dir,output))),JSON.parse(fs.readFileSync(path.join(dir,reverse))).forward_sha256);}
const before=fs.readFileSync(path.join(dir,input),'utf8'),eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol);let forward=before;const hunks=[];
function replace(a,b){a=native(a);b=native(b);assert.equal(forward.split(a).length,2,a.slice(0,100));forward=forward.replace(a,()=>b);hunks.push({before:a,after:b});}
replace('request_counts as materialized (',`agency_issue_requirements as materialized (
  select s.invoice_id,private.weekly_source_agency_issue_requirements_v1(s.invoice_id) codes
  from invoice_scope s
),
request_counts as materialized (`);
replace('    ],null)::text[]) blockers\n  from facts f',`    ],null)::text[])||coalesce(agency.codes,array[]::text[]) blockers
  from facts f
  left join agency_issue_requirements agency on agency.invoice_id=f.invoice_id`);
replace("  case when c.deliver\n    then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,", "  case when c.deliver\n    then coalesce(c.route_blockers,'[]'::jsonb)||to_jsonb(coalesce((select a.codes from agency_issue_requirements a where a.invoice_id=c.invoice_id),array[]::text[])) else '[]'::jsonb end,");
replace("    'delivery_blockers',case when c.deliver\n      then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,", "    'delivery_blockers',case when c.deliver\n      then coalesce(c.route_blockers,'[]'::jsonb)||to_jsonb(coalesce((select a.codes from agency_issue_requirements a where a.invoice_id=c.invoice_id),array[]::text[])) else '[]'::jsonb end,");
// Eligibility already consults c.blockers, so the new three codes naturally
// hold both issue-only and issue-and-deliver; no independent send predicate.
let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
fs.writeFileSync(path.join(dir,output),forward);fs.writeFileSync(path.join(dir,reverse),JSON.stringify({source_only:true,native:false,input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks,complete_reverse:true,independent_review_pending:true,mount_ready:false},null,2)+'\n');console.log(JSON.stringify({source_only:true,native:false,forward_sha256:sha(forward),hunks:hunks.length}));

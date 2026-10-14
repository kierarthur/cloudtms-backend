import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const out=path.join(here,'source-selector-precision-construction');fs.mkdirSync(out,{recursive:true});
const input='docs/healthroster-build/proposals/agency-source-dependency-current-reader-forward-20261014/construction/01_native_source_dependency_witness.sqlinc';
const raw=fs.readFileSync(path.join(repo,input),'utf8'),sha=s=>crypto.createHash('sha256').update(s).digest('hex');
assert.equal(sha(raw),'0f475d7896b7fffe553fdc5c164d14a6cfef680609c01cb3857d69f2c4cdb155');
const before=' -- Group/client and date predicates are retained as typed native selectors,';
const selectors=` -- Predicate-shaped phantom scopes come from the SAME real facts read.
 -- Every Source row/publication upload match is retained for every actually
 -- selected Client, including slots with no currently visible resolution.
 -- An existing report-bound resolution is a separate exact object dependency.
 -- No parent-only upload/revision/event/Client scope substitutes for these
 -- conjunctions, and no LIMIT/sample/truncation can claim a complete footprint.
 with actual_rows as (
  select (row_data->>'id')::uuid upload_row_id,(row_data->>'upload_id')::uuid upload_id
   from jsonb_array_elements(facts->'rows') row_data
 ), actual_publications as (
  select (publication->>'upload_id')::uuid upload_id,
   coalesce(publication->>'projection_generation',publication->>'authority_scope_version')::integer generation
   from jsonb_array_elements(facts->'publications') publication
 ), actual_slots as (
  select distinct row_data.upload_row_id,publication.generation,client_id
   from actual_rows row_data join actual_publications publication on publication.upload_id=row_data.upload_id
   cross join unnest(clients) client_id
 )
 select coalesce(jsonb_agg(jsonb_build_array(upload_row_id,generation,client_id)
  order by upload_row_id,generation,client_id),'[]'::jsonb) into resolution_slots from actual_slots;
 select coalesce(jsonb_agg(jsonb_build_array(revision_id,client_id) order by revision_id,client_id),'[]'::jsonb)
  into revision_client_pairs from(
   select distinct (revision->>'id')::uuid revision_id,client_id
    from jsonb_array_elements(facts->'revisions') revision cross join unnest(clients) client_id
  ) native;
 select coalesce(jsonb_agg(jsonb_build_array(revision_id,event_id) order by revision_id,event_id),'[]'::jsonb)
  into revision_event_pairs from(
   select distinct (revision->>'id')::uuid revision_id,(event->>'id')::uuid event_id
    from jsonb_array_elements(facts->'revisions') revision cross join jsonb_array_elements(facts->'events') event
  ) native;
 select coalesce(jsonb_agg(to_jsonb(resolution_id) order by resolution_id),'[]'::jsonb)
  into exact_report_bound_resolutions from(
   select distinct (binding->>'bound_row_resolution_id')::uuid resolution_id
    from jsonb_array_elements(facts->'report_bindings') binding
    where binding->>'bound_row_resolution_id' is not null
  ) native;
`+before;
const hunks=[
 {label:'local complete native selector projection fields',before:' billing_movements jsonb; rule_set_ids jsonb;',after:' billing_movements jsonb; rule_set_ids jsonb; resolution_slots jsonb; revision_client_pairs jsonb;\n revision_event_pairs jsonb; exact_report_bound_resolutions jsonb;'},
 {label:'full real Source row/publication/client and revision conjunctions',before,after:selectors},
 {label:'closed literal tuple fields for the SAME arbiter',before:"   'contract_coverage',jsonb_build_object('client_ids',to_jsonb(clients),",after:"   'resolution_slots',resolution_slots,\n   'revision_client_pairs',revision_client_pairs,'revision_event_pairs',revision_event_pairs,\n   'exact_report_bound_resolution_ids',exact_report_bound_resolutions,\n   'contract_coverage',jsonb_build_object('client_ids',to_jsonb(clients),"}
];
let forward=raw;for(const h of hunks){assert.equal(forward.split(h.before).length,2,h.label);forward=forward.replace(h.before,()=>h.after);}
let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2,h.label);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,raw);
fs.writeFileSync(path.join(out,'85_complete_source_dependency_witness_with_exact_selector_tuples.sqlinc'),forward);
fs.writeFileSync(path.join(out,'01_source20_witness_before.sql.txt'),raw);
fs.writeFileSync(path.join(out,'SOURCE_SELECTOR_COMPLETE_REVERSE.json'),JSON.stringify({source_only:true,native:false,input_path:input,
 input_commit:'63965738af6f148a75cd1bea5052600e4b007da5',input_sha256:sha(raw),forward_sha256:sha(forward),hunks},null,2)+'\n');
console.log(JSON.stringify({native:false,full_owner:true,unique_reverse_hunks:3,forward_sha256:sha(forward)}));

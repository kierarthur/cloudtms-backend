import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(here,'../../../..');
const sha=raw=>crypto.createHash('sha256').update(raw).digest('hex');
const pins=JSON.parse(fs.readFileSync(path.join(here,'NATIVE_INVOICE_OWNER_PINS.json')));
assert.deepEqual([pins.roots,pins.uniqueInputs,pins.occurrences],[1484,1745,2160]);
assert.equal(pins.SQL_executed,false);assert.equal(pins.admission_and_release_are_writer_reference_only,true);
for(const pin of [...pins.selected,...pins.readGraph])assert.equal(sha(fs.readFileSync(path.join(here,pin.saved_file))),pin.definition_sha256,pin.id);
assert.equal(pins.readGraph.length,49);
for(const pin of pins.readGraph){assert.equal(pin.direct_row_lock,false,pin.id);assert.deepEqual(pin.conservative_write_or_lock_tokens,[],pin.id);}
const sqlPath='supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_source_invoice_hold_facts.sqlinc';
const sql=fs.readFileSync(path.join(root,sqlPath),'utf8');
assert.equal(scanManagedSql(sql).statements.length,6);
const declarations=functionEvents(sql).events.filter(x=>x.definition);
assert.equal(declarations.length,2);
for(const d of declarations){
  assert.ok(d.name.startsWith('private.weekly_source_checklist_source_invoice_'));
  const tokens=sqlTokens(d.body).filter(t=>t.kind==='word').map(t=>t.value);
  assert.deepEqual(tokens.filter(t=>['insert','update','delete','truncate','create','lock','notify','execute','call'].includes(t)),[]);
  assert.doesNotMatch(d.body,/\bfor\s+(?:update|share|key\s+share|no\s+key\s+update)\b/i);
}
assert.match(sql,/amounts:=private\.weekly_source_invoice_line_amounts_v1\(resolution\.id,revision\.id\)/);
assert.match(sql,/if certificate->'known' is distinct from 'true'::jsonb then continue; end if;/);
assert.match(fs.readFileSync(path.join(here,'CHECKPOINT.md'),'utf8'),/unresolved construction defect/);
const files=[];
function visit(dir){for(const entry of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,entry.name);if(entry.isDirectory())visit(p);else if(entry.name!=='CHECKPOINT_MANIFEST.json')files.push(p);}}
visit(here);files.push(path.join(root,sqlPath));files.sort();
const manifest={status:'UNFINISHED_SOURCE_CHECKPOINT_NOT_READY_TO_INTEGRATE',
  source_commit:pins.sourceCommit,census_sha256:pins.censusSha256,
  checks:{saved_native_pins:true,selected_owners:5,read_graph_bodies:49,direct_read_graph_token_flags:0,private_leaf_declarations:2,lexical_statements:6},
  known_unresolved_catalogue_suppression:true,public_producer_consumes_leaf:false,DTO_consumes_leaf:false,
  SQL_executed:false,compiled:false,native_qualified:false,
  artifacts:files.map(file=>{const raw=fs.readFileSync(file);return {file:path.relative(root,file).replaceAll('\\','/'),bytes:raw.length,sha256:sha(raw)};})};
const output=JSON.stringify(manifest,null,2)+'\n';
const destination=path.join(here,'CHECKPOINT_MANIFEST.json');
if(process.argv.includes('--write'))fs.writeFileSync(destination,output);else assert.equal(fs.readFileSync(destination,'utf8'),output);
console.log(JSON.stringify({...manifest,artifacts:manifest.artifacts.length}));

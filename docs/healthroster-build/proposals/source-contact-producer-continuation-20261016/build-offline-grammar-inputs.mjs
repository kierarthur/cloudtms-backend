import {readFileSync,writeFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),sha=value=>createHash('sha256').update(value).digest('hex');
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const driver=path.join(root,'.codex-tmp/contact-03b-grammar/inputs.json'),original=JSON.parse(readFileSync(driver,'utf8'));
const pin=file=>({file,sha256:sha(readFileSync(file))});
const inputs=['one-time','forward','tests'].flatMap(directory=>readdirSync(path.join(base,directory)).filter(file=>file.endsWith('.sqlinc')).sort()
 .map(file=>({source:directory+'/'+file,...pin(path.join(base,directory,file)),kind:directory==='tests'?'FIXTURE_NOT_MOUNTED':'PROPOSAL_NATIVE_SOURCE'})));
writeFileSync(path.join(base,'OFFLINE_GRAMMAR_INPUTS.json'),JSON.stringify({version:'CONTACT_SOURCE_OFFLINE_PG17_INPUTS_V1',
 source_base_commit:'03b444e0ead2f70ab193101dbf8f151e64efccaf',source_status:'EXACT_AUTHORED_SOURCE_BYTES_NOT_NATIVE_QUALIFICATION',
 parserPath:original.parserPath,root_schema_reference_manifest:pin(driver),contract:pin(original.contractFile),
 schemaInputs:original.schemaInputs.map(pin),inputs,
 limitations:['Offline grammar and declared row fields only','ROWTYPE is replaced with record for PLpgSQL parser only',
 'No PostgreSQL object resolution, execution, ACL, concurrency or first-use proof','No DB/network/install operation']},null,2)+'\n');
console.log(JSON.stringify({leaves:inputs.length,schema_inputs:original.schemaInputs.length,source_only:true}));

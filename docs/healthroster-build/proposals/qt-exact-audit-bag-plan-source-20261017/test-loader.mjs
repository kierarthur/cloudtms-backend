// Offline public module loader. No environment, provider, source admission or DB.
import fs from 'node:fs';import vm from 'node:vm';import crypto from 'node:crypto';
import {SqlstateStderrCapture} from './parent/metadata-diagnostic.mjs';
import {diagnosticMarker} from './parent/metadata-progress-diagnostic.mjs';
export {diagnosticMarker};
export async function pure(){const context=vm.createContext({Buffer,TextDecoder,Date,structuredClone});const mod=new vm.SourceTextModule(fs.readFileSync(new URL('./payload/audit-bag-plan.mjs',import.meta.url),'utf8'),{context});
 const imports={'node:crypto':{createHash:crypto.createHash},'./metadata-diagnostic.mjs':{SqlstateStderrCapture},'./metadata-progress-diagnostic.mjs':{diagnosticMarker}};
 await mod.link(async spec=>{if(!imports[spec])throw Error('OFFLINE_IMPORT_REFUSED');const o=imports[spec],m=new vm.SyntheticModule(Object.keys(o),function(){for(const[k,v]of Object.entries(o))this.setExport(k,v);},{context});await m.link(()=>{throw Error('OFFLINE_NESTED_IMPORT_REFUSED')});await m.evaluate();return m;});await mod.evaluate();return mod.namespace;
}

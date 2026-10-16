// Source-authoring only: refresh the two owned samples after verifying every old byte.
// Operational users generate a fresh nonce/new directory with build.mjs instead.
import fs from'node:fs';import path from'node:path';import{here,hash,buildPacket,preflightPacket}from'./component.mjs';import{buildDiscoveryPackets}from'./discovery.mjs';
for(const mode of['session-context','protected-owner-request']){
 const dir=path.join(here,'packets',mode),old=JSON.parse(fs.readFileSync(path.join(dir,'INPUT_RECEIPT.json')));
 for(const f of old.files){const b=fs.readFileSync(path.join(dir,f.path));if(b.length!==f.bytes||hash(b)!==f.sha256)throw Error('OWNED_SAMPLE_CHANGED:'+f.path);}
 const packet=buildPacket({mode,marker:old.marker}),d=buildDiscoveryPackets(),p=preflightPacket();
 const values={'REQUEST.json':packet.request,'EXPECTED.json':packet.expected,'PREFLIGHT_REQUEST.json':p,'DISCOVERY_PREFLIGHT_REQUEST.json':d.preflight,'DISCOVERY_REQUEST.json':d.discovery,'DISCOVERY_EXPECTED.json':d.expected};
 const sql={'COMPONENT.sql':packet.request.sql,'FRESH_VERIFY.sql':packet.request.verification_sql,'READINESS_PREFLIGHT.sql':p.sql,'DISCOVERY_PREFLIGHT.sql':d.preflight.sql,'DISCOVERY.sql':d.discovery.sql};
 for(const[n,v]of Object.entries(values))fs.writeFileSync(path.join(dir,n),JSON.stringify(v,null,2)+'\n');
 for(const[n,v]of Object.entries(sql))fs.writeFileSync(path.join(dir,n),v);
 const files=[...Object.keys(values),...Object.keys(sql)].sort().map(p=>{const b=fs.readFileSync(path.join(dir,p));return{path:p,bytes:b.length,sha256:hash(b)};});
 fs.writeFileSync(path.join(dir,'INPUT_RECEIPT.json'),JSON.stringify({version:'CONTACT_CLOSED_COMPONENT_BUILT_INPUT_RECEIPT_V1',...packet.expected,files,sourceOnlyBuilt:true,native:'NOT_RUN',caller:'ROOT_ONLY_KIER_ROLLBACK_TOOL',locks:'ROOT_OWNS_EXISTING_SHARED_PROTOCOL_NOT_ACQUIRED_BY_BUILDER'},null,2)+'\n');
 console.log(JSON.stringify({mode,statementCount:packet.expected.statementCount,sqlBytes:Buffer.byteLength(packet.request.sql),sqlSha256:packet.expected.sqlSha256}));
}

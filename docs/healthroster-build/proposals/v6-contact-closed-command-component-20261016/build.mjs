// Generates reviewable Kier rollback requests; never submits one or opens a DB.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{buildPacket,preflightPacket,hash}from'./component.mjs';import{buildDiscoveryPackets}from'./discovery.mjs';
export function writePacketDirectory(output,mode='session-context'){
 if(!path.isAbsolute(output)||fs.existsSync(output))throw Error('CONTACT_COMPONENT_NEW_ABSOLUTE_OUTPUT_REQUIRED');
 const packet=buildPacket({mode}),discovery=buildDiscoveryPackets(),preflight=preflightPacket();fs.mkdirSync(output,{recursive:true});
 for(const[name,value]of Object.entries({'REQUEST.json':packet.request,'EXPECTED.json':packet.expected,'PREFLIGHT_REQUEST.json':preflight,'DISCOVERY_PREFLIGHT_REQUEST.json':discovery.preflight,'DISCOVERY_REQUEST.json':discovery.discovery,'DISCOVERY_EXPECTED.json':discovery.expected}))fs.writeFileSync(path.join(output,name),JSON.stringify(value,null,2)+'\n');
 fs.writeFileSync(path.join(output,'COMPONENT.sql'),packet.request.sql);fs.writeFileSync(path.join(output,'FRESH_VERIFY.sql'),packet.request.verification_sql);
 fs.writeFileSync(path.join(output,'READINESS_PREFLIGHT.sql'),preflight.sql);fs.writeFileSync(path.join(output,'DISCOVERY_PREFLIGHT.sql'),discovery.preflight.sql);fs.writeFileSync(path.join(output,'DISCOVERY.sql'),discovery.discovery.sql);
 const files=fs.readdirSync(output).sort().map(p=>{const b=fs.readFileSync(path.join(output,p));return{path:p,bytes:b.length,sha256:hash(b)};});fs.writeFileSync(path.join(output,'INPUT_RECEIPT.json'),JSON.stringify({version:'CONTACT_CLOSED_COMPONENT_BUILT_INPUT_RECEIPT_V1',...packet.expected,files,sourceOnlyBuilt:true,native:'NOT_RUN',caller:'ROOT_ONLY_KIER_ROLLBACK_TOOL',locks:'ROOT_OWNS_EXISTING_SHARED_PROTOCOL_NOT_ACQUIRED_BY_BUILDER'},null,2)+'\n');return packet.expected;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const args=process.argv.slice(2);if(args.length!==4||args[0]!=='--mode'||args[2]!=='--out')throw Error('USAGE_BUILD_MODE_SESSION_CONTEXT_OR_PROTECTED_OWNER_REQUEST_OUT_NEW_ABSOLUTE_DIRECTORY');
 console.log(JSON.stringify(writePacketDirectory(args[3],args[1]),null,2));
}

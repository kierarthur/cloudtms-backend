import childProcess from'node:child_process';import{syncBuiltinESMExports}from'node:module';
const original=childProcess.spawn;childProcess.spawn=(cmd,...args)=>{if(/psql|docker|wrangler/i.test(String(cmd)))throw Error('PEER_NATIVE_PROCESS_FORBIDDEN');return original(cmd,...args);};syncBuiltinESMExports();globalThis.fetch=()=>{throw Error('PEER_NETWORK_FORBIDDEN');};

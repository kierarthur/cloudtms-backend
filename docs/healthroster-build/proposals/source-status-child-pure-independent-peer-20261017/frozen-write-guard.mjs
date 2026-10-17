// Offline replay only: preserve frozen outputs, refuse native socket/child calls.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import net from 'node:net';import tls from 'node:tls';import {syncBuiltinESMExports} from 'node:module';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),frozen=path.join(here,'frozen')+path.sep,write=fs.writeFileSync,spawn=cp.spawn;
fs.writeFileSync=function(file,data,...rest){const target=path.resolve(String(file));if(target.startsWith(frozen)){const bytes=Buffer.isBuffer(data)?data:Buffer.from(data);if(!fs.existsSync(target)||!fs.readFileSync(target).equals(bytes))throw Error('PEER_FROZEN_WRITE_DIFFERS');process.stderr.write('PEER_EQUAL_FROZEN_WRITE_SUPPRESSED '+path.basename(target)+'\n');return;}if(!target.startsWith(here+path.sep))throw Error('PEER_OUTSIDE_WRITE_REFUSED');return write.call(this,file,data,...rest);};
cp.spawn=function(file,...args){if(!/^(node|git)(?:\.exe)?$/i.test(path.basename(String(file))))throw Error('PEER_NATIVE_CHILD_REFUSED');return spawn.call(this,file,...args);};
net.connect=net.createConnection=tls.connect=()=>{throw Error('PEER_NATIVE_SOCKET_REFUSED');};
syncBuiltinESMExports();

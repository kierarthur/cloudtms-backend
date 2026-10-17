import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import net from 'node:net';import tls from 'node:tls';import {syncBuiltinESMExports} from 'node:module';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),write=fs.writeFileSync;
fs.writeFileSync=function(file,data,...rest){const target=path.resolve(String(file));if(!target.startsWith(here+path.sep)||target.includes(path.sep+'frozen'+path.sep))throw Error('PEER_FROZEN_OR_OUTSIDE_WRITE_REFUSED');return write.call(this,file,data,...rest);};
const spawn=cp.spawn;cp.spawn=function(exe,...rest){if(!/^(node|git)(?:\.exe)?$/i.test(path.basename(String(exe))))throw Error('PEER_NATIVE_CHILD_REFUSED');return spawn.call(this,exe,...rest);};
net.connect=net.createConnection=tls.connect=()=>{throw Error('PEER_NATIVE_SOCKET_REFUSED');};syncBuiltinESMExports();

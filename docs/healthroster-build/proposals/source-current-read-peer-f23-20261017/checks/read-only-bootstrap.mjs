// Author test replay: preserve every frozen byte; redirect only test receipts.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {syncBuiltinESMExports} from 'node:module';
const own=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const report=path.resolve(own,'../../../..'),money=path.resolve(report,'../healthroster-v2-money-build-13102026');
const packetPrefix=path.join(money,'docs/healthroster-build/proposals')+path.sep;
const allowed=new Set(['source-current-read-health-successor-20261017',
  'source-complete-classification-current-successor-20261017','source-read-identity-portability-forward-20261017']);
const write=fs.writeFileSync.bind(fs);
fs.writeFileSync=(file,data,...args)=>{
  const absolute=path.resolve(String(file));
  if(absolute.startsWith(own+path.sep))return write(file,data,...args);
  if(absolute.startsWith(packetPrefix)){
    const relative=path.relative(packetPrefix,absolute),parts=relative.split(path.sep);
    if(parts.length===2&&allowed.has(parts[0])&&/_RESULTS[.]json$/.test(parts[1])){
      const destination=path.join(own,'replayed',relative);
      fs.mkdirSync(path.dirname(destination),{recursive:true});
      return write(destination,data,...args);
    }
  }
  throw Error('INDEPENDENT_REPLAY_WRITE_REFUSED:'+absolute);
};
syncBuiltinESMExports();
globalThis.fetch=()=>{throw Error('INDEPENDENT_REPLAY_NETWORK_REFUSED');};

import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),target=path.join(dir,'.merge-inputs/test-mount/scripts');fs.mkdirSync(target,{recursive:true});
for(const name of fs.readdirSync(path.join(dir,'forward/scripts')))if(name.endsWith('.mjs'))fs.copyFileSync(path.join(dir,'forward/scripts',name),path.join(target,name));
console.log('UNIT_ONLY collector source modules mounted from own forward; native seal remains null.');

// Author-location adapter only. Production tests import mounted relative owners.
import fs from 'node:fs';import path from 'node:path';import{pathToFileURL}from'node:url';
const H=path.dirname(new URL(import.meta.url).pathname).replace(/^\/([A-Z]:)/,'$1'),R='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
let source=fs.readFileSync(H+'/payload/scripts/healthroster-install2-native-coordinator.mjs','utf8');
source=source.replaceAll(/from '(\.\/[A-Za-z0-9_.-]+)'/g,(_,f)=>'from '+JSON.stringify(pathToFileURL(R+'/scripts/'+f.slice(2)).href));
const rootLine="const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');";
if(source.split(rootLine).length!==2)throw Error('ONE_AUTHOR_LOCATION_ROOT_REQUIRED');source=source.replace(rootLine,'const root='+JSON.stringify(R)+';');
const main="if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))await runNativeInstall2();";
if(source.split(main).length!==2)throw Error('ONE_NATIVE_MAIN_REQUIRED');source=source.replace(main,'');
const moduleUrl='data:text/javascript;base64,'+Buffer.from(source).toString('base64');
let cases=fs.readFileSync(H+'/coordinator.test.source.mjs','utf8').replace("'../scripts/healthroster-install2-native-coordinator.mjs'",JSON.stringify(moduleUrl)).replace("'../scripts/cloudtms-managed-release-sql.mjs'",JSON.stringify(pathToFileURL(R+'/scripts/cloudtms-managed-release-sql.mjs').href));
cases=cases.replace("const read=file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');",`const read=file=>fs.readFileSync(file.startsWith('scripts/healthroster-install2-')?${JSON.stringify(H+'/payload/')}+file:${JSON.stringify(R+'/')}+file,'utf8');`);
await import('data:text/javascript;base64,'+Buffer.from(cases).toString('base64'));

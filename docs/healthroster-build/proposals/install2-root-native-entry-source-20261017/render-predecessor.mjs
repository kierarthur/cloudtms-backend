// Source-only renderer: fixed metadata SQL text, never connects or executes.
import fs from 'node:fs';import path from 'node:path';import{fileURLToPath,pathToFileURL}from'node:url';
const H=path.dirname(fileURLToPath(import.meta.url)),R='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
let source=fs.readFileSync(H+'/payload/scripts/healthroster-install2-native-coordinator.mjs','utf8');
source=source.replaceAll(/from '(\.\/[A-Za-z0-9_.-]+)'/g,(_,f)=>'from '+JSON.stringify(pathToFileURL(R+'/scripts/'+f.slice(2)).href));
const rootLine="const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');",main="if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))await runNativeInstall2();";
if(source.split(rootLine).length!==2||source.split(main).length!==2)throw Error('EXACT_SOURCE_LOCATION_ADAPTATION_REQUIRED');
source=source.replace(rootLine,'const root='+JSON.stringify(R)+';').replace(main,'');
const m=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
process.stdout.write(m.install2PredecessorProbeSql('hr_install2_'+'a'.repeat(32)));

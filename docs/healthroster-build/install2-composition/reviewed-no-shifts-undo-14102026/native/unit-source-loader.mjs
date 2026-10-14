import {registerHooks,createRequire} from 'node:module';
import path from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../../../../healthroster-install2-14102026');
if(!root.endsWith('healthroster-install2-14102026'))throw new Error('EXACT_AUTHORIZED_SOURCE_ROOT_REQUIRED');
const actual=createRequire(path.join(path.resolve(root),'package.json'));
const xlsxUrl=pathToFileURL(actual.resolve('xlsx')).href;
globalThis.fetch=()=>{throw new Error('UNIT_ONLY_NO_NETWORK_ALLOWED');};
// Actual installed dependency from root's source-only npm ci; no installation,
// fake xlsx implementation, application request or environment/secret read.
registerHooks({resolve(specifier,context,nextResolve){
  if(specifier==='xlsx')return {url:xlsxUrl,shortCircuit:true};
  return nextResolve(specifier,context);
}});

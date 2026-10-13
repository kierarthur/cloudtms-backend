// Offline test runtime only. Resolve the unchanged installed native xlsx
// package from the read-only integrator; no install, network or config change.
import { registerHooks } from 'node:module';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
const lane=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../../..');
const nativeRequire=createRequire(path.join(lane,'../healthroster-v2-backend-13102026/package.json'));
const packageDirectory=path.dirname(nativeRequire.resolve('xlsx'));
const packageMetadata=JSON.parse(fs.readFileSync(path.join(packageDirectory,'package.json'),'utf8'));
const xlsxUrl=pathToFileURL(path.join(packageDirectory,packageMetadata.exports['.'].import)).href;
registerHooks({ resolve(specifier,context,nextResolve) {
  if(specifier==='xlsx') return nextResolve(xlsxUrl,context);
  return nextResolve(specifier,context);
} });

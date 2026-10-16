// Source-only test overlay: unchanged dependency imports resolve to pinned Root.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),forward=path.join(base,'forward');
const sourceRoot=path.resolve(base,'../../../../../healthroster-install2-14102026');
export async function resolve(specifier,context,nextResolve){
 if(specifier.startsWith('.')&&context.parentURL?.startsWith(pathToFileURL(forward+path.sep).href)){
  const candidate=fileURLToPath(new URL(specifier,context.parentURL));
  if(!fs.existsSync(candidate)){
   const relative=path.relative(forward,candidate);if(!relative.startsWith('..')){
    const fallback=path.join(sourceRoot,relative);if(fs.existsSync(fallback))return nextResolve(pathToFileURL(fallback).href,context);
   }
  }
 }
 return nextResolve(specifier,context);
}

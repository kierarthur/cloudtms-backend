// Imports missing unchanged dependencies from the read-only integrator tree.
import fs from'node:fs';import path from'node:path';import{fileURLToPath,pathToFileURL}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),forward=path.join(dir,'forward'),sourceRoot=path.resolve(dir,'../../../../../healthroster-install2-14102026');
export async function resolve(specifier,context,nextResolve){
 if(specifier.startsWith('.')&&context.parentURL?.startsWith(pathToFileURL(forward+path.sep).href)){
  const candidate=fileURLToPath(new URL(specifier,context.parentURL));if(!fs.existsSync(candidate)){const relative=path.relative(forward,candidate);if(!relative.startsWith('..')){const fallback=path.join(sourceRoot,relative);if(fs.existsSync(fallback))return nextResolve(pathToFileURL(fallback).href,context);}}
 }return nextResolve(specifier,context);
}

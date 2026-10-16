// Source-only fixture dependency resolution. Relative code stays in the owned graph.
import path from'node:path';import{createRequire}from'node:module';import{pathToFileURL}from'node:url';
const root=process.env.HEALTHROSTER_PREBODY_TEST_DEPENDENCY_ROOT,require=createRequire(path.join(root,'package.json'));
export async function resolve(specifier,context,nextResolve){
 if(specifier==='xlsx'){const entry=require.resolve('xlsx');return{url:pathToFileURL(path.join(path.dirname(entry),'xlsx.mjs')).href,shortCircuit:true};}
 return nextResolve(specifier,context);
}

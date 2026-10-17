// Test-only exact loader substitution; never writes the I2 checkout.
import fs from'node:fs';import path from'node:path';import{registerHooks}from'node:module';import{fileURLToPath,pathToFileURL}from'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),i2=path.resolve(repo,'../healthroster-install2-14102026'),exact=pathToFileURL(path.join(i2,'docs/healthroster-build/proposals/source-current-read-health-successor-20261017/loader.mjs')).href;
registerHooks({load(url,context,next){if(url===exact)return{format:'module',source:fs.readFileSync(path.join(here,process.env.HEALTH_READ_PORTABILITY_VECTOR==='ORIGINAL'?'current/loader.mjs':'forward/loader.mjs'),'utf8'),shortCircuit:true};return next(url,context);}});

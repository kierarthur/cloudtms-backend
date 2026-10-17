// Only two public module locators change for the copied frozen test. Exact
// original163646 module bytes are already captured in the independent0459 peer.
import path from 'node:path';import {registerHooks} from 'node:module';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),test=pathToFileURL(here+'/reference/forward/batch-image.test.mjs').href,original=path.resolve(here,'../daily-health-genuine-staged-operator-independent-peer-20261017/reference/operator');
registerHooks({resolve(specifier,context,next){if(context.parentURL===test&&['../daily-health-genuine-staged-operator-20261017/contract.mjs','../daily-health-genuine-staged-operator-20261017/stages.mjs'].includes(specifier))return next(pathToFileURL(original+'/'+path.basename(specifier)).href,context);return next(specifier,context);}});

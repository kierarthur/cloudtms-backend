import {registerHooks} from 'node:module';
import {pathToFileURL} from 'node:url';
import path from 'node:path';
const target=pathToFileURL(path.resolve('broker/src/weekly-source/contract-preset-write.mjs')).href;
registerHooks({resolve(specifier,context,next){
 if(specifier==='./contract-preset-write.mjs'&&context.parentURL?.endsWith('/agency-prospective-contract-fixed-approval-native-forward-20261015/prospective-contract.test.mjs'))return{url:target,shortCircuit:true};
 return next(specifier,context);
}});

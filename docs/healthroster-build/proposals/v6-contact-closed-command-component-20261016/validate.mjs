import fs from'node:fs';import{validateNativeResult}from'./component.mjs';import{validateDiscoveryResult}from'./discovery.mjs';
const args=process.argv.slice(2);let validation;
if(args.length===4&&args[0]==='--expected'&&args[2]==='--result')validation=validateNativeResult(JSON.parse(fs.readFileSync(args[3])),JSON.parse(fs.readFileSync(args[1])));
else if(args.length===2&&args[0]==='--discovery-result')validation=validateDiscoveryResult(JSON.parse(fs.readFileSync(args[1])));
else throw Error('USAGE_VALIDATE_EXPECTED_FILE_RESULT_FILE_OR_DISCOVERY_RESULT_FILE');
console.log(JSON.stringify(validation,null,2));

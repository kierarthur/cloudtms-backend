// Explicitly synthetic nonDB stdio transport probe. Never launches the staged operator.
let buffer='';const timer=setTimeout(()=>process.exit(2),45000);
process.stdin.setEncoding('utf8');process.stdout.write(JSON.stringify({status:'CONTROLLED_STARTED'})+'\n');
process.stdin.on('data',text=>{buffer+=text;if(buffer.length>64)process.exit(3);let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end).replace(/\r$/,'');buffer=buffer.slice(end+1);if(line==='PROBE')setTimeout(()=>process.stdout.write(JSON.stringify({status:'CONTROLLED_READY'})+'\n'),250);else if(line==='QUIT'){clearTimeout(timer);process.exit(0);}else if(line==='HOLD'){process.stdout.write(JSON.stringify({status:'HOLD',code:'CONTROLLED_HOLD'})+'\n');clearTimeout(timer);process.exit(1);}else process.exit(4);}});
process.stdin.on('end',()=>{clearTimeout(timer);process.exit(1);});

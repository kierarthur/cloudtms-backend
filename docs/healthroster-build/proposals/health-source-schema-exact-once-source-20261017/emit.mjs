// Author-only pure compilation. No SQL execution, source admission or database.
import fs from'node:fs';import path from'node:path';
import {load,model,item,context,here}from'./test.mjs';
const{api}=await load();fs.mkdirSync(path.join(here,'generated'),{recursive:true});
for(const file of model.consumers)fs.writeFileSync(path.join(here,'generated',path.basename(file)),api.unitCheckpointSql(api.compileManagedUnit(item(file),'migration',{root:'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'}),context));

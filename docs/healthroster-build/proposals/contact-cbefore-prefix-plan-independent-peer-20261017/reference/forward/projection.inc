export function projectEstimatedPlan(value){
 check(Array.isArray(value)&&value.length===1&&value[0]&&Object.keys(value[0]).join('|')==='Plan','LOCAL_METADATA_PLAN_ENVELOPE_REFUSED');
 const nodes=[];let ordinal=0;
 const visit=(node,parent=null)=>{check(node&&typeof node==='object'&&!Array.isArray(node)&&nodeTypes.has(node['Node Type']),'LOCAL_METADATA_PLAN_NODE_REFUSED');check(!Object.keys(node).some(k=>/^Actual |^Execution Time$|^Triggers$/.test(k)),'LOCAL_METADATA_ANALYZE_DATA_REFUSED');
  const id=++ordinal;check(id<=16384,'LOCAL_METADATA_PLAN_NODE_BOUND');const row={ordinal:id,parent,nodeType:node['Node Type']},relations=new Set(),ctes=new Set();
  for(const [key,out]of[['Startup Cost','startupCost'],['Total Cost','totalCost'],['Plan Rows','estimatedRows'],['Plan Width','estimatedWidth']]){const n=node[key];check(typeof n==='number'&&Number.isFinite(n)&&n>=0,'LOCAL_METADATA_PLAN_NUMERIC_REFUSED');row[out]=n;}
  if(node.Schema!==undefined)check(typeof node.Schema==='string'&&scopeSchemas.has(node.Schema),'LOCAL_METADATA_PLAN_SOURCE_SCHEMA_REFUSED');
  if(node['Relation Name']!==undefined){const name=node['Relation Name'],scope=scopeRelations.get(name);check(typeof name==='string'&&scope&&(!node.Schema||scope.schemas.includes(node.Schema)),'LOCAL_METADATA_PLAN_SOURCE_RELATION_REFUSED');row.relationOrdinal=scope.ordinal;relations.add(scope.ordinal);}
  const addCte=name=>{const scope=scopeCtes.get(name);check(typeof name==='string'&&scope,'LOCAL_METADATA_PLAN_SOURCE_CTE_REFUSED');ctes.add(scope.ordinal);return scope.ordinal;};
  if(node['CTE Name']!==undefined)row.cteOrdinal=addCte(node['CTE Name']);
  if(typeof node['Subplan Name']==='string'&&node['Subplan Name'].startsWith('CTE ')){const match=node['Subplan Name'].match(/^CTE ([a-z_][a-z0-9_]*)$/);check(match,'LOCAL_METADATA_PLAN_SOURCE_CTE_REFUSED');row.cteProducerOrdinal=addCte(match[1]);}
  nodes.push(row);check(node.Plans===undefined||Array.isArray(node.Plans),'LOCAL_METADATA_PLAN_CHILDREN_REFUSED');for(const child of node.Plans??[]){const scope=visit(child,id);for(const r of scope.relations)relations.add(r);for(const c of scope.ctes)ctes.add(c);}
  row.sourceRelationOrdinals=[...relations].sort((a,b)=>a-b);row.sourceCteOrdinals=[...ctes].sort((a,b)=>a-b);return{relations,ctes};};visit(value[0].Plan);
 const top=nodes.slice().sort((a,b)=>b.totalCost-a.totalCost||a.ordinal-b.ordinal).slice(0,32);
 return{version:'CONTACT_CBEFORE_NO_ANALYZE_ESTIMATED_PLAN_V2',sourceScopeMapSha256:scopeMapSha256,nodeCount:nodes.length,topEstimatedCostNodes:top,actualRowsOrTimings:false,rawPlanPersisted:false};
}


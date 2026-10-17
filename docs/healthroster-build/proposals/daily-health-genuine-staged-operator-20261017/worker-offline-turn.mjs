// Test-only explicitly synthetic import target. Production worker never references this path.
let synthetic;
export function setSyntheticTurn(value){synthetic=value;}
export async function connectTurnControl(){if(!synthetic)throw Error('SYNTHETIC_CONTROL_REQUIRED');return synthetic;}

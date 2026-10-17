// Test-only namespace. Never loaded by a payload module outside the synthetic VM.
let turn;
export const setSyntheticTurn=value=>{turn=value;};
export const connectTurnControl=async()=>turn;
export const assertTurnIdentity=value=>value;

// test-election.js
const { startElection, amILeader, getCurrentLeader } = require("./coordinator/leader");

async function test() {
    console.log("Starting election test...");
    await startElection();
    
    setInterval(() => {
        console.log(`\n--- Status at ${new Date().toISOString()} ---`);
        console.log(`Am I leader? ${amILeader()}`);
        console.log(`Current leader: ${getCurrentLeader()}`);
    }, 5000);
}

test();

// To test, run this script in multiple terminals:
// Terminal 1: node test-election.js
// Terminal 2: node test-election.js
// Terminal 3: node test-election.js
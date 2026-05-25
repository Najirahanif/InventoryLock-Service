const client = require("./zk");
const zookeeper = require("node-zookeeper-client");

const ELECTION_PATH = "/election_v2";

let isLeader = false;
let myNodePath = null;
let currentLeader = null;

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

async function ensurePath() {
    return new Promise((resolve, reject) => {
        client.exists(ELECTION_PATH, (err, stat) => {
            if (err) return reject(false);
            
            if (stat) return resolve(true);
            
            client.create(ELECTION_PATH, (err) => {
                if (err && err.getCode() !== zookeeper.Exception.NODE_EXISTS) {
                    return reject(false);
                }
                resolve(true);
            });
        });
    });
}

async function startElection() {
    // Wait until znode is ready
    let ready = false;
    while (!ready) {
        try {
            ready = await ensurePath();
            if (!ready) {
                console.log("Waiting for /election_v2...");
                await sleep(500);
            }
        } catch (err) {
            console.log("Error ensuring path, retrying...");
            await sleep(500);
        }
    }

    // Create sequential ephemeral node
    client.create(
        `${ELECTION_PATH}/node-`,
        Buffer.from(process.pid.toString()), // Store PID as data
        zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
        (err, path) => {
            if (err) {
                console.error("Failed to create election node:", err);
                setTimeout(startElection, 5000);
                return;
            }

            myNodePath = path;
            console.log(`Created node: ${myNodePath}`);
            checkLeaderAndWatch();
        }
    );
}

function checkLeaderAndWatch() {
    // Get all children and set a watch
    client.getChildren(ELECTION_PATH, (err, children, stat) => {
        if (err) {
            console.error("Failed to get children:", err);
            setTimeout(checkLeaderAndWatch, 1000);
            return;
        }

        if (!children || children.length === 0) {
            console.log("No nodes yet, retrying...");
            setTimeout(checkLeaderAndWatch, 500);
            return;
        }

        // Sort children (they're sequential so lexicographic order works)
        children.sort();
        
        const leaderNode = children[0];
        const isThisNodeLeader = myNodePath.endsWith(leaderNode);
        
        // Only log when leadership changes
        if (isLeader !== isThisNodeLeader) {
            isLeader = isThisNodeLeader;
            
            if (isLeader) {
                console.log(`\n🔥 BECAME LEADER at ${new Date().toISOString()}`);
                console.log(`   Path: ${myNodePath}`);
                // Trigger any leader-specific initialization here
                onBecomeLeader();
            } else {
                console.log(`\n⏳ BECAME FOLLOWER at ${new Date().toISOString()}`);
                console.log(`   Path: ${myNodePath}`);
                console.log(`   Leader is: ${leaderNode}`);
                onLoseLeadership();
            }
        }
        
        currentLeader = leaderNode;
        
        // IMPORTANT: Watch the node that's immediately before us
        // This avoids "herd effect" (all nodes watching leader)
        const myIndex = children.findIndex(child => myNodePath.endsWith(child));
        
        if (myIndex > 0) {
            // Watch the node before me, not the leader directly
            const nodeToWatch = `${ELECTION_PATH}/${children[myIndex - 1]}`;
            console.log(`👀 Watching previous node: ${nodeToWatch}`);
            
            client.exists(nodeToWatch, (err, exists) => {
                if (err) {
                    console.error("Error setting watch:", err);
                    return;
                }
                
                if (!exists) {
                    // Node disappeared, re-check leadership immediately
                    console.log("Previous node disappeared, re-electing...");
                    checkLeaderAndWatch();
                }
            });
        } else {
            // I am the leader, watch my own node (to detect if I lose leadership)
            client.exists(myNodePath, (err, exists) => {
                if (err || !exists) {
                    console.log("Lost my leader node, re-electing...");
                    setTimeout(checkLeaderAndWatch, 100);
                }
            });
        }
    });
}

// Optional: Add watch for root path changes
function watchElectionPath() {
    client.getChildren(ELECTION_PATH, (err, children) => {
        if (err) return;
        
        // Re-check leadership when children change
        if (children && children.length > 0) {
            const leaderNode = children.sort()[0];
            const wasLeader = isLeader;
            isLeader = myNodePath.endsWith(leaderNode);
            
            if (wasLeader && !isLeader) {
                console.log("⚠️ Lost leadership!");
                onLoseLeadership();
            } else if (!wasLeader && isLeader) {
                console.log("🎉 Gained leadership!");
                onBecomeLeader();
            }
            
            currentLeader = leaderNode;
        }
        
        // Continue watching
        setTimeout(watchElectionPath, 1000);
    });
}

// Callbacks for leadership changes
function onBecomeLeader() {
    // Start Kafka consumer only when leader
    // This is where you'd start your business logic
    console.log("Starting leader tasks...");
}

function onLoseLeadership() {
    // Stop any leader-only operations
    console.log("Stopping leader tasks...");
}

function amILeader() {
    return isLeader;
}

function getCurrentLeader() {
    return currentLeader;
}

module.exports = { startElection, amILeader, getCurrentLeader };
use std::collections::HashMap;

use kitsune2_api::*;
use kitsune2_toy::*;
use polestar::generate::*;

const NUM_NODES: usize = 3;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut g = proptest::test_runner::TestRunner::default();

    let mut nodes = HashMap::new();
    for _ in 0..NUM_NODES {
        let id: AgentId = g.generate().unwrap();
        let (node, _handle) = Node::spawn(id.clone());
        nodes.insert(id, node);
    }

    for (n, node) in nodes.iter() {
        for (p, peer) in nodes.iter() {
            if n != p {
                let sender = peer.lock().await.transport.sender_to_self.clone();
                node.lock().await.transport.peers.insert(p.clone(), sender);
            }
        }
    }

    loop {
        let op: OpId = g.generate().unwrap();
    }
}

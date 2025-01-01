use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};

use kitsune2_api::*;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct Op;

#[derive(Default)]
pub struct Fetch {
    requests: Vec<(OpId, AgentId)>,
    cooldown: HashSet<AgentId>,
}

impl Fetch {
    pub fn add_request(&mut self, op_id: OpId, agent_id: AgentId) {
        self.requests.push((op_id, agent_id));
    }
}

#[derive(Default)]
pub struct OpStore {
    ops: HashMap<OpId, Op>,
}

impl OpStore {
    pub fn add_op(&mut self, op_id: OpId, op: Op) {
        self.ops.insert(op_id, op);
    }
}

pub struct Transport {
    pub sender_to_self: MsgSender,
    pub peers: HashMap<AgentId, MsgSender>,
}

impl Transport {
    pub fn new() -> (Self, MsgReceiver) {
        let (tx, rx) = channel(100);
        (
            Self {
                sender_to_self: tx,
                peers: HashMap::new(),
            },
            rx,
        )
    }
}

pub type MsgSender = Sender<(AgentId, Msg)>;
pub type MsgReceiver = Receiver<(AgentId, Msg)>;

pub enum Msg {
    Publish(OpId),
    OpRequest(OpId),
    OpData(OpId, Option<Op>),
}

pub struct Node {
    pub id: AgentId,
    pub fetch: Fetch,
    pub op_store: OpStore,
    pub transport: Transport,
}

impl Node {
    pub fn spawn(agent_id: AgentId) -> (Arc<Mutex<Self>>, JoinHandle<()>) {
        let fetch = Fetch::default();
        let op_store = OpStore::default();
        let (transport, receiver) = Transport::new();
        let node = Arc::new(Mutex::new(Node {
            id: agent_id,
            fetch,
            op_store,
            transport,
        }));
        let handle = tokio::spawn(Self::run(node.clone(), receiver));
        (node, handle)
    }

    async fn run(node: Arc<Mutex<Node>>, mut receiver: MsgReceiver) {
        loop {
            if let Some((agent_id, msg)) = receiver.recv().await {
                let mut node = node.lock().await;
                match msg {
                    Msg::Publish(op_id) => {
                        node.fetch.add_request(op_id, agent_id);
                    }
                    Msg::OpRequest(op_id) => {
                        let op = node.op_store.ops.get(&op_id).cloned();
                        node.transport
                            .peers
                            .get(&agent_id)
                            .unwrap()
                            .send((node.id.clone(), Msg::OpData(op_id, op)))
                            .await
                            .unwrap();
                    }
                    Msg::OpData(op_id, op) => {
                        if let Some(op) = op {
                            node.op_store.add_op(op_id, op);
                        }
                    }
                }
            } else {
                break;
            }
        }
    }

    pub fn add_peer(&mut self, peer: AgentId, sender: MsgSender) {
        self.transport.peers.insert(peer, sender);
    }
}

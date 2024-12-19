use anyhow::{anyhow, bail};
use im::{HashMap, HashSet, OrdSet};
use polestar::prelude::*;

use crate::{fetch::*, op_store_memory::*, peer_store_basic::*, AgentId, OpId};

pub type DelayMax = UpToLazy<847923>;

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

type Action = NetworkAction;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
#[display("(A{_0}: {_1})")]
pub struct NetworkAction(AgentId, NodeAction);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum NodeAction {
    #[display("Tick")]
    Tick,

    #[display("AuthorOp(op{})", _0)]
    AuthorOp(OpId),

    #[display("Request(A{}, op{}, T{})", _0, _1, _2)]
    RequestOp(AgentId, OpId, Delay),
    // #[display("{}", _0)]
    // Sub(NetworkSubAction),
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum NetworkSubAction {
    #[display("({}", _0)]
    Op(OpStoreMemoryAction),
    #[display("({})", _0)]
    Fetch(FetchAction),
    #[display("({})", _0)]
    Peer(PeerStoreBasicAction),
}

/*        █████               █████
         ░░███               ░░███
  █████  ███████    ██████   ███████    ██████
 ███░░  ░░░███░    ░░░░░███ ░░░███░    ███░░███
░░█████   ░███      ███████   ░███    ░███████
 ░░░░███  ░███ ███ ███░░███   ░███ ███░███░░░
 ██████   ░░█████ ░░████████  ░░█████ ░░██████
░░░░░░     ░░░░░   ░░░░░░░░    ░░░░░   ░░░░░░  */

type State = NetworkState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkState {
    pub nodes: HashMap<AgentId, NodeState>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeState {
    pub inflight: OrdSet<InflightOp>,

    pub sub: NetworkSubState,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkSubState {
    pub peers: PeerStoreBasicState,
    pub ops: OpStoreMemoryState,
    pub fetch: FetchState,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display,
)]
#[display("(A{from_agent}, op{op_id}, T{delay})")]
pub struct InflightOp {
    pub delay: Delay,
    pub from_agent: AgentId,
    pub op_id: OpId,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum Delay {
    #[display("{}", _0)]
    Finite(DelayMax),
    #[display("∞")]
    Infinite,
}

impl Delay {
    pub fn finite(num: usize) -> Self {
        Delay::Finite(UpToLazy::new(num))
    }

    pub fn tick(self) -> Self {
        match self {
            Delay::Infinite => Delay::Infinite,
            Delay::Finite(delay) => {
                Delay::Finite(if *delay == 0 { delay } else { delay - 1 })
            }
        }
    }
}

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

/// Each node in the network uses the same model, so we don't need one per node.
pub struct NetworkModel {
    node_ids: HashSet<AgentId>,
    sub: NetworkSubModel,

    /// If true, only request ops that are known to be held by the peer.
    /// (this is an unrealistic form of prescience, but weeds out a lot of uninteresting states).
    only_request_held: bool,
}

#[derive(Default)]
pub struct NetworkSubModel {
    // peers: PeerStoreBasicModel,
    ops: OpStoreMemoryModel,
    // fetch: FetchModel,
}

impl Machine for NetworkModel {
    type Action = Action;
    type State = State;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        NetworkAction(node_id, action): NetworkAction,
    ) -> TransitionResult<Self> {
        let () = state.nodes.owned_update(node_id, |network, mut node| {
            match action {
                NodeAction::AuthorOp(op_id) => {
                    if network.values().any(|n| n.sub.ops.contains(&op_id)) {
                        bail!("Op already authored");
                    }

                    node.sub.ops = self.sub.ops.transition_(
                        node.sub.ops,
                        OpStoreMemoryAction::AddOp(op_id),
                    )?;
                }

                NodeAction::RequestOp(agent_id, op_id, delay) => {
                    if agent_id == node_id {
                        bail!("Node cannot request op from self");
                    }
                    if !node.sub.peers.contains(&agent_id) {
                        bail!("Agent not in peer store");
                    }
                    if node.sub.ops.contains(&op_id) {
                        bail!("Op already in op store");
                    }

                    if self.only_request_held {
                        if self.only_request_held
                            && !network
                                .get(&agent_id)
                                .ok_or_else(|| {
                                    anyhow!("no peer with id {}", agent_id)
                                })?
                                .sub
                                .ops
                                .contains(&op_id)
                        {
                            bail!("only_request_held: Op not held by peer");
                        }
                    }

                    node.inflight.insert(InflightOp {
                        delay,
                        from_agent: agent_id,
                        op_id,
                    });
                }

                NodeAction::Tick => {
                    if node.inflight.is_empty() {
                        bail!("No inflight ops: tick has no effect");
                    }

                    let delay_split = if DelayMax::limit() > 1 {
                        Delay::finite(1)
                    } else {
                        Delay::Infinite
                    };

                    // pull out the currently ready items (delay==0)
                    let (instant, pending) = node.inflight.split(&InflightOp {
                        delay: delay_split,
                        from_agent: AgentId::new(0),
                        op_id: OpId::new(0),
                    });

                    // simulate performing the fetch
                    node.sub.ops = instant.into_iter().fold(
                        Ok(node.sub.ops),
                        |ops: anyhow::Result<OpStoreMemoryState>, op| {
                            let ops = ops?;
                            let peer = network.get(&op.from_agent).ok_or_else(
                                || anyhow!("no peer with id {}", op.from_agent),
                            )?;

                            if peer.sub.ops.contains(&op.op_id) {
                                self.sub.ops.transition_(
                                    ops,
                                    OpStoreMemoryAction::AddOp(op.op_id),
                                )
                            } else {
                                Ok(ops)
                            }
                        },
                    )?;

                    node.inflight = pending
                        .into_iter()
                        .map(|mut op| {
                            op.delay = op.delay.tick();
                            op
                        })
                        .collect();
                } // NodeAction::Sub(sub) => match sub {
                  //     NetworkSubAction::Op(op_action) => match op_action {
                  //         OpStoreMemoryAction::AddOp(op_id) => {
                  //             let (ops, _) = self
                  //                 .sub
                  //                 .ops
                  //                 .transition(node.sub.ops, op_action)?;
                  //             node.sub.ops = ops;
                  //         }
                  //     },
                  //     NetworkSubAction::Peer(peer_action) => {
                  //         #[allow(irrefutable_let_patterns)]
                  //         if let PeerStoreBasicAction::AddAgent(a)
                  //         | PeerStoreBasicAction::RemoveAgent(a) = peer_action
                  //         {
                  //             if a == node_id {
                  //                 bail!("Node cannot add or remove self as peer");
                  //             }
                  //         }
                  //         let (peers, _) = self
                  //             .sub
                  //             .peers
                  //             .transition(node.sub.peers, peer_action)?;
                  //         node.sub.peers = peers;
                  //     }
                  //     NetworkSubAction::Fetch(fetch_action) => {
                  //         let (fetch, _) = self
                  //             .sub
                  //             .fetch
                  //             .transition(node.sub.fetch, fetch_action)?;
                  //         node.sub.fetch = fetch;
                  //     }
                  // },
            }
            Ok((node, ()))
        })?;

        Ok((state, ()))
    }
}

impl Default for NetworkModel {
    fn default() -> Self {
        Self {
            node_ids: AgentId::all_values().into(),
            sub: NetworkSubModel::default(),
            only_request_held: true,
        }
    }
}

impl NetworkModel {
    pub fn initial(&self) -> NetworkState {
        NetworkState {
            nodes: self
                .node_ids
                .iter()
                .map(|node_id| {
                    let mut state = NodeState::default();
                    for peer in self.node_ids.iter() {
                        if peer != node_id {
                            state.sub.peers.peers.insert(*peer);
                        }
                    }
                    (*node_id, state)
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delay_ordering() {
        DelayMax::set_limit(3);
        assert!(
            Delay::Finite(DelayMax::new(1)) < Delay::Finite(DelayMax::new(2))
        );
        assert!(Delay::Finite(DelayMax::new(2)) < Delay::Infinite);
    }
}

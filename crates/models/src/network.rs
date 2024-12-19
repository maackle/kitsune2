use anyhow::bail;
use im::{HashMap, OrdSet, Vector};
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
pub enum NetworkAction {
    #[display("Tick")]
    Tick,
    #[display("RequestOp({}, {})", _0, _1)]
    RequestOp(AgentId, OpId),
    #[display("SetDelay({}, {})", _0, _1)]
    SetDelay(AgentId, Delay),

    #[display("Sub({})", _0)]
    Sub(NetworkSubAction),
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
    #[display("Op({:?})", _0)]
    Op(OpStoreMemoryAction),
    #[display("Peer({:?})", _0)]
    Peer(PeerStoreBasicAction),
    #[display("Fetch({:?})", _0)]
    Fetch(FetchAction),
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

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkState {
    pub inflight: OrdSet<InflightOp>,
    pub delays: HashMap<AgentId, Delay>,

    pub sub: NetworkSubState,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkSubState {
    pub peers: PeerStoreBasicState,
    pub ops: OpStoreMemoryState,
    pub fetch: FetchState,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    #[display("∞")]
    Infinite,
    #[display("{}", _0)]
    Finite(DelayMax),
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

type Model = NetworkModel;

#[derive(Default)]
pub struct NetworkModel {
    sub: NetworkSubModel,
}

#[derive(Default)]
pub struct NetworkSubModel {
    peers: PeerStoreBasicModel,
    ops: OpStoreMemoryModel,
    fetch: FetchModel,
}

impl Machine for NetworkModel {
    type Action = NetworkAction;
    type State = NetworkState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        match action {
            NetworkAction::Tick => {
                let (instant, pending) = if DelayMax::limit() > 1 {
                    state.inflight.split(&InflightOp {
                        delay: Delay::finite(1),
                        from_agent: AgentId::new(0),
                        op_id: OpId::new(0),
                    })
                } else {
                    (state.inflight, Default::default())
                };

                state.sub.ops = instant.into_iter().fold(
                    Ok(state.sub.ops),
                    |ops: anyhow::Result<OpStoreMemoryState>, op| {
                        let (ops, _) = self.sub.ops.transition(
                            ops?,
                            OpStoreMemoryAction::AddOp(op.op_id),
                        )?;
                        Ok(ops)
                    },
                )?;

                state.inflight = pending
                    .into_iter()
                    .map(|mut op| {
                        op.delay = op.delay.tick();
                        op
                    })
                    .collect();
            }

            NetworkAction::SetDelay(agent_id, delay) => {
                let old = state.delays.insert(agent_id, delay);
                if old == Some(delay) {
                    bail!("no change to delay");
                }
            }

            NetworkAction::RequestOp(agent_id, op_id) => {
                if !state.sub.peers.contains(&agent_id) {
                    bail!("Agent not in peer store");
                }
                if state.sub.ops.contains(&op_id) {
                    bail!("Op already in op store");
                }

                let delay = state
                    .delays
                    .get(&agent_id)
                    .cloned()
                    .unwrap_or_else(|| Delay::finite(0));
                state.inflight.insert(InflightOp {
                    delay,
                    from_agent: agent_id,
                    op_id,
                });
            }

            NetworkAction::Sub(sub) => match sub {
                NetworkSubAction::Op(op_action) => {
                    let (ops, _) =
                        self.sub.ops.transition(state.sub.ops, op_action)?;
                    state.sub.ops = ops;
                }
                NetworkSubAction::Peer(peer_action) => {
                    let (peers, _) = self
                        .sub
                        .peers
                        .transition(state.sub.peers, peer_action)?;
                    state.sub.peers = peers;
                }
                NetworkSubAction::Fetch(fetch_action) => {
                    //TODO
                    // let (fetch, _) = self
                    //     .sub
                    //     .fetch
                    //     .transition(state.sub.fetch, fetch_action)?;
                    // state.sub.fetch = fetch;
                }
            },
        }
        Ok((state, ()))
    }
}

impl NetworkModel {
    pub fn initial(&self) -> NetworkState {
        NetworkState::default()
    }
}

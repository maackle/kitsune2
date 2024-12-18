use polestar::*;

use crate::{fetch::*, op_store_memory::*, peer_store_basic::*, OpId};

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

type Action = AvailabilityAction;

pub enum AvailabilityAction {
    Op(OpStoreMemoryAction),
    Peer(PeerStoreBasicAction),
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

type State = AvailabilityState;

pub struct AvailabilityState {
    peers: PeerStoreBasicState,
    ops: OpStoreMemoryState,
    fetch: FetchState,
}

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

type Model = AvailabilityModel;

pub struct AvailabilityModel {
    peers: PeerStoreBasicModel,
    ops: OpStoreMemoryModel,
    fetch: FetchModel,
}

impl Machine for AvailabilityModel {
    type Action = AvailabilityAction;
    type State = AvailabilityState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        match action {
            AvailabilityAction::Op(op_action) => {
                let (ops, _) = self.ops.transition(state.ops, op_action)?;
                state.ops = ops;
            }
            AvailabilityAction::Peer(peer_action) => {
                let (peers, _) =
                    self.peers.transition(state.peers, peer_action)?;
                state.peers = peers;
            }
            AvailabilityAction::Fetch(fetch_action) => {
                let (fetch, _) =
                    self.fetch.transition(state.fetch, fetch_action)?;
                state.fetch = fetch;
            }
        }
        Ok((state, ()))
    }
}

use derive_more::derive::{Deref, DerefMut};
use im::HashSet;
use polestar::{Machine, TransitionResult};

use crate::AgentId;

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

type Action = PeerStoreBasicAction;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum PeerStoreBasicAction {
    AddAgent(AgentId),
    RemoveAgent(AgentId),
}

/*        █████               █████
         ░░███               ░░███
  █████  ███████    ██████   ███████    ██████
 ███░░  ░░░███░    ░░░░░███ ░░░███░    ███░░███
░░█████   ░███      ███████   ░███    ░███████
 ░░░░███  ░███ ███ ███░░███   ░███ ███░███░░░
 ██████   ░░█████ ░░████████  ░░█████ ░░██████
░░░░░░     ░░░░░   ░░░░░░░░    ░░░░░   ░░░░░░  */

type State = PeerStoreBasicState;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Deref, DerefMut)]
pub struct PeerStoreBasicState {
    pub peers: HashSet<AgentId>,
}

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

type Model = PeerStoreBasicModel;

#[derive(Default)]
pub struct PeerStoreBasicModel;

impl Machine for PeerStoreBasicModel {
    type Action = PeerStoreBasicAction;
    type State = PeerStoreBasicState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        match action {
            PeerStoreBasicAction::AddAgent(agent_id) => {
                state.peers.insert(agent_id);
            }
            PeerStoreBasicAction::RemoveAgent(agent_id) => {
                state.peers.remove(&agent_id);
            }
        }
        Ok((state, ()))
    }
}

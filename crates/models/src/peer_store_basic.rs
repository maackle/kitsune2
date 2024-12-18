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

pub struct PeerStoreBasicState;

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

type Model = PeerStoreBasicModel;

pub struct PeerStoreBasicModel;

impl Machine for PeerStoreBasicModel {
    type Action = PeerStoreBasicAction;
    type State = PeerStoreBasicState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        todo!()
    }

    fn is_terminal(&self, _: &Self::State) -> bool {
        todo!()
    }
}

use polestar::*;

use crate::{AgentId, OpId};

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

type Action = FetchAction;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum FetchAction {
    #[display("AddOp(o{}, a{})", _0, _1)]
    AddOp(OpId, AgentId),
    #[display("RemoveOp(o{}, a{})", _0, _1)]
    RemoveOp(OpId, AgentId),
    #[display("RemoveOpsForAgent(a{})", _0)]
    RemoveOpsForAgent(AgentId),

    #[display("AgentCoolDown(a{})", _0)]
    AgentCoolDown(AgentId),
    #[display("AgentWarmUp(a{})", _0)]
    AgentWarmUp(AgentId),
}

/*        █████               █████
         ░░███               ░░███
  █████  ███████    ██████   ███████    ██████
 ███░░  ░░░███░    ░░░░░███ ░░░███░    ███░░███
░░█████   ░███      ███████   ░███    ░███████
 ░░░░███  ░███ ███ ███░░███   ░███ ███░███░░░
 ██████   ░░█████ ░░████████  ░░█████ ░░██████
░░░░░░     ░░░░░   ░░░░░░░░    ░░░░░   ░░░░░░  */

type State = FetchState;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchState {}

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

type Model = FetchModel;

#[derive(Default)]
pub struct FetchModel;

impl Machine for FetchModel {
    type Action = FetchAction;
    type State = FetchState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        todo!()
    }
}

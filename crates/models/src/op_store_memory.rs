use derive_more::derive::{Deref, DerefMut};
use im::HashSet;
use polestar::{Machine, TransitionResult};

use crate::OpId;

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

type Action = OpStoreMemoryAction;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    exhaustive::Exhaustive,
)]
pub enum OpStoreMemoryAction {
    AddOp(OpId),
}

/*        █████               █████
░░███               ░░███
  █████  ███████    ██████   ███████    ██████
 ███░░  ░░░███░    ░░░░░███ ░░░███░    ███░░███
░░█████   ░███      ███████   ░███    ░███████
 ░░░░███  ░███ ███ ███░░███   ░███ ███░███░░░
 ██████   ░░█████ ░░████████  ░░█████ ░░██████
░░░░░░     ░░░░░   ░░░░░░░░    ░░░░░   ░░░░░░  */

type State = OpStoreMemoryState;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Deref, DerefMut)]
pub struct OpStoreMemoryState {
    pub ops: HashSet<OpId>,
}

/*                            █████          ████
                             ░░███          ░░███
 █████████████    ██████   ███████   ██████  ░███
░░███░░███░░███  ███░░███ ███░░███  ███░░███ ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███████  ░███
 ░███ ░███ ░███ ░███ ░███░███ ░███ ░███░░░   ░███
 █████░███ █████░░██████ ░░████████░░██████  █████
░░░░░ ░░░ ░░░░░  ░░░░░░   ░░░░░░░░  ░░░░░░  ░░░░░  */

type Model = OpStoreMemoryModel;

#[derive(Default)]
pub struct OpStoreMemoryModel;

impl Machine for OpStoreMemoryModel {
    type Action = OpStoreMemoryAction;
    type State = OpStoreMemoryState;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        match action {
            OpStoreMemoryAction::AddOp(op_id) => {
                state.ops.insert(op_id);
            }
        }
        Ok((state, ()))
    }
}

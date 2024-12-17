//! Polestar model for the bootstrap server.
//!
//! Omissions:
//! - Does not handle new entries which are older than the current newest entry.
//! - Does not handle the case of Update for a particular space with no new entry.
//! - Does not explicitly model the Putting of data, which is a separate atomic operation and would be good to model.
//!
//! As with any model, wall-clock time is not modeled, and instead relative time is used.

#![allow(unused)]

use itertools::Itertools;
use std::collections::BTreeMap;

use anyhow::{anyhow, bail};
use exhaustive::Exhaustive;
use polestar::prelude::*;
use serde::{Deserialize, Serialize};

const NUM_SPACES: usize = 2;
const NUM_AGENTS: usize = 3;
const ENTRIES_PER_SPACE: usize = 4;
const MAX_ENTRIES: usize = NUM_SPACES * ENTRIES_PER_SPACE;

/// Space IDs
pub type S = UpTo<NUM_SPACES>;

/// Agent IDs
pub type A = UpTo<NUM_AGENTS>;

/// Entry IDs
pub type E = UpTo<ENTRIES_PER_SPACE>;

/// Expiry ordering IDs
pub type T = UpTo<MAX_ENTRIES>;

/*                   █████     ███
                    ░░███     ░░░
  ██████    ██████  ███████   ████   ██████  ████████
 ░░░░░███  ███░░███░░░███░   ░░███  ███░░███░░███░░███
  ███████ ░███ ░░░   ░███     ░███ ░███ ░███ ░███ ░███
 ███░░███ ░███  ███  ░███ ███ ░███ ░███ ░███ ░███ ░███
░░████████░░██████   ░░█████  █████░░██████  ████ █████
 ░░░░░░░░  ░░░░░░     ░░░░░  ░░░░░  ░░░░░░  ░░░░ ░░░░░   */

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Exhaustive,
    Serialize,
    Deserialize,
    derive_more::Display,
)]
pub enum BootAction {
    Get,

    #[display("U {} ({expire_count})", info.map(|i| format!("{i}")).unwrap_or("*".to_string()))]
    Update {
        /// The new info to add, if any
        info: Option<NewInfo>,
        /// The number of entries to expire during this update
        expire_count: T,
    },

    StopWorker,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Exhaustive,
    Serialize,
    Deserialize,
    derive_more::Display,
)]
#[display("[{expiry_index}]{}{agent}:{space}", if *is_tombstone { "X" } else { " " })]
pub struct NewInfo {
    pub agent: A,
    pub space: S,
    pub expiry_index: T,
    /// Whether this info is newer than any existing info
    pub is_newer: bool,
    pub is_tombstone: bool,
}

/*        █████               █████
         ░░███               ░░███
  █████  ███████    ██████   ███████    ██████
 ███░░  ░░░███░    ░░░░░███ ░░░███░    ███░░███
░░█████   ░███      ███████   ░███    ░███████
 ░░░░███  ░███ ███ ███░░███   ░███ ███░███░░░
 ██████   ░░█████ ░░████████  ░░█████ ░░██████
░░░░░░     ░░░░░   ░░░░░░░░    ░░░░░   ░░░░░░  */

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::Display,
)]
#[display("({num_workers}) [{}]", entries.iter().map(|e| format!("{e}")).join(", "))]
pub struct BootState {
    num_workers: usize,
    entries: im::Vector<StoredEntry>,
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::Display,
)]
#[display("{space}:{agent}")]
pub struct StoredEntry {
    space: S,
    agent: A,
}

/*                                  █████       ███
                                   ░░███       ░░░
 █████████████    ██████    ██████  ░███████   ████  ████████    ██████
░░███░░███░░███  ░░░░░███  ███░░███ ░███░░███ ░░███ ░░███░░███  ███░░███
 ░███ ░███ ░███   ███████ ░███ ░░░  ░███ ░███  ░███  ░███ ░███ ░███████
 ░███ ░███ ░███  ███░░███ ░███  ███ ░███ ░███  ░███  ░███ ░███ ░███░░░
 █████░███ █████░░████████░░██████  ████ █████ █████ ████ █████░░██████
░░░░░ ░░░ ░░░░░  ░░░░░░░░  ░░░░░░  ░░░░ ░░░░░ ░░░░░ ░░░░ ░░░░░  ░░░░░░  */

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BootModel {
    pub initial_workers: usize,
}

impl Machine for BootModel {
    type State = BootState;
    type Action = BootAction;
    type Fx = ();
    type Error = anyhow::Error;

    fn transition(
        &self,
        mut state: Self::State,
        action: Self::Action,
    ) -> TransitionResult<Self> {
        if state.num_workers == 0 {
            bail!("no workers left");
        }

        match action {
            BootAction::Get => bail!("unimplemented"),
            BootAction::Update { info, expire_count } => {
                if *expire_count > state.entries.len() {
                    bail!("expire_count is greater than the number of entries");
                } else if *expire_count == state.entries.len() {
                    state.entries.clear();
                } else if *expire_count > 0 {
                    let (_before, after) =
                        state.entries.split_at(*expire_count);
                    state.entries = after;
                }

                if let Some(info) = info {
                    let existing =
                        state.entries.iter().enumerate().find(|(_, e)| {
                            e.space == info.space && e.agent == info.agent
                        });

                    if existing.is_some() && !info.is_newer {
                        return Ok((state, ()));
                    }

                    if existing.is_none() && info.is_tombstone {
                        bail!("nothing to tombstone");
                    }

                    // this is certainly wrong since it's not clear whether expiry_index
                    // is pre- or post-pruning
                    if info.is_tombstone {
                        if *info.expiry_index > state.entries.len() {
                            bail!("expiry_index too large");
                        }
                    } else {
                        if *info.expiry_index > state.entries.len() {
                            bail!("expiry_index too large");
                        }
                    }

                    // add the new entry if it's not a tombstone
                    // TODO: shouldn't the tombstone actually persist?
                    if !info.is_tombstone {
                        let i = (*info.expiry_index).min(state.entries.len());
                        let entry = StoredEntry {
                            space: info.space,
                            agent: info.agent,
                        };
                        state.entries.insert(*info.expiry_index, entry);
                    }
                }
            }
            BootAction::StopWorker => {
                state.num_workers -= 1;
            }
        }
        Ok((state, ()))
    }

    fn is_terminal(&self, s: &Self::State) -> bool {
        s.num_workers == 0
    }
}

impl BootModel {
    pub fn initial(&self) -> BootState {
        BootState {
            num_workers: self.initial_workers,
            entries: im::Vector::new(),
        }
    }
}

/*█████                      █████
 ░░███                      ░░███
 ███████    ██████   █████  ███████    █████
░░░███░    ███░░███ ███░░  ░░░███░    ███░░
  ░███    ░███████ ░░█████   ░███    ░░█████
  ░███ ███░███░░░   ░░░░███  ░███ ███ ░░░░███
  ░░█████ ░░██████  ██████   ░░█████  ██████
   ░░░░░   ░░░░░░  ░░░░░░     ░░░░░  ░░░░░░*/

#[cfg(test)]
mod tests {

    use super::*;
}

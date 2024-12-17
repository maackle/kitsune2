#![allow(unused)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};

use ed25519_dalek::VerifyingKey;
use polestar::{
    id::IdMap,
    mapping::{ActionOf, JsonActionWriter, ModelMapping, StateOf},
};

use crate::{ParsedEntry, SpaceId};

use bootstrap_model::{BootAction, BootModel, NewInfo, StoredEntry};

static EVENT_WRITER: once_cell::sync::Lazy<
    Mutex<JsonActionWriter<BootstrapModelMapping>>,
> = once_cell::sync::Lazy::new(|| {
    Mutex::new(
        JsonActionWriter::new(
            "/tmp/k2-events.json",
            BootstrapModelMapping::default(),
        )
        .unwrap(),
    )
});

#[derive(Debug)]
pub enum BootstrapEvent {
    Get,
    Put(ParsedEntry),
    Update { now: i64, entry: ParsedEntry },
    UpdateAll { now: i64 },
    StopWorker,
}

impl BootstrapEvent {
    pub fn record(self) -> std::io::Result<()> {
        EVENT_WRITER.lock().unwrap().write_event(&self)
    }
}

#[derive(Default)]
pub struct BootstrapModelMapping {
    spaces: IdMap<SpaceId, bootstrap_model::S>,
    agents: IdMap<VerifyingKey, bootstrap_model::A>,
    /// Entries keyed by expiry time
    entries: HashMap<(SpaceId, VerifyingKey), ParsedEntry>,
}

impl ModelMapping for BootstrapModelMapping {
    type Model = BootModel;
    type System = ();
    type Event = BootstrapEvent;

    fn map_event(
        &mut self,
        event: &Self::Event,
    ) -> Option<ActionOf<Self::Model>> {
        return None;
        Some(match event {
            BootstrapEvent::Get => BootAction::Get,
            BootstrapEvent::Put(_) => return None,
            BootstrapEvent::Update { now, entry } => {
                // this is probably too much
                let is_newer = match self
                    .entries
                    .entry((entry.space.clone(), entry.agent))
                {
                    std::collections::hash_map::Entry::Occupied(mut o) => {
                        let existing = o.get_mut();
                        let is_newer = entry.created_at > existing.created_at;
                        // if entry.is_tombstone {
                        //     o.remove();
                        // } else {
                        //     *existing = entry.clone();
                        // }

                        todo!()
                    }
                    std::collections::hash_map::Entry::Vacant(v) => {
                        todo!()
                    }
                };

                self.expire_upto(*now);

                self.entries.values().min_by_key(|v| v.expires_at);

                let info = NewInfo {
                    expiry_index: todo!(),
                    agent: self.agents.lookup(entry.agent).unwrap(),
                    space: self.spaces.lookup(entry.space.clone()).unwrap(),
                    is_newer,
                    is_tombstone: entry.is_tombstone,
                };

                self.entries
                    .insert((entry.space.clone(), entry.agent), entry.clone());

                let expire_count = self.expire_upto(*now);

                BootAction::Update {
                    info: Some(info),
                    expire_count,
                }
            }
            BootstrapEvent::UpdateAll { now } => {
                let expire_count = self.expire_upto(*now);

                BootAction::Update {
                    info: None,
                    expire_count,
                }
            }
            BootstrapEvent::StopWorker => BootAction::StopWorker,
        })
    }

    fn map_state(
        &mut self,
        _state: &Self::System,
    ) -> Option<StateOf<Self::Model>> {
        unimplemented!()
    }
}

impl BootstrapModelMapping {
    pub fn expire_upto(&mut self, now: i64) -> bootstrap_model::T {
        let count_before = self.entries.len();
        self.entries.retain(|_, e| e.expires_at > now);
        let count_after = self.entries.len();
        (count_before - count_after).try_into().unwrap()
    }
}

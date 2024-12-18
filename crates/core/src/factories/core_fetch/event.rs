//! Events to be mapped to the FetchModel

use kitsune2_api::{AgentId, OpId};

/// Events to be mapped to the FetchModel
#[derive(Debug)]
#[must_use = "must call `.record()` to record the event"]
pub enum CoreFetchEvent {
    /// Add an op to fetch from an agent.
    AddOp(OpId, AgentId),

    /// Remove an op from the fetch set.
    RemoveOp(OpId, AgentId),

    /// Remove all ops for an agent.
    RemoveOpsForAgent(AgentId),

    /// Agent is unresponsive.
    AgentCoolDown(AgentId),
}

impl CoreFetchEvent {
    /// Record the event with the mapper
    pub fn record(self) {}
}

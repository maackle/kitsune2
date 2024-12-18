use polestar::id::UpTo;

pub const NUM_AGENTS: usize = 2;
pub const DHT_ARC_BITS: usize = 2;

/// AgentId doubles as a locator, i.e. replaces the URL
pub type AgentId = UpTo<NUM_AGENTS>;

/// DhtArc is "highly quantized" with only a few bits of precision.
/// Each bit represents a chunk of the DHT held.
const DHT_ARC_VALUE_SIZE: usize = 1 << DHT_ARC_BITS;
pub type DhtArc = UpTo<DHT_ARC_VALUE_SIZE>;

pub enum PeerStoreShardedAction {
    AddAgent(AgentId),
    RemoveAgent(AgentId),
}

/// Abstract AgentInfo
///
/// - URL is omitted, and is covered by AgentId
/// -
pub struct AgentInfo {
    agent: AgentId,
    storage_arc: DhtArc,
    is_tombstone: bool,
}

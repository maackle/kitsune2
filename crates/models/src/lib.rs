pub mod bootstrap;
pub mod fetch;
pub mod network;
pub mod op_store_memory;
pub mod peer_store_basic;
// pub mod peer_store_sharded;

pub type OpId = polestar::id::UpToLazy<100>;
pub type AgentId = polestar::id::UpToLazy<200>;

pub type DhtArc = polestar::id::UpTo<4>;

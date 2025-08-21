//! Kitsune2 top-level module related types.

use crate::*;
use std::sync::Arc;

/// Handler for events coming out of Kitsune2.
pub trait KitsuneHandler: 'static + Send + Sync + std::fmt::Debug {
    /// A notification that a new listening address has been bound.
    /// Peers should now go to this new address to reach this node.
    fn new_listening_address(&self, this_url: Url) -> BoxFut<'static, ()> {
        drop(this_url);
        Box::pin(async move {})
    }

    /// A peer has disconnected from us. If they did so gracefully
    /// the reason will be is_some().
    fn peer_disconnect(&self, peer: Url, reason: Option<String>) {
        drop((peer, reason));
    }

    /// Gather preflight data to send to a new opening connection.
    /// Returning an Err result will close this connection.
    ///
    /// The default implementation sends an empty preflight message.
    fn preflight_gather_outgoing(
        &self,
        peer_url: Url,
    ) -> K2Result<bytes::Bytes> {
        drop(peer_url);
        Ok(bytes::Bytes::new())
    }

    /// Validate preflight data sent by a remote peer on a new connection.
    /// Returning an Err result will close this connection.
    ///
    /// The default implementation ignores the preflight data,
    /// and considers it valid.
    fn preflight_validate_incoming(
        &self,
        peer_url: Url,
        data: bytes::Bytes,
    ) -> K2Result<()> {
        drop(peer_url);
        drop(data);
        Ok(())
    }

    /// Kitsune would like to construct a space. Provide a handler.
    fn create_space(
        &self,
        space_id: SpaceId,
    ) -> BoxFut<'_, K2Result<space::DynSpaceHandler>>;
}

/// Trait-object [KitsuneHandler].
pub type DynKitsuneHandler = Arc<dyn KitsuneHandler>;

/// The top-level Kitsune2 api trait.
pub trait Kitsune: 'static + Send + Sync + std::fmt::Debug {
    /// Register the kitsune handler exactly once before invoking any other
    /// api functions.
    ///
    /// This dependency injection strategy makes it possible for a struct
    /// to both act as a Handler and contain the resulting Kitsune instance.
    ///
    /// Implementations should error if this is invoked more that once,
    /// and should return errors for any other api invocations if this has
    /// not yet been called.
    fn register_handler(
        &self,
        handler: DynKitsuneHandler,
    ) -> BoxFut<'_, K2Result<()>>;

    /// List the active spaces.
    fn list_spaces(&self) -> Vec<SpaceId>;

    /// Get an existing space with the provided [SpaceId] or create
    /// a new one.
    fn space(&self, space_id: SpaceId)
        -> BoxFut<'_, K2Result<space::DynSpace>>;

    /// Get a space, only if it exists.
    fn space_if_exists(
        &self,
        space_id: SpaceId,
    ) -> BoxFut<'_, Option<space::DynSpace>>;

    /// Remove a space, if it exists.
    ///
    /// This will remove the space from the list of active spaces. The space will only shutdown
    /// cleanly if all modules are careful about not holding references to the space and transport.
    fn remove_space(&self, space_id: SpaceId) -> BoxFut<'_, K2Result<()>>;

    /// Get the transport handle.
    fn transport(&self) -> BoxFut<'_, K2Result<DynTransport>>;
}

/// Trait-object [Kitsune].
pub type DynKitsune = Arc<dyn Kitsune>;

/// A factory for constructing Kitsune instances.
pub trait KitsuneFactory: 'static + Send + Sync + std::fmt::Debug {
    /// Help the builder construct a default config from the chosen
    /// module factories.
    fn default_config(&self, config: &mut config::Config) -> K2Result<()>;

    /// Validate configuration.
    fn validate_config(&self, config: &config::Config) -> K2Result<()>;

    /// Construct a space instance.
    fn create(
        &self,
        builder: Arc<builder::Builder>,
    ) -> BoxFut<'static, K2Result<DynKitsune>>;
}

/// Trait-object [KitsuneFactory].
pub type DynKitsuneFactory = Arc<dyn KitsuneFactory>;

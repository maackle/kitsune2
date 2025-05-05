#![deny(missing_docs)]
//! kitsune2 iroh transport module.

use base64::Engine;
use iroh::{
    endpoint::{self, Connection, VarInt},
    protocol::Router,
    Endpoint, NodeAddr, NodeId, RelayUrl,
};
use kitsune2_api::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::instrument::WithSubscriber;

#[allow(missing_docs)]
pub mod config {
    /// Configuration parameters for [IrohTransportFactory](super::IrohTransportFactory).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IrohTransportConfig {}

    impl Default for IrohTransportConfig {
        fn default() -> Self {
            Self {}
        }
    }

    /// Module-level configuration for IrohTransport.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IrohTransportModConfig {
        /// IrohTransport configuration.
        pub iroh_transport: IrohTransportConfig,
    }
}

pub use config::*;
/// Provides a Kitsune2 transport module based on the Tx5 crate.
#[derive(Debug)]
pub struct IrohTransportFactory {}

impl IrohTransportFactory {
    /// Construct a new Tx5TransportFactory.
    pub fn create() -> DynTransportFactory {
        let out: DynTransportFactory = Arc::new(IrohTransportFactory {});
        out
    }
}

impl TransportFactory for IrohTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&IrohTransportModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let config: IrohTransportModConfig = config.get_module_config()?;

        // make sure our signal server url is parse-able.
        // let sig = config.iroh_transport.server_url.as_str().to_sig_url()?;
        // let sig = url::Url::parse(&sig)
        //     .map_err(|err| K2Error::other_src("invalid tx5 server url", err))?;
        // let uses_tls = sig.scheme() == "wss";

        // if !uses_tls && !config.tx5_transport.signal_allow_plain_text {
        //     return Err(K2Error::other(format!(
        //         "disallowed plaintext signal url, either specify wss or set signal_allow_plain_text to true: {}",
        //         sig,
        //     )));
        // }

        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        Box::pin(async move {
            let config: IrohTransportModConfig =
                builder.config.get_module_config()?;

            // let mut tx5_init_config = tx5_core::Tx5InitConfig {
            //     tracing_enabled: config.tx5_transport.tracing_enabled,
            //     ..Default::default()
            // };

            // if let Some(port) = config.tx5_transport.ephemeral_udp_port_min {
            //     tx5_init_config.ephemeral_udp_port_min = port;
            // }

            // if let Some(port) = config.tx5_transport.ephemeral_udp_port_max {
            //     tx5_init_config.ephemeral_udp_port_max = port;
            // }

            // // Ignore errors. Only the first call of this can succeed.
            // let _ = tx5_init_config.set_as_global_default();

            let handler = TxImpHnd::new(handler);
            let imp =
                IrohTransport::create(config.iroh_transport, handler.clone())
                    .await?;
            Ok(DefaultTransport::create(&handler, imp))
        })
    }
}

const ALPN: &[u8] = b"kitsune2";

#[derive(Debug)]
struct IrohTransport {
    endpoint: Arc<Endpoint>,
    connections: Arc<Mutex<BTreeMap<NodeAddr, Connection>>>,
    evt_task: tokio::task::AbortHandle,
}

impl Drop for IrohTransport {
    fn drop(&mut self) {
        self.evt_task.abort();
    }
}

impl IrohTransport {
    pub async fn create(
        config: IrohTransportConfig,
        handler: Arc<TxImpHnd>,
    ) -> K2Result<DynTxImp> {
        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .map_err(|err| K2Error::other("bad"))?;

        let endpoint = Arc::new(endpoint);

        let evt_task = tokio::task::spawn(evt_task(handler, endpoint.clone()))
            .abort_handle();

        let out: DynTxImp = Arc::new(Self {
            endpoint,
            connections: Arc::new(Mutex::new(BTreeMap::new())),
            evt_task,
        });

        Ok(out)
    }
}

fn peer_url_to_node_addr(peer_url: Url) -> Result<NodeAddr, K2Error> {
    let Some(peer_id) = peer_url.peer_id() else {
        return Err(K2Error::other("empty peer url"));
    };
    let node_id = NodeId::try_from(peer_id.as_bytes())
        .map_err(|err| K2Error::other("bad peer id"))?;

    let relay_url = url::Url::parse(peer_url.addr())
        .map_err(|err| K2Error::other("Bad addr"))?;

    Ok(NodeAddr {
        node_id,
        relay_url: Some(RelayUrl::from(relay_url)),
        direct_addresses: BTreeSet::new(),
    })
}

fn node_addr_to_peer_url(node_addr: NodeAddr) -> Option<Url> {
    let Some(relay_url) = node_addr.relay_url else {
        return None;
    };
    let u = format!("{}/{}", relay_url, node_addr.node_id);
    Url::from_str(u.as_str()).ok()
}

impl TxImp for IrohTransport {
    fn url(&self) -> Option<Url> {
        let Ok(Some(relay_url)) = self.endpoint.home_relay().get() else {
            return None;
        };
        let u = format!("{}/{}", relay_url, self.endpoint.node_id());
        Url::from_str(u.as_str()).ok()
    }

    fn disconnect(
        &self,
        peer: Url,
        _payload: Option<(String, bytes::Bytes)>,
    ) -> BoxFut<'_, ()> {
        Box::pin(async move {
            let Ok(addr) = peer_url_to_node_addr(peer) else {
                tracing::error!("Bad peer url to node addr");
                return;
            };
            let mut connections = self.connections.lock().await;
            if let Some(connection) = connections.get(&addr) {
                connection.close(VarInt::from_u32(0), &[]);
                connections.remove(&addr);
            }
            ()
        })
    }

    fn send(&self, peer: Url, data: bytes::Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            let addr = peer_url_to_node_addr(peer)
                .map_err(|err| K2Error::other("bad peer url"))?;

            let mut connections = self.connections.lock().await;

            if !connections.contains_key(&addr) {
                let connection = self
                    .endpoint
                    .connect(addr.clone(), ALPN)
                    .await
                    .map_err(|err| K2Error::other("failed to connect"))?;
                connections.insert(addr.clone(), connection);
            }

            let Some(connection) = connections.get(&addr) else {
                return Err(K2Error::other("no connection with peer"));
            };
            let mut send = connection
                .open_uni()
                .await
                .map_err(|err| K2Error::other("Failed to open uni"))?;

            send.write_all(data.as_ref())
                .await
                .map_err(|err| K2Error::other("Failed to write all"))?;
            send.finish()
                .map_err(|err| K2Error::other("Failed to close stream"))?;
            Ok(())
            // let connection = self.connections.entry(addr).or_insert_with(default)

            // // this would be more efficient if we retool tx5 to use bytes
            // self.ep
            //     .send(peer, data.to_vec())
            //     .await
            //     .map_err(|e| K2Error::other_src("tx5 send error", e))
        })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<TransportStats>> {
        Box::pin(async move {
            let connections = self.connections.lock().await;

            Ok(TransportStats {
                backend: format!("iroh"),
                peer_urls: connections
                    .iter()
                    .filter_map(|(node_addr, _)| {
                        node_addr_to_peer_url(node_addr.clone())
                    })
                    .collect(),
                connections: connections
                    .iter()
                    .map(|(peer_addr, conn)| TransportConnectionStats {
                        pub_key: base64::prelude::BASE64_URL_SAFE_NO_PAD
                            .encode(peer_addr.node_id),
                        send_message_count: 0,
                        send_bytes: 0,
                        recv_message_count: 0,
                        recv_bytes: 0,
                        opened_at_s: 0,
                        is_webrtc: false,
                    })
                    .collect(),
            })
        })
    }
}

async fn evt_task(handler: Arc<TxImpHnd>, endpoint: Arc<Endpoint>) {
    // let _drop = TaskDrop("evt_task");
    // use tx5::EndpointEvent::*;
    while let Some(incoming) = endpoint.accept().await {
        // match evt {
        //     ListeningAddressOpen { local_url } => {
        //         let local_url = match local_url.to_kitsune() {
        //             Ok(local_url) => local_url,
        //             Err(err) => {
        //                 tracing::debug!(?err, "ignoring malformed local url");
        //                 continue;
        //             }
        //         };
        //         handler.new_listening_address(local_url).await;
        //     }
        //     ListeningAddressClosed { local_url: _ } => {
        //         // MAYBE trigger tombstone of our bootstrap entry here
        //     }
        //     Connected { peer_url: _ } => {
        //         // This is handled in our preflight hook,
        //         // we can safely ignore this event.
        //     }
        //     Disconnected { peer_url } => {
        //         let peer_url = match peer_url.to_kitsune() {
        //             Ok(peer_url) => peer_url,
        //             Err(err) => {
        //                 tracing::debug!(?err, "ignoring malformed peer url");
        //                 continue;
        //             }
        //         };
        //         handler.peer_disconnect(peer_url, None);
        //     }
        //     Message { peer_url, message } => {
        //         if let Err(err) =
        //             handle_msg(&handler, peer_url.clone(), message)
        //         {
        //             ep.close(&peer_url);
        //             tracing::debug!(?err);
        //         }
        //     }
        // }
        let endpoint = endpoint.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                tracing::error!("Incoming connection error");
                return;
            };
            // .map_err(|err| K2Error::other("connection error"))?;
            let Ok(mut recv) = connection.accept_uni().await else {
                tracing::error!("Accept uni error");
                return;
            };

            // let node_addr = connection

            let Ok(data) = recv.read_to_end(1_000_000_000).await else {
                tracing::error!("Read to end error");
                return;
            };
            let Ok(node_id) = connection.remote_node_id() else {
                tracing::error!("Remote node id error");
                return;
            };

            let Some(remote_info) = endpoint.remote_info(node_id) else {
                tracing::error!("Remote info error ");
                return;
            };
            let Some(relay_url_info) = remote_info.relay_url else {
                tracing::error!("Remote info error ");
                return;
            };
            let u = format!("{}/{}", relay_url_info.relay_url, node_id);
            let Ok(peer) = Url::from_str(u.as_str()) else {
                tracing::error!("Url from str error");
                return;
            };

            let Ok(()) = handler.recv_data(peer, data.into()) else {
                tracing::error!("recv_data error");
                return;
            };
        });
    }
}

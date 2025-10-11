#![deny(missing_docs)]
//! kitsune2 iroh transport module.

use base64::Engine;
use iroh::{
    endpoint::{Connection, VarInt},
    Endpoint, NodeAddr, NodeId, RelayMap, RelayMode, RelayUrl, Watcher,
};
use kitsune2_api::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc::Receiver, Mutex};

#[allow(missing_docs)]
pub mod config {
    /// Configuration parameters for [IrohTransportFactory](super::IrohTransportFactory).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct IrohTransportConfig {
        pub custom_relay_url: Option<String>,
    }

    impl Default for IrohTransportConfig {
        fn default() -> Self {
            Self {
                custom_relay_url: None,
            }
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

/// Specifies how to integrate kitsune's iroh transport into an existing endpoint.
///
/// Iroh can only really accommodate a single accept loop for a given endpoint.
/// Kitsune needs its own logic for accepting connections, but if you want to run
/// other protocols on the same endpoint (e.g. iroh-blobs), you can define an accept
/// loop externally, and for the kitsune2 ALPN, send the incoming connections
/// to kitsune via the `receiver` here.
pub struct IrohIntegration {
    /// The shared iroh endpoint
    pub endpoint: iroh::Endpoint,
    /// The receiver for incoming connections from the "mother" protocol.
    pub receiver: Receiver<iroh::endpoint::Connection>,
}

enum IrohListener {
    Endpoint(Endpoint),
    Receiver(Receiver<iroh::endpoint::Connection>),
}

#[derive(Clone, Debug)]
struct IrohSender(Endpoint);

pub use config::*;
/// Provides a Kitsune2 transport module based on the iroh crate.
pub struct IrohTransportFactory {
    integration: std::sync::Mutex<Option<IrohIntegration>>,
    initialized: AtomicBool,
}

impl std::fmt::Debug for IrohTransportFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IrohTransportFactory {{ initialized: {:?} }}",
            self.initialized.load(Ordering::Relaxed)
        )
    }
}

impl IrohTransportFactory {
    /// Construct a new IrohTransportFactory.
    pub fn create(integration: Option<IrohIntegration>) -> DynTransportFactory {
        let out: DynTransportFactory = Arc::new(IrohTransportFactory {
            integration: std::sync::Mutex::new(integration),
            initialized: AtomicBool::new(false),
        });
        out
    }
}

impl TransportFactory for IrohTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&IrohTransportModConfig::default())
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let config: IrohTransportModConfig = config.get_module_config()?;

        // make sure our relay server url is parse-able.i
        if let Some(relay_url) = config.iroh_transport.custom_relay_url {
            let _sig = url::Url::parse(relay_url.as_str()).map_err(|err| {
                K2Error::other_src("invalid iroh custom relay url", err)
            })?;
        }

        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let integration = self.integration.lock().unwrap().take();
        if self
            .initialized
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Box::pin(async move {
                Err(K2Error::other(
                    "IrohTransportFactory can only be instantiated once",
                ))
            });
        }

        Box::pin(async move {
            let config: IrohTransportModConfig =
                builder.config.get_module_config()?;

            let handler = TxImpHnd::new(handler);
            let imp = IrohTransport::create(
                config.iroh_transport,
                handler.clone(),
                integration,
            )
            .await?;
            Ok(DefaultTransport::create(&handler, imp))
        })
    }
}

/// The iroh ALPN for kitsune2 iroh transport.
pub const ALPN: &[u8] = b"kitsune2";

#[derive(Debug)]
struct IrohTransport {
    endpoint: IrohSender,
    handler: Arc<TxImpHnd>,
    connections: Arc<Mutex<BTreeMap<NodeAddr, Connection>>>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl Drop for IrohTransport {
    fn drop(&mut self) {
        for task in &mut self.tasks {
            task.abort();
        }
    }
}

impl IrohTransport {
    pub async fn create(
        config: IrohTransportConfig,
        handler: Arc<TxImpHnd>,
        integration: Option<IrohIntegration>,
    ) -> K2Result<DynTxImp> {
        let relay_mode = match config.custom_relay_url {
            Some(relay_url_str) => {
                let relay_url = url::Url::parse(relay_url_str.as_str())
                    .map_err(|err| {
                        K2Error::other(format!(
                            "Failed to parse custom relay url: {err}"
                        ))
                    })?;
                RelayMode::Custom(RelayMap::from(RelayUrl::from(relay_url)))
            }
            None => RelayMode::Default,
        };
        let (sender, listener) = if let Some(integration) = integration {
            (
                IrohSender(integration.endpoint),
                IrohListener::Receiver(integration.receiver),
            )
        } else {
            let endpoint = iroh::Endpoint::builder()
                .discovery_local_network()
                .discovery_n0()
                .relay_mode(relay_mode)
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .map_err(|err| K2Error::other(format!("bad: {err}")))?;

            (
                IrohSender(endpoint.clone()),
                IrohListener::Endpoint(endpoint),
            )
        };
        println!("k2 endpoint node_id: {:?}", sender.0.node_id());

        let h = handler.clone();
        let e = sender.clone();
        // XXX: home_relay removed after iroh 0.90, what to do?
        // let watch_relay_task = tokio::spawn(async move {
        //     loop {
        //         match e.0.home_relay().updated().await {
        //             Ok(new_urls) => {
        //                 for url in new_urls {
        //                     let url =
        //                         to_peer_url(url.clone().into(), e.0.node_id())
        //                             .expect("Invalid URL");

        //                     tracing::info!("New relay URL: {url:?}");

        //                     h.new_listening_address(url).await
        //                 }
        //             }
        //             Err(err) => {
        //                 tracing::error!(
        //                     "Failed to get new relay url: {err:?}."
        //                 );
        //             }
        //         }
        //     }
        // })
        // .abort_handle();

        let evt_task = tokio::task::spawn(evt_task(
            handler.clone(),
            sender.clone(),
            listener,
        ))
        .abort_handle();

        let out: DynTxImp = Arc::new(Self {
            handler,
            endpoint: sender,
            connections: Arc::new(Mutex::new(BTreeMap::new())),
            tasks: vec![evt_task],
            // tasks: vec![watch_relay_task, evt_task],
        });

        Ok(out)
    }
}

/// Convert a kitsune2 peer Url to an iroh NodeAddr.
pub fn peer_url_to_node_addr(peer_url: Url) -> Result<NodeAddr, K2Error> {
    // println!("peer_url_to_node_addr: peer_url: {:?}", peer_url);
    let url = url::Url::parse(peer_url.as_str()).map_err(|err| {
        K2Error::other(format!("Failed to parse peer url: {err:?}"))
    })?;
    let Some(peer_id) = peer_url.peer_id() else {
        return Err(K2Error::other("empty peer url"));
    };
    let decoded_peer_id = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(peer_id)
        .map_err(|err| {
            K2Error::other(format!("failed to decode peer id: {err}"))
        })?;
    let node_id = NodeId::try_from(decoded_peer_id.as_slice())
        .map_err(|err| K2Error::other(format!("bad peer id: {err}")))?;

    let relay_url = url::Url::parse(
        format!("{}://{}", url.scheme(), peer_url.addr()).as_str(),
    )
    .map_err(|err| K2Error::other(format!("Bad addr: {err}")))?;

    let r = NodeAddr {
        node_id,
        relay_url: Some(RelayUrl::from(relay_url)),
        direct_addresses: BTreeSet::new(),
    };

    // println!("peer_url_to_node_addr: r: {:?}", r);

    Ok(r)
}

fn to_peer_url(url: url::Url, node_id: NodeId) -> Result<Url, K2Error> {
    let port = url.port().unwrap_or(443);

    let mut url_str = url.to_string();
    if let Some(s) = url_str.strip_suffix("./") {
        url_str = s.to_string();
    }
    let u = format!(
        "{}:{port}/{}",
        url_str,
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(node_id)
    );
    Url::from_str(u.as_str())
}

fn node_addr_to_peer_url(node_addr: NodeAddr) -> Result<Url, K2Error> {
    // println!("node_addr_to_peer_url: node_addr: {:?}", node_addr);
    let r = match node_addr.relay_url {
        Some(relay_url) => to_peer_url(relay_url.into(), node_addr.node_id),
        None => {
            let Some(direct_address) = node_addr
                .direct_addresses
                .into_iter()
                .collect::<Vec<SocketAddr>>()
                .first()
                .cloned()
            else {
                return Err(K2Error::other(
                    "node addr has no relay url and no direct addresses",
                ));
            };
            let Ok(url) =
                url::Url::parse(format!("http://{}", direct_address).as_str())
            else {
                return Err(K2Error::other("Failed to parse direct addresses"));
            };
            to_peer_url(url, node_addr.node_id)
        }
    };

    // println!("node_addr_to_peer_url: r: {:?}", r);
    r
}

impl TxImp for IrohTransport {
    fn url(&self) -> Option<Url> {
        // XXX: home_relay removed after iroh 0.90, what to do?
        // let home_relay = self.endpoint.0.home_relay().get();
        // let Ok(urls) = home_relay else {
        //     tracing::error!("Failed to get home relay");
        //     return None;
        // };
        // let Some(url) = urls.first() else {
        //     tracing::error!("Failed to get home relay");
        //     return None;
        // };
        // Some(
        //     to_peer_url(url.clone().into(), self.endpoint.0.node_id())
        //         .expect("Invalid URL"),
        // )
        None
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
                connection.close(VarInt::from_u32(0), b"disconnected");
                connections.remove(&addr);
            }
            ()
        })
    }

    fn send(&self, peer: Url, data: bytes::Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            let addr = peer_url_to_node_addr(peer.clone()).map_err(|err| {
                K2Error::other(format!("bad peer url: {:?}", err))
            })?;

            let connection_result =
                self.endpoint.0.connect(addr.clone(), ALPN).await;

            let connection = match connection_result {
                Ok(c) => c,
                Err(err) => {
                    tracing::warn!(
                        "connect() failed: marking {peer} as unresponsive."
                    );
                    self.handler
                        .set_unresponsive(peer, Timestamp::now())
                        .await?;

                    return Err(K2Error::other(format!(
                        "failed to connect: {err:?}"
                    )));
                }
            };
            // let mut connections = self.connections.lock().await;
            // if !connections.contains_key(&addr) {
            //     connections.insert(addr.clone(), connection.clone());
            // }
            // drop(connections);

            // let Some(connection) = connections.get(&addr) else {
            //     return Err(K2Error::other("no connection with peer"));
            // };

            let mut send = match connection.open_uni().await {
                Ok(s) => s,

                Err(err) => {
                    tracing::warn!(
                        "open_uni() failed: Marking {peer} as unresponsive."
                    );
                    self.handler
                        .set_unresponsive(peer, Timestamp::now())
                        .await?;

                    return Err(K2Error::other(format!(
                        "failed to open uni: {err:?}"
                    )));
                }
            };

            send.write_all(data.as_ref()).await.map_err(|err| {
                K2Error::other(format!("Failed to write all: {err:?}"))
            })?;
            send.finish().map_err(|err| {
                K2Error::other(format!("Failed to close stream: {err:?}"))
            })?;
            connection.closed().await;
            Ok(())
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
                        node_addr_to_peer_url(node_addr.clone()).ok()
                    })
                    .collect(),
                connections: connections
                    .iter()
                    .map(|(peer_addr, _conn)| TransportConnectionStats {
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

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        Box::pin(async move {
            let connections = self.connections.lock().await;
            Ok(connections
                .keys()
                .map(|addr| node_addr_to_peer_url(addr.clone()).unwrap())
                .collect())
        })
    }
}

async fn evt_task(
    handler: Arc<TxImpHnd>,
    sender: IrohSender,
    mut listener: IrohListener,
) {
    while let Some(connection) = match &mut listener {
        IrohListener::Endpoint(endpoint) => {
            endpoint.accept().await.expect("TODO").await.ok()
        }
        IrohListener::Receiver(receiver) => receiver.recv().await,
    } {
        let ep = sender.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            let mut recv = match connection.accept_uni().await {
                Ok(r) => r,
                Err(err) => {
                    tracing::error!("Accept uni error: {err:?}.");
                    return;
                }
            };

            let Ok(data) = recv.read_to_end(1_000_000_000).await else {
                tracing::error!("Read to end error");
                return;
            };
            let Ok(node_id) = connection.remote_node_id() else {
                tracing::error!("Remote node id error");
                return;
            };

            // XXX: removed in iroh 0.90, what do to?
            // let Some(remote_info) = ep.0.remote_info(node_id) else {
            //     tracing::error!("Remote info error ");
            //     return;
            // };
            // let Some(relay_url_info) = remote_info.relay_url else {
            //     tracing::error!("Remote info error ");
            //     return;
            // };

            // let Ok(peer) =
            //     to_peer_url(relay_url_info.relay_url.into(), node_id)
            // else {
            //     tracing::error!("Url from str error");
            //     return;
            // };

            // let Ok(()) = handler.recv_data(peer, data.into()) else {
            //     tracing::error!("recv_data error");
            //     return;
            // };
            connection.close(VarInt::from_u32(0), b"ended");
        });
    }
}

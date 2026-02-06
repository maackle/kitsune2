use iroh::Endpoint;

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
    pub receiver: tokio::sync::mpsc::Receiver<iroh::endpoint::Connection>,
}

enum IrohListener {
    Endpoint(Endpoint),
    Receiver(tokio::sync::mpsc::Receiver<iroh::endpoint::Connection>),
}

#[derive(Clone, Debug)]
struct IrohSender(Endpoint);

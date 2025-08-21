use kitsune2_api::*;
use message_handler::FetchMessageHandler;
use std::collections::HashMap;
use std::sync::MutexGuard;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

mod message_handler;

#[cfg(test)]
mod test;

/// CoreFetch module name.
pub const MOD_NAME: &str = "Fetch";

/// CoreFetch configuration types.
mod config {
    /// Configuration parameters for [CoreFetchFactory](super::CoreFetchFactory).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreFetchConfig {
        /// How many parallel op fetch requests can be made at once.
        ///
        /// Default: 2.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub parallel_request_count: u8,
        /// Delay before re-inserting ops to request back into the outgoing request queue.
        ///
        /// Default: 30 s.
        #[cfg_attr(feature = "schema", schemars(default))]
        pub re_insert_outgoing_request_delay_ms: u32,
    }

    impl Default for CoreFetchConfig {
        // Maximum back off is 11:40 min.
        fn default() -> Self {
            Self {
                parallel_request_count: 2,
                re_insert_outgoing_request_delay_ms: 30000,
            }
        }
    }

    /// Module-level configuration for CoreFetch.
    #[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct CoreFetchModConfig {
        /// CoreFetch configuration.
        pub core_fetch: CoreFetchConfig,
    }
}

pub use config::*;

/// A production-ready fetch module.
#[derive(Debug)]
pub struct CoreFetchFactory {}

impl CoreFetchFactory {
    /// Construct a new CoreFetchFactory.
    pub fn create() -> DynFetchFactory {
        Arc::new(Self {})
    }
}

impl FetchFactory for CoreFetchFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        config.set_module_config(&CoreFetchModConfig::default())?;
        Ok(())
    }

    fn validate_config(&self, _config: &Config) -> K2Result<()> {
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        space_id: SpaceId,
        op_store: DynOpStore,
        peer_meta_store: DynPeerMetaStore,
        transport: DynTransport,
    ) -> BoxFut<'static, K2Result<DynFetch>> {
        Box::pin(async move {
            let config: CoreFetchModConfig =
                builder.config.get_module_config()?;
            let out: DynFetch = Arc::new(CoreFetch::new(
                config.core_fetch,
                space_id,
                op_store,
                peer_meta_store,
                transport,
            ));
            Ok(out)
        })
    }
}

type OutgoingRequest = (OpId, Url);
type IncomingRequest = (Vec<OpId>, Url);
type IncomingResponse = Vec<Op>;

#[derive(Debug)]
struct State {
    requests: HashSet<OutgoingRequest>,
    notify_when_drained_senders: Vec<futures::channel::oneshot::Sender<()>>,
}

impl State {
    fn summary(&self) -> FetchStateSummary {
        FetchStateSummary {
            pending_requests: self.requests.iter().fold(
                HashMap::new(),
                |mut acc, (op_id, peer_url)| {
                    acc.entry(op_id.clone())
                        .or_default()
                        .push(peer_url.clone());
                    acc
                },
            ),
        }
    }
}

#[derive(Debug)]
struct CoreFetch {
    state: Arc<Mutex<State>>,
    outgoing_request_tx: Sender<OutgoingRequest>,
    tasks: Vec<JoinHandle<()>>,
    op_store: DynOpStore,
    #[cfg(test)]
    message_handler: DynTxModuleHandler,
}

impl CoreFetch {
    fn new(
        config: CoreFetchConfig,
        space_id: SpaceId,
        op_store: DynOpStore,
        peer_meta_store: DynPeerMetaStore,
        transport: DynTransport,
    ) -> Self {
        Self::spawn_tasks(
            config,
            space_id,
            op_store,
            peer_meta_store,
            transport,
        )
    }
}

impl Fetch for CoreFetch {
    fn request_ops(
        &self,
        op_ids: Vec<OpId>,
        source: Url,
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            // Filter out requests for ops that are already in the op store.
            let new_op_ids =
                self.op_store.filter_out_existing_ops(op_ids).await?;

            // Add requests to set.
            {
                let requests = &mut self.state.lock().unwrap().requests;
                requests.extend(
                    new_op_ids
                        .clone()
                        .into_iter()
                        .map(|op_id| (op_id.clone(), source.clone())),
                );
            }
            // Insert requests into fetch queue.
            for op_id in new_op_ids {
                if let Err(err) =
                    self.outgoing_request_tx.send((op_id, source.clone())).await
                {
                    tracing::warn!(
                        "could not insert fetch request into fetch queue: {err}"
                    );
                }
            }

            Ok(())
        })
    }

    fn notify_on_drained(&self, notify: futures::channel::oneshot::Sender<()>) {
        let mut lock = self.state.lock().expect("poisoned");
        if lock.requests.is_empty() {
            if let Err(err) = notify.send(()) {
                tracing::warn!(?err, "Failed to send notification on drained");
            }
        } else {
            lock.notify_when_drained_senders.push(notify);
        }
    }

    fn get_state_summary(&self) -> BoxFut<'_, K2Result<FetchStateSummary>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().summary()) })
    }
}

impl CoreFetch {
    pub fn spawn_tasks(
        config: CoreFetchConfig,
        space_id: SpaceId,
        op_store: DynOpStore,
        peer_meta_store: DynPeerMetaStore,
        transport: DynTransport,
    ) -> Self {
        // Create a queue to process outgoing op requests. Requests are sent to peers.
        let (outgoing_request_tx, outgoing_request_rx) =
            channel::<OutgoingRequest>(16_384);
        let outgoing_request_rx =
            Arc::new(tokio::sync::Mutex::new(outgoing_request_rx));

        // Create a queue to process incoming op requests. Requested ops are retrieved from the
        // store and returned to the requester.
        let (incoming_request_tx, incoming_request_rx) =
            channel::<IncomingRequest>(16_384);

        // Create a queue to process incoming op responses. Ops are passed to the op store and op
        // ids removed from the set of ops to fetch.
        let (incoming_response_tx, incoming_response_rx) =
            channel::<IncomingResponse>(16_384);

        let state = Arc::new(Mutex::new(State {
            requests: HashSet::new(),
            notify_when_drained_senders: vec![],
        }));

        let mut tasks =
            Vec::with_capacity(config.parallel_request_count as usize);
        // Spawn request tasks.
        for _ in 0..config.parallel_request_count {
            let request_task =
                tokio::task::spawn(CoreFetch::outgoing_request_task(
                    state.clone(),
                    outgoing_request_tx.clone(),
                    outgoing_request_rx.clone(),
                    space_id.clone(),
                    peer_meta_store.clone(),
                    Arc::downgrade(&transport),
                    config.re_insert_outgoing_request_delay_ms,
                ));
            tasks.push(request_task);
        }

        // Spawn incoming request task.
        let incoming_request_task =
            tokio::task::spawn(CoreFetch::incoming_request_task(
                incoming_request_rx,
                op_store.clone(),
                Arc::downgrade(&transport),
                space_id.clone(),
            ));
        tasks.push(incoming_request_task);

        // Spawn incoming response task.
        let incoming_response_task =
            tokio::task::spawn(CoreFetch::incoming_response_task(
                incoming_response_rx,
                op_store.clone(),
                state.clone(),
            ));
        tasks.push(incoming_response_task);

        // Register transport module handler for incoming op requests and responses.
        let message_handler = Arc::new(FetchMessageHandler {
            incoming_request_tx,
            incoming_response_tx,
        });
        transport.register_module_handler(
            space_id.clone(),
            MOD_NAME.to_string(),
            message_handler.clone(),
        );

        Self {
            state,
            outgoing_request_tx,
            tasks,
            op_store,
            #[cfg(test)]
            message_handler,
        }
    }

    async fn outgoing_request_task(
        state: Arc<Mutex<State>>,
        outgoing_request_tx: Sender<OutgoingRequest>,
        outgoing_request_rx: Arc<tokio::sync::Mutex<Receiver<OutgoingRequest>>>,
        space_id: SpaceId,
        peer_meta_store: DynPeerMetaStore,
        transport: WeakDynTransport,
        re_insert_outgoing_request_delay: u32,
    ) {
        while let Some((op_id, peer_url)) =
            outgoing_request_rx.lock().await.recv().await
        {
            tracing::debug!(?op_id, ?peer_url, "processing outgoing request");
            let Some(transport) = transport.upgrade() else {
                tracing::info!(
                    "Transport dropped, stopping outgoing request task"
                );
                break;
            };

            // If peer URL is set as unresponsive, remove current request from state.
            let peer_url_unresponsive = match peer_meta_store
                .get_unresponsive(peer_url.clone())
                .await
            {
                Ok(maybe_value) => maybe_value.is_some(),
                Err(err) => {
                    tracing::warn!(?err, "could not query peer meta store");
                    false
                }
            };
            if peer_url_unresponsive {
                state
                    .lock()
                    .expect("poisoned")
                    .requests
                    .remove(&(op_id.clone(), peer_url.clone()));
            }

            // Do nothing if op id is no longer in the set of requests to send.
            //
            // If the peer URL is unresponsive, the current request will have been removed
            // from state and no request will be sent and the request
            // will not be re-inserted into the queue.
            {
                let lock = state.lock().expect("poisoned");
                if !lock.requests.contains(&(op_id.clone(), peer_url.clone())) {
                    // Check if the fetch queue is drained and notify listeners.
                    Self::notify_listeners_if_queue_drained(lock);

                    continue;
                }
            }

            tracing::debug!(
                ?peer_url,
                ?space_id,
                ?op_id,
                "sending fetch request"
            );

            // Send fetch request to peer.
            let data = serialize_request_message(vec![op_id.clone()]);
            if let Err(err) = transport
                .send_module(
                    peer_url.clone(),
                    space_id.clone(),
                    MOD_NAME.to_string(),
                    data,
                )
                .await
            {
                tracing::warn!(
                    ?op_id,
                    ?peer_url,
                    "could not send fetch request: {err}."
                );
                state
                    .lock()
                    .expect("poisoned")
                    .requests
                    .retain(|(_, a)| *a != peer_url);
            }

            // After processing this request, check if the fetch queue is drained.
            //
            // Note that using flow control above could skip this step, so please only `continue`
            // if it is safe to do so.
            {
                let mut lock = state.lock().expect("poisoned");
                if lock.requests.is_empty() {
                    // Notify all listeners that the fetch queue is drained.
                    for notify in lock.notify_when_drained_senders.drain(..) {
                        if notify.send(()).is_err() {
                            tracing::warn!(
                                "Failed to send notification on drained"
                            );
                        }
                    }
                }
            }

            // Re-insert the fetch request into the queue after a delay.
            let outgoing_request_tx = outgoing_request_tx.clone();

            tokio::task::spawn({
                let state = state.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(
                        re_insert_outgoing_request_delay as u64,
                    ))
                    .await;
                    let mut lock = state.lock().expect("poisoned");
                    // Only re-insert the request if it is still in the state, meaning that it
                    // has not been removed from state because the requested op has come in or
                    // the peer URL has been set as unresponsive.
                    if lock
                        .requests
                        .contains(&(op_id.clone(), peer_url.clone()))
                    {
                        if let Err(err) = outgoing_request_tx
                            .try_send((op_id.clone(), peer_url.clone()))
                        {
                            tracing::warn!(
                                "could not re-insert fetch request for op {op_id} to peer {peer_url} into queue: {err}"
                            );
                            // Remove request from set to prevent build-up of state.
                            lock.requests.remove(&(op_id, peer_url));

                            Self::notify_listeners_if_queue_drained(lock);
                        }
                    }
                }
            });
        }
    }

    fn notify_listeners_if_queue_drained(mut state: MutexGuard<State>) {
        // Check if the fetch queue is drained.
        if state.requests.is_empty() {
            // Notify all listeners that the fetch queue is drained.
            for notify in state.notify_when_drained_senders.drain(..) {
                if notify.send(()).is_err() {
                    tracing::warn!("Failed to send notification on drained");
                }
            }
        }
    }

    async fn incoming_request_task(
        mut response_rx: Receiver<IncomingRequest>,
        op_store: DynOpStore,
        transport: WeakDynTransport,
        space_id: SpaceId,
    ) {
        while let Some((op_ids, peer)) = response_rx.recv().await {
            tracing::debug!(?peer, ?op_ids, "incoming request");

            let Some(transport) = transport.upgrade() else {
                tracing::info!(
                    "Transport dropped, stopping incoming request task"
                );
                break;
            };

            // Retrieve ops to send from store.
            let ops = match op_store.retrieve_ops(op_ids.clone()).await {
                Err(err) => {
                    tracing::error!("could not read ops from store: {err}");
                    continue;
                }
                Ok(ops) => {
                    ops.into_iter().map(|op| op.op_data).collect::<Vec<_>>()
                }
            };

            if ops.is_empty() {
                tracing::info!(
                    "none of the ops requested from {peer} found in store"
                );
                // Do not send a response when no ops could be retrieved.
                continue;
            }

            let data = serialize_response_message(ops);
            if let Err(err) = transport
                .send_module(
                    peer.clone(),
                    space_id.clone(),
                    MOD_NAME.to_string(),
                    data,
                )
                .await
            {
                tracing::warn!(
                    ?op_ids,
                    ?peer,
                    "could not send ops to requesting peer: {err}"
                );
            }
        }
    }

    async fn incoming_response_task(
        mut incoming_response_rx: Receiver<IncomingResponse>,
        op_store: DynOpStore,
        state: Arc<Mutex<State>>,
    ) {
        while let Some(ops) = incoming_response_rx.recv().await {
            let op_count = ops.len();
            tracing::debug!(?op_count, "incoming op response");
            let ops_data = ops.clone().into_iter().map(|op| op.data).collect();
            match op_store.process_incoming_ops(ops_data).await {
                Err(err) => {
                    tracing::error!("could not process incoming ops: {err}");
                    // Ops could not be written to the op store. Their ids remain in the set of ops
                    // to fetch.
                    continue;
                }
                Ok(processed_op_ids) => {
                    tracing::debug!(
                        "processed incoming ops with op ids {processed_op_ids:?}"
                    );
                    // Ops were processed successfully by op store. Op ids are returned.
                    // The op ids are removed from the set of ops to fetch.
                    let mut lock = state.lock().unwrap();
                    lock.requests
                        .retain(|(op_id, _)| !processed_op_ids.contains(op_id));
                }
            }
        }
    }
}

impl Drop for CoreFetch {
    fn drop(&mut self) {
        for t in self.tasks.iter() {
            t.abort();
        }
    }
}

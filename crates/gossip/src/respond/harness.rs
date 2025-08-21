use crate::burst::AcceptBurstTracker;
use crate::gossip::K2Gossip;
use crate::peer_meta_store::K2PeerMetaStore;
use crate::protocol::{deserialize_gossip_message, GossipMessage};
use crate::state::GossipRoundState;
use crate::{K2GossipConfig, MOD_NAME};
use base64::Engine;
use bytes::Bytes;
use kitsune2_api::*;
use kitsune2_core::{default_test_builder, Ed25519LocalAgent};
use kitsune2_dht::{ArcSet, Dht};
use kitsune2_test_utils::agent::AgentBuilder;
use kitsune2_test_utils::space::TEST_SPACE_ID;
use rand::RngCore;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub(crate) struct RespondTestHarness {
    pub(crate) gossip: K2Gossip,
    pub(crate) rx: tokio::sync::mpsc::Receiver<(Url, Bytes)>,
    pub(crate) _transport: DynTransport,
}

impl RespondTestHarness {
    pub(crate) async fn create() -> Self {
        RespondTestHarness::create_with_config(Default::default()).await
    }

    pub(crate) async fn create_with_config(config: K2GossipConfig) -> Self {
        let builder =
            Arc::new(default_test_builder().with_default_config().unwrap());

        let op_store = builder
            .op_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();

        let dht = Dht::try_from_store(Timestamp::now(), op_store.clone())
            .await
            .unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut transport = MockTransport::new();
        transport.expect_send_module().returning(
            move |peer, space_id, module_id, data| {
                assert_eq!(space_id, TEST_SPACE_ID);
                assert_eq!(module_id.as_str(), MOD_NAME);

                let tx = tx.clone();
                Box::pin(async move {
                    tx.send((peer, data)).await.unwrap();
                    Ok(())
                })
            },
        );
        transport
            .expect_register_module_handler()
            .returning(|_, _, _| {});
        let transport: DynTransport = Arc::new(transport);
        let peer_meta_store = builder
            .peer_meta_store
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();
        let blocks = builder
            .blocks
            .create(builder.clone(), TEST_SPACE_ID)
            .await
            .unwrap();

        let config = Arc::new(config);
        Self {
            gossip: K2Gossip {
                config: config.clone(),
                initiated_round_state: Default::default(),
                accepted_round_states: Default::default(),
                dht: Arc::new(RwLock::new(dht)),
                space_id: TEST_SPACE_ID,
                peer_store: builder
                    .peer_store
                    .create(builder.clone(), TEST_SPACE_ID, blocks)
                    .await
                    .unwrap(),
                local_agent_store: builder
                    .local_agent_store
                    .create(builder.clone())
                    .await
                    .unwrap(),
                peer_meta_store: Arc::new(K2PeerMetaStore::new(
                    peer_meta_store.clone(),
                )),
                op_store: op_store.clone(),
                fetch: builder
                    .fetch
                    .create(
                        builder.clone(),
                        TEST_SPACE_ID,
                        op_store.clone(),
                        peer_meta_store.clone(),
                        transport.clone(),
                    )
                    .await
                    .unwrap(),
                agent_verifier: builder.verifier.clone(),
                transport: Arc::downgrade(&transport),
                burst: AcceptBurstTracker::new(config),
                _initiate_task: Default::default(),
                _timeout_task: Default::default(),
                _dht_update_task: Default::default(),
            },
            rx,
            _transport: transport,
        }
    }

    pub(crate) async fn create_agent(
        &self,
        tgt_storage_arc: DhtArc,
    ) -> TestAgent {
        let local_agent = Ed25519LocalAgent::default();
        local_agent.set_tgt_storage_arc_hint(tgt_storage_arc);

        let builder = AgentBuilder::default().with_url(Some(
            Url::from_str(format!(
                "ws://test:80/{}",
                base64::prelude::BASE64_URL_SAFE
                    .encode(local_agent.agent().0.as_ref())
            ))
            .unwrap(),
        ));

        let local: DynLocalAgent = Arc::new(local_agent);
        TestAgent {
            local: local.clone(),
            agent_info: builder.build(local),
        }
    }

    pub(crate) async fn insert_initiated_round_state(
        &self,
        local_agent: &TestAgent,
        with_remote_agent: &TestAgent,
    ) -> Bytes {
        let mut round_state = self.gossip.initiated_round_state.lock().await;
        assert!(round_state.is_none());
        let state = GossipRoundState::new(
            with_remote_agent.url.clone().unwrap(),
            vec![local_agent.agent.clone()],
            ArcSet::new(vec![DhtArc::FULL]).unwrap(),
        );
        let session_id = state.session_id.clone();
        *round_state = Some(state);
        session_id
    }

    pub(crate) async fn insert_accepted_round_state(
        &self,
        local_agent: &TestAgent,
        with_remote_agent: &TestAgent,
    ) -> Bytes {
        let mut accepted = self.gossip.accepted_round_states.write().await;
        assert!(!accepted.contains_key(with_remote_agent.url.as_ref().unwrap()));
        let state = GossipRoundState::new(
            with_remote_agent.url.clone().unwrap(),
            vec![local_agent.agent.clone()],
            ArcSet::new(vec![DhtArc::FULL]).unwrap(),
        );
        let session_id = state.session_id.clone();
        accepted.insert(
            with_remote_agent.url.clone().unwrap(),
            Arc::new(Mutex::new(state)),
        );
        session_id
    }

    pub(crate) async fn wait_for_sent_response(&mut self) -> GossipMessage {
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();

        deserialize_gossip_message(received.1).unwrap()
    }
}

#[derive(Debug)]
pub struct TestAgent {
    pub local: DynLocalAgent,
    pub agent_info: Arc<AgentInfoSigned>,
}

impl Deref for TestAgent {
    type Target = Arc<AgentInfoSigned>;

    fn deref(&self) -> &Self::Target {
        &self.agent_info
    }
}

pub(crate) fn test_session_id() -> Bytes {
    let mut session_id = bytes::BytesMut::zeroed(12);
    rand::thread_rng().fill_bytes(&mut session_id);

    session_id.freeze()
}

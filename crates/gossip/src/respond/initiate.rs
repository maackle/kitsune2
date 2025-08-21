use crate::error::{K2GossipError, K2GossipResult};
use crate::gossip::K2Gossip;
use crate::protocol::k2_gossip_accept_message::SnapshotMinimalMessage;
use crate::protocol::{
    encode_agent_ids, encode_op_ids, ArcSetMessage, GossipMessage,
    K2GossipAcceptMessage, K2GossipBusyMessage, K2GossipInitiateMessage,
};
use crate::state::RoundStage;
use kitsune2_api::{K2Error, Timestamp, Url};
use kitsune2_dht::ArcSet;

impl K2Gossip {
    pub(super) async fn respond_to_initiate(
        &self,
        from_peer: Url,
        initiate: K2GossipInitiateMessage,
    ) -> K2GossipResult<Option<GossipMessage>> {
        // Rate limit incoming gossip messages by peer
        if !self
            .check_peer_initiate_rate(from_peer.clone(), &initiate)
            .await?
        {
            tracing::info!(?initiate.session_id, "Dropping initiate from {:?}", from_peer);
            return Ok(None);
        }

        // If we've already accepted the maximum number of rounds, we can't accept another.
        // Send back a busy message to let the peer know.
        if self.config.max_concurrent_accepted_rounds != 0
            && self.accepted_round_states.read().await.len()
                >= self.config.max_concurrent_accepted_rounds as usize
        {
            tracing::debug!(?initiate.session_id, "Busy, refusing initiate from {:?}", from_peer);
            return Ok(Some(GossipMessage::Busy(K2GossipBusyMessage {
                session_id: initiate.session_id,
            })));
        }

        // Note the gap between the check and write here. It's possible that both peers
        // could initiate at the same time. This is slightly wasteful but shouldn't be a
        // problem.
        self.peer_meta_store
            .set_last_gossip_timestamp(from_peer.clone(), Timestamp::now())
            .await?;

        let other_arc_set = match &initiate.arc_set {
            Some(message) => ArcSet::decode(&message.value)?,
            None => {
                return Err(
                    K2Error::other("no arc set in initiate message").into()
                );
            }
        };

        let (our_agents, our_arc_set) = self.local_agent_state().await?;
        let common_arc_set = our_arc_set.intersection(&other_arc_set);

        // There's no validation to be done with an accept beyond what's been done above
        // to check how recently this peer initiated with us. We'll just record that they
        // have initiated and that we plan to accept.
        let mut state = self
            .create_accept_state(
                self.config.clone(),
                &from_peer,
                &initiate,
                our_agents.clone(),
                common_arc_set.clone(),
            )
            .await?;

        // Now we can start the work of creating an accept response, starting with a
        // minimal DHT snapshot if there is an arc set overlap.
        let snapshot: Option<SnapshotMinimalMessage> =
            if common_arc_set.covered_sector_count() > 0 {
                let snapshot = self
                    .dht
                    .read()
                    .await
                    .snapshot_minimal(common_arc_set.clone())
                    .await?;
                Some(snapshot.try_into()?)
            } else {
                // TODO Need to decide what to do here. It's useful for now and it's reasonable
                //      to need to initiate to discover this but we do want to minimize work
                //      in this case.
                tracing::info!(
                    ?initiate.session_id,
                    "no common arc set, continue to sync agents but not ops"
                );
                None
            };

        let missing_agents = self
            .filter_known_agents(&initiate.participating_agents)
            .await?;

        let new_since = self.get_request_new_since(from_peer.clone()).await?;

        let (new_ops, used_bytes, new_bookmark) = self
            .retrieve_new_op_ids(
                &common_arc_set,
                Timestamp::from_micros(initiate.new_since),
                initiate.max_op_data_bytes,
            )
            .await?;

        // Update the peer's max op data bytes to reflect the amount of data we're sending ids for.
        // The remaining limit will be used for the DHT diff as required.
        tracing::debug!(
            ?initiate.session_id,
            "Used {}/{} op budget to send {} op ids",
            used_bytes,
            initiate.max_op_data_bytes,
            new_ops.len()
        );
        state.peer_max_op_data_bytes -= used_bytes as i32;

        Ok(Some(GossipMessage::Accept(K2GossipAcceptMessage {
            session_id: initiate.session_id,
            participating_agents: encode_agent_ids(our_agents),
            arc_set: Some(ArcSetMessage {
                value: our_arc_set.encode(),
            }),
            missing_agents,
            new_since: new_since.as_micros(),
            max_op_data_bytes: self.config.max_gossip_op_bytes,
            new_ops: encode_op_ids(new_ops),
            updated_new_since: new_bookmark.as_micros(),
            snapshot,
        })))
    }

    async fn check_peer_initiate_rate(
        &self,
        from_peer: Url,
        initiate: &K2GossipInitiateMessage,
    ) -> K2GossipResult<bool> {
        let mut initiate_lock = self.initiated_round_state.lock().await;
        if let Some(initiated) = initiate_lock.as_ref() {
            // We've initiated with this peer, and we're now receiving an initiate message from them
            if initiated.session_with_peer == from_peer {
                return match &initiated.stage {
                    RoundStage::Initiated(i) => {
                        if i.tie_breaker > initiate.tie_breaker {
                            // We win, our initiation should be accepted by the peer, and we can drop this incoming message.
                            Ok(false)
                        } else {
                            // We lose, the other peer's initiation should be accepted, and we should drop our own initiation.
                            *initiate_lock = None;
                            Ok(true)
                        }
                    }
                    _ => {
                        // This would be odd. We've made it past the initial message exchange and
                        // the peer is trying to initiate again. That definitely wouldn't be
                        // following the protocol so treat this as a peer behaviour error
                        Err(K2GossipError::peer_behavior(
                            "Attempted to initiate during a round",
                        ))
                    }
                };
            }
        }

        if !self.burst.check_accept(&from_peer, Timestamp::now()) {
            return Err(K2GossipError::peer_behavior("initiate too soon"));
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::burst::AcceptBurstTracker;
    use crate::protocol::K2GossipTerminateMessage;
    use crate::respond::harness::{test_session_id, RespondTestHarness};
    use crate::state::GossipRoundState;
    use crate::K2GossipConfig;
    use kitsune2_api::{
        decode_ids, DhtArc, Gossip, LocalAgent, OpId, UNIX_TIMESTAMP,
    };
    use kitsune2_core::factories::MemoryOp;
    use kitsune2_core::Ed25519LocalAgent;
    use kitsune2_dht::SECTOR_SIZE;
    use kitsune2_test_utils::enable_tracing;
    use rand::RngCore;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn creates_accepted_state() {
        let harness = RespondTestHarness::create().await;

        // Add two local agents, whose arcs we will combine.
        let local_agent_1 = Ed25519LocalAgent::default();
        local_agent_1.set_tgt_storage_arc_hint(DhtArc::Arc(
            7 * SECTOR_SIZE,
            12 * SECTOR_SIZE - 1,
        ));
        let local_agent_1 = Arc::new(local_agent_1);
        harness
            .gossip
            .local_agent_store
            .add(local_agent_1.clone())
            .await
            .unwrap();

        let local_agent_2 = Ed25519LocalAgent::default();
        local_agent_2.set_tgt_storage_arc_hint(DhtArc::Arc(
            10 * SECTOR_SIZE,
            15 * SECTOR_SIZE - 1,
        ));
        let local_agent_2 = Arc::new(local_agent_2);
        harness
            .gossip
            .local_agent_store
            .add(local_agent_2.clone())
            .await
            .unwrap();

        // Provide a remote agent who we don't need to know anything about.
        let remote_agent = harness.create_agent(DhtArc::Empty).await;

        let response = harness
            .gossip
            .respond_to_initiate(
                remote_agent.url.clone().unwrap(),
                K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: encode_agent_ids([remote_agent
                        .agent
                        .clone()]),
                    arc_set: Some(ArcSetMessage {
                        value: ArcSet::new(vec![DhtArc::Arc(
                            11 * SECTOR_SIZE,
                            20 * SECTOR_SIZE - 1,
                        )])
                        .unwrap()
                        .encode(),
                    }),
                    tie_breaker: 0,
                    new_since: Timestamp::now().as_micros(),
                    max_op_data_bytes: 5_000,
                },
            )
            .await
            .unwrap();

        match response {
            Some(GossipMessage::Accept(accept)) => {
                let accept_arc_set =
                    ArcSet::decode(&accept.arc_set.unwrap().value).unwrap();

                assert_eq!(
                    ArcSet::new(vec![DhtArc::Arc(
                        7 * SECTOR_SIZE,
                        15 * SECTOR_SIZE - 1
                    )])
                    .unwrap(),
                    accept_arc_set
                );
            }
            other => {
                panic!("Unexpected response: {:?}", other);
            }
        };

        let accepted_lock = harness.gossip.accepted_round_states.read().await;
        let accepted = accepted_lock.get(&remote_agent.url.clone().unwrap());
        assert!(accepted.is_some());

        let accepted = accepted.unwrap().lock().await;
        assert_eq!(5_000, accepted.peer_max_op_data_bytes);
        assert_eq!(
            remote_agent.url.clone().unwrap(),
            accepted.session_with_peer
        );

        match &accepted.stage {
            RoundStage::Accepted(accepted) => {
                let mut expected_agents = vec![
                    local_agent_1.agent().clone(),
                    local_agent_2.agent().clone(),
                ];
                expected_agents.sort();

                let mut our_agents = accepted.our_agents.clone();
                our_agents.sort();
                assert_eq!(expected_agents, our_agents);

                assert_eq!(
                    ArcSet::new(vec![DhtArc::Arc(
                        11 * SECTOR_SIZE,
                        15 * SECTOR_SIZE - 1
                    )])
                    .unwrap(),
                    accepted.common_arc_set
                );
            }
            other => {
                panic!("Unexpected round stage: {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn initiate_while_busy() {
        let mut harness = RespondTestHarness::create().await;

        // Fill up our accepted round states.
        for i in 0..harness.gossip.config.max_concurrent_accepted_rounds {
            let url =
                Url::from_str(format!("ws://test-host:80/init-{}", i)).unwrap();
            harness.gossip.accepted_round_states.write().await.insert(
                url.clone(),
                Arc::new(Mutex::new(GossipRoundState::new_accepted(
                    url,
                    test_session_id(),
                    500,
                    vec![],
                    ArcSet::new(vec![DhtArc::FULL]).unwrap(),
                ))),
            );
        }

        // Set up a new initiate request and try to process it
        let other_peer_url = Url::from_str("ws://test-host:80/extra").unwrap();
        let arc_set = ArcSet::new(vec![DhtArc::FULL]).unwrap();
        harness
            .gossip
            .respond_to_msg(
                other_peer_url,
                GossipMessage::Initiate(K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: vec![],
                    arc_set: Some(ArcSetMessage {
                        value: arc_set.encode(),
                    }),
                    tie_breaker: 0,
                    new_since: Timestamp::now().as_micros(),
                    max_op_data_bytes: 0,
                }),
            )
            .await
            .unwrap();

        // Should result in a busy response
        let response = harness.wait_for_sent_response().await;
        assert!(matches!(response, GossipMessage::Busy(_)));
    }

    #[tokio::test]
    async fn initiate_with_unlimited_concurrent_rounds() {
        // Configure max rounds to 0, which should be treated as unlimited
        let config = K2GossipConfig {
            max_concurrent_accepted_rounds: 0,
            ..Default::default()
        };

        let mut harness = RespondTestHarness::create_with_config(config).await;

        // Try to initiate some rounds
        for i in 0..3 {
            // Set up a new initiate request and try to process it
            let other_peer_url =
                Url::from_str(format!("ws://test-host:80/{i}")).unwrap();
            let arc_set = ArcSet::new(vec![DhtArc::FULL]).unwrap();
            harness
                .gossip
                .respond_to_msg(
                    other_peer_url,
                    GossipMessage::Initiate(K2GossipInitiateMessage {
                        session_id: test_session_id(),
                        participating_agents: vec![],
                        arc_set: Some(ArcSetMessage {
                            value: arc_set.encode(),
                        }),
                        tie_breaker: 0,
                        new_since: Timestamp::now().as_micros(),
                        max_op_data_bytes: 0,
                    }),
                )
                .await
                .unwrap();

            // Each one should result in an accept response
            let response = harness.wait_for_sent_response().await;
            assert!(matches!(response, GossipMessage::Accept(_)));
        }

        assert_eq!(3, harness.gossip.accepted_round_states.read().await.len());
    }

    #[tokio::test]
    async fn initiate_exceed_burst_limit() {
        let harness = RespondTestHarness::create().await;

        let other_peer_url = Url::from_str("ws://test-host:80/1").unwrap();
        let arc_set = ArcSet::new(vec![DhtArc::FULL]).unwrap();
        let session_id = test_session_id();
        let message = GossipMessage::Initiate(K2GossipInitiateMessage {
            session_id: session_id.clone(),
            participating_agents: vec![],
            arc_set: Some(ArcSetMessage {
                value: arc_set.encode(),
            }),
            tie_breaker: 0,
            new_since: Timestamp::now().as_micros(),
            max_op_data_bytes: 5_000,
        });

        let terminate_message =
            GossipMessage::Terminate(K2GossipTerminateMessage {
                session_id,
                reason: "testing burst".to_string(),
            });

        let max_burst = AcceptBurstTracker::max_burst(&harness.gossip.config);

        for _ in 0..max_burst {
            harness
                .gossip
                .respond_to_msg(other_peer_url.clone(), message.clone())
                .await
                .unwrap();

            harness
                .gossip
                .respond_to_msg(
                    other_peer_url.clone(),
                    terminate_message.clone(),
                )
                .await
                .unwrap()
        }

        let err = harness
            .gossip
            .respond_to_msg(other_peer_url, message)
            .await
            .unwrap_err();

        assert_eq!(
            "Rejected peer behavior - initiate too soon",
            err.to_string()
        );
    }

    #[tokio::test]
    async fn initiate_without_arc_set() {
        let harness = RespondTestHarness::create().await;

        let other_peer_url = Url::from_str("ws://test-host:80/1").unwrap();
        let err = harness
            .gossip
            .respond_to_msg(
                other_peer_url,
                GossipMessage::Initiate(K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: vec![],
                    arc_set: None,
                    tie_breaker: 0,
                    new_since: Timestamp::now().as_micros(),
                    max_op_data_bytes: 5_000,
                }),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no arc set in initiate message"));
    }

    #[tokio::test]
    async fn resolve_tie_break_win() {
        enable_tracing();

        let mut harness = RespondTestHarness::create().await;

        let remote_agent = harness.create_agent(DhtArc::Empty).await;

        // Initiate a session with the remote agent
        let initiated = harness
            .gossip
            .initiate_gossip(remote_agent.url.clone().unwrap())
            .await
            .unwrap();
        assert!(initiated);

        // Wait for us to send the initiate message
        let response = harness.wait_for_sent_response().await;
        let initiate = match response {
            GossipMessage::Initiate(initiate) => initiate,
            other => panic!("Expected initiate message, got: {:?}", other),
        };

        assert!(initiate.tie_breaker > 0, "Expected tie breaker to be set");

        // Send an initiate message. It doesn't need to be valid for this test, it just needs to
        // have a lower tie-breaker.
        harness
            .gossip
            .respond_to_msg(
                remote_agent.url.clone().unwrap(),
                GossipMessage::Initiate(K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: vec![],
                    arc_set: None,
                    tie_breaker: 0,
                    new_since: 0,
                    max_op_data_bytes: 0,
                }),
            )
            .await
            .unwrap();

        // Check that we didn't send a response
        harness.rx.try_recv().unwrap_err();

        // And that our initiated state is still set
        {
            let initiated_lock =
                harness.gossip.initiated_round_state.lock().await;
            assert!(initiated_lock.is_some());
            let initiated = initiated_lock.as_ref().unwrap();
            assert_eq!(
                remote_agent.url.clone().unwrap(),
                initiated.session_with_peer
            );
            assert!(matches!(initiated.stage, RoundStage::Initiated(_)));
        }
    }

    #[tokio::test]
    async fn resolve_tie_break_lose() {
        enable_tracing();

        let mut harness = RespondTestHarness::create().await;

        let remote_agent = harness.create_agent(DhtArc::Empty).await;

        // Initiate a session with the remote agent
        let initiated = harness
            .gossip
            .initiate_gossip(remote_agent.url.clone().unwrap())
            .await
            .unwrap();
        assert!(initiated);

        // Wait for us to send the initiate message
        let response = harness.wait_for_sent_response().await;
        let initiate = match response {
            GossipMessage::Initiate(initiate) => initiate,
            other => panic!("Expected initiate message, got: {:?}", other),
        };

        // Send an initiate message with a higher tie-breaker
        let arc_set = ArcSet::new(vec![DhtArc::FULL]).unwrap();
        harness
            .gossip
            .respond_to_msg(
                remote_agent.url.clone().unwrap(),
                GossipMessage::Initiate(K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: vec![],
                    arc_set: Some(ArcSetMessage {
                        value: arc_set.encode(),
                    }),
                    tie_breaker: initiate.tie_breaker.saturating_add(1),
                    new_since: Timestamp::now().as_micros(),
                    max_op_data_bytes: 5_000,
                }),
            )
            .await
            .unwrap();

        // Check that we accepted the session
        let response = harness.wait_for_sent_response().await;
        match response {
            GossipMessage::Accept(accept) => accept,
            other => panic!("Expected accept message, got: {:?}", other),
        };

        // Check that we removed our initiated state
        {
            let initiated_lock =
                harness.gossip.initiated_round_state.lock().await;
            assert!(initiated_lock.is_none());
        }
    }

    #[tokio::test]
    async fn resolve_tie_break_tie() {
        enable_tracing();

        let mut harness = RespondTestHarness::create().await;

        let remote_agent = harness.create_agent(DhtArc::Empty).await;

        // Initiate a session with the remote agent
        let initiated = harness
            .gossip
            .initiate_gossip(remote_agent.url.clone().unwrap())
            .await
            .unwrap();
        assert!(initiated);

        // Wait for us to send the initiate message
        let response = harness.wait_for_sent_response().await;
        let initiate = match response {
            GossipMessage::Initiate(initiate) => initiate,
            other => panic!("Expected initiate message, got: {:?}", other),
        };

        assert!(initiate.tie_breaker > 0, "Expected tie breaker to be set");

        // Receive an initiate message. It doesn't need to be valid for this test, it just needs to
        // have the same value for the tie-breaker.
        let arc_set = ArcSet::new(vec![DhtArc::FULL]).unwrap();
        harness
            .gossip
            .respond_to_msg(
                remote_agent.url.clone().unwrap(),
                GossipMessage::Initiate(K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: vec![],
                    arc_set: Some(ArcSetMessage {
                        value: arc_set.encode(),
                    }),
                    // Use the same tie_breaker value to simulate both sides picking the same value.
                    tie_breaker: initiate.tie_breaker,
                    new_since: Timestamp::now().as_micros(),
                    max_op_data_bytes: 5_000,
                }),
            )
            .await
            .unwrap();

        // Check that we accepted the session in preference to the session we initiated.
        let response = harness.wait_for_sent_response().await;
        match response {
            GossipMessage::Accept(accept) => accept,
            other => panic!("Expected accept message, got: {:?}", other),
        };

        // Check that we are now in an accepted state.
        let accepted_lock = harness.gossip.accepted_round_states.read().await;
        let accepted = accepted_lock.get(&remote_agent.url.clone().unwrap());
        assert!(accepted.is_some());

        // Check that we removed our initiated state
        {
            let initiated_lock =
                harness.gossip.initiated_round_state.lock().await;
            assert!(initiated_lock.is_none());
        }

        // Now receive an accept message which is unsolicited because we cancelled our request in favour
        // of the other agent's initiation request.
        let response = harness
            .gossip
            .respond_to_msg(
                remote_agent.url.clone().unwrap(),
                GossipMessage::Accept(K2GossipAcceptMessage {
                    session_id: initiate.session_id,
                    participating_agents: initiate.participating_agents,
                    arc_set: initiate.arc_set,
                    missing_agents: Vec::new(),
                    new_since: UNIX_TIMESTAMP.as_micros(),
                    max_op_data_bytes: harness
                        .gossip
                        .config
                        .max_gossip_op_bytes,
                    new_ops: Vec::new(),
                    updated_new_since: UNIX_TIMESTAMP.as_micros(),
                    snapshot: None,
                }),
            )
            .await;
        assert!(
            matches!(
                response,
                Err(K2GossipError::PeerBehaviorError { ref ctx })
                    if ctx.as_ref() ==  "Unsolicited Accept message",
            ),
            "Expected 'Unsolicited Accept message', got: {response:?}"
        );
    }

    #[tokio::test]
    async fn respect_size_limit_for_new_ops() {
        enable_tracing();

        let max_ops_per_round = 3usize;
        let available_ops = 5;
        let op_size = 128usize;

        let harness = RespondTestHarness::create_with_config(K2GossipConfig {
            max_gossip_op_bytes: (max_ops_per_round * op_size) as u32,
            ..Default::default()
        })
        .await;

        let mut ops = Vec::new();
        for i in 0u8..available_ops {
            let op = MemoryOp::new(Timestamp::now(), vec![i; op_size]);
            ops.push(op);
        }

        harness
            .gossip
            .op_store
            .process_incoming_ops(
                ops.clone().into_iter().map(Into::into).collect(),
            )
            .await
            .unwrap();
        harness
            .gossip
            .inform_ops_stored(
                ops.clone().into_iter().map(Into::into).collect(),
            )
            .await
            .unwrap();

        let local_agent = harness.create_agent(DhtArc::FULL).await;
        harness
            .gossip
            .local_agent_store
            .add(local_agent.local.clone())
            .await
            .unwrap();

        let remote_agent = harness.create_agent(DhtArc::FULL).await;

        let accept = harness
            .gossip
            .respond_to_initiate(
                remote_agent.url.clone().unwrap(),
                K2GossipInitiateMessage {
                    session_id: test_session_id(),
                    participating_agents: encode_agent_ids([remote_agent
                        .agent
                        .clone()]),
                    arc_set: Some(ArcSetMessage {
                        value: ArcSet::new(vec![remote_agent
                            .local
                            .get_tgt_storage_arc()])
                        .unwrap()
                        .encode(),
                    }),
                    tie_breaker: rand::thread_rng()
                        .next_u32()
                        .saturating_add(1),
                    new_since: UNIX_TIMESTAMP.as_micros(),
                    max_op_data_bytes: harness
                        .gossip
                        .config
                        .max_gossip_op_bytes,
                },
            )
            .await
            .unwrap();

        assert!(
            accept.is_some(),
            "Should have accepted the initiate message"
        );
        let accept = accept.unwrap();
        let accept = match accept {
            GossipMessage::Accept(accept) => accept,
            _ => panic!("Expected an Accept message, got: {:?}", accept),
        };
        let sent_ops = decode_ids::<OpId>(accept.new_ops);
        assert_eq!(
            max_ops_per_round,
            sent_ops.len(),
            "Should have sent only 3 ops due to size limit"
        );

        let remaining_budget = harness
            .gossip
            .accepted_round_states
            .read()
            .await
            .get(remote_agent.url.as_ref().unwrap())
            .unwrap()
            .lock()
            .await
            .peer_max_op_data_bytes;
        assert_eq!(
            0, remaining_budget,
            "Should have used up the entire budget for new ops"
        );
    }
}

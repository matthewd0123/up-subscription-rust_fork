/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use log::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::UnboundedReceiver, oneshot};
use up_rust::UPriority;

use up_rust::core::usubscription::{
    FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
    FetchSubscriptionsResponse, Request, State, SubscriberInfo, Subscription, SubscriptionRequest,
    SubscriptionResponse, SubscriptionStatus, UnsubscribeRequest,
};
use up_rust::{communication::CallOptions, communication::RpcClient, UCode, UStatus, UUri, UUID};

use crate::helpers;
use crate::usubscription::UP_REMOTE_TTL;
use crate::usubscription_uris::{
    UP_SUBSCRIBE_ID, UP_UNSUBSCRIBE_ID, USUBSCRIPTION_SERVICE_ID,
    USUBSCRIPTION_SERVICE_VERSION_MAJOR,
};

// This is the core business logic for handling and tracking subscriptions. It is currently implemented as a single event-consuming
// function `handle_message()`, which is supposed to be spawned into a task and process the various `Events` that it can receive
// via tokio mpsc channel. This design allows to forgo the use of any synhronization primitives on the subscription-tracking container
// data types, as any access is coordinated/serialized via the Event selection loop.

// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIBERS_LEN: usize = 100;
// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIPTIONS_LEN: usize = 100;

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: SubscriberInfo,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: SubscriberInfo,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscribers {
        request: FetchSubscribersRequest,
        respond_to: oneshot::Sender<FetchSubscribersResponse>,
    },
    FetchSubscriptions {
        request: FetchSubscriptionsRequest,
        respond_to: oneshot::Sender<FetchSubscriptionsResponse>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<HashMap<UUri, HashSet<SubscriberInfo>>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: HashMap<UUri, HashSet<SubscriberInfo>>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetRemoteTopics {
        respond_to: oneshot::Sender<HashMap<UUri, State>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetRemoteTopics {
        topic_subscribers_replacement: HashMap<UUri, State>,
        respond_to: oneshot::Sender<()>,
    },
}

// Internal subscription manager API - used to update on remote subscriptions (deal with _PENDING states)
enum RemoteSubscriptionEvent {
    RemoteSubscriptionStateUpdate { topic: UUri, state: State },
}

// Wrapper type, include all kinds of actions subscription manager knows
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(RemoteSubscriptionEvent),
}

// Core business logic of subscription management - includes container data types for tracking subscriptions and remote subscriptions.
// Interfacing with this purely works via channels, so we do not have to deal with mutexes and similar concepts.
pub(crate) async fn handle_message(
    own_uri: UUri,
    up_client: Arc<dyn RpcClient>,
    mut command_receiver: UnboundedReceiver<SubscriptionEvent>,
) {
    // track subscribers for topics - if you're in this list, you have SUBSCRIBED, otherwise you're considered UNSUBSCRIBED
    #[allow(clippy::mutable_key_type)]
    let mut topic_subscribers: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();

    // for remote topics, we need to additionally deal with _PENDING states, this tracks states of these topics
    #[allow(clippy::mutable_key_type)]
    let mut remote_topics: HashMap<UUri, State> = HashMap::new();

    let (remote_sub_sender, mut remote_sub_receiver) =
        mpsc::unbounded_channel::<RemoteSubscriptionEvent>();

    loop {
        let event: Event = tokio::select! {
            // "Outside" events - actions that need to be performed
            event = command_receiver.recv() => match event {
                None => break,
                Some(event) => Event::LocalSubscription(event),
            },
            // "Inside" events - updates around remote subscription states
            event = remote_sub_receiver.recv() => match event {
                None => break,
                Some(event) => Event::RemoteSubscription(event),
            },

        };
        match event {
            // These all deal with user-driven interactions (the core usubscription interface functionality)
            Event::LocalSubscription(event) => match event {
                SubscriptionEvent::AddSubscription {
                    subscriber,
                    topic,
                    respond_to,
                } => {
                    // Add new subscriber to topic subscription tracker (create new entries as necessary)
                    topic_subscribers
                        .entry(topic.clone())
                        .or_default()
                        .insert(subscriber);

                    // This really should unwrap() ok, as we just inserted an entry above
                    let subscribers_count =
                        topic_subscribers.get(&topic).map(|e| e.len()).unwrap_or(0);

                    let mut state = State::SUBSCRIBED; // everything in topic_subscribers is considered SUBSCRIBED by default

                    if topic.is_remote_authority(&own_uri) {
                        state = State::SUBSCRIBE_PENDING; // for remote_topics, we explicitly track state due to the _PENDING scenarios
                        remote_topics.entry(topic.clone()).or_insert(state);

                        if subscribers_count == 1 {
                            // this is the first subscriber to this (remote) topic, so perform remote subscription
                            let own_uri_clone = own_uri.clone();
                            let up_client_clone = up_client.clone();
                            let remote_sub_sender_clone = remote_sub_sender.clone();

                            helpers::spawn_and_log_error(async move {
                                remote_subscribe(
                                    own_uri_clone,
                                    topic,
                                    up_client_clone,
                                    remote_sub_sender_clone,
                                )
                                .await?;
                                Ok(())
                            });
                        }
                    }
                    let _ = respond_to.send(SubscriptionStatus {
                        state: state.into(),
                        ..Default::default()
                    });
                }
                SubscriptionEvent::RemoveSubscription {
                    subscriber,
                    topic,
                    respond_to,
                } => {
                    let mut state = State::UNSUBSCRIBED; // everything not in topic_subscribers is considered UNSUBSCRIBED by default
                    if let Some(entry) = topic_subscribers.get_mut(&topic) {
                        // check if we even know this subscriber-topic combo
                        entry.remove(&subscriber);

                        // if topic is remote, we were tracking this remote topic already, and this was the last subscriber
                        if topic.is_remote_authority(&own_uri)
                            && remote_topics.contains_key(&topic)
                            && entry.is_empty()
                        {
                            // until remote ubsubscribe confirmed (below), set to UNSUBSCRIBE_PENDING
                            state = State::UNSUBSCRIBE_PENDING;
                            if let Some(entry) = remote_topics.get_mut(&topic) {
                                *entry = state;
                            }

                            // this was the last subscriber to this (remote) topic, so perform remote unsubscription
                            let own_uri_clone = own_uri.clone();
                            let up_client_clone = up_client.clone();
                            let remote_sub_sender_clone = remote_sub_sender.clone();
                            let topic_cloned = topic.clone();

                            helpers::spawn_and_log_error(async move {
                                remote_unsubscribe(
                                    own_uri_clone,
                                    topic_cloned,
                                    up_client_clone,
                                    remote_sub_sender_clone,
                                )
                                .await?;
                                Ok(())
                            });
                        }
                    }
                    // If this was the last subscriber to topic, remote the entire subscription entry from tracker
                    if topic_subscribers.get(&topic).is_some_and(|e| e.is_empty()) {
                        topic_subscribers.remove(&topic);
                    }

                    let _ = respond_to.send(SubscriptionStatus {
                        state: state.into(),
                        ..Default::default()
                    });
                }
                SubscriptionEvent::FetchSubscribers {
                    request,
                    respond_to,
                } => {
                    let FetchSubscribersRequest { topic, offset, .. } = request;

                    // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
                    // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
                    if let Some(subs) = topic_subscribers.get(&topic) {
                        let mut subscribers: Vec<&SubscriberInfo> = subs.iter().collect();

                        if let Some(offset) = offset {
                            subscribers.drain(..offset as usize);
                        }

                        // split up result list, to make sense of has_more_records field
                        let mut has_more = false;
                        if subscribers.len() > UP_MAX_FETCH_SUBSCRIBERS_LEN {
                            subscribers.truncate(UP_MAX_FETCH_SUBSCRIBERS_LEN);
                            has_more = true;
                        }

                        let _ = respond_to.send(FetchSubscribersResponse {
                            subscribers: subscribers.iter().map(|s| (*s).clone()).collect(),
                            has_more_records: has_more.into(),
                            ..Default::default()
                        });
                    } else {
                        let _ = respond_to.send(FetchSubscribersResponse::default());
                    }
                }
                SubscriptionEvent::FetchSubscriptions {
                    request,
                    respond_to,
                } => {
                    let FetchSubscriptionsRequest {
                        request, offset, ..
                    } = request;
                    debug!("BEGIN");
                    let mut fetch_subscriptions_response = FetchSubscriptionsResponse::default();

                    if let Some(request) = request {
                        match request {
                            Request::Subscriber(subscriber) => {
                                // This is where someone wants "all subscriptions of a specific subscriber",
                                // which isn't very straighforward with the way we do bookeeping, so
                                // first, get all entries from our topic-subscribers ledger that contain the requested SubscriberInfo
                                let subscriptions: Vec<(&UUri, &HashSet<SubscriberInfo>)> =
                                    topic_subscribers
                                        .iter()
                                        .filter(|entry| entry.1.contains(&subscriber))
                                        .collect();

                                // from that set, we use the topics and build Subscription response objects
                                let mut result_subs: Vec<Subscription> = Vec::new();
                                for (topic, _) in subscriptions {
                                    // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                                    // if nothing is available there we fall back to default assumption that any
                                    // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                                    let state =
                                        remote_topics.get(topic).unwrap_or(&State::SUBSCRIBED);

                                    let subscription = Subscription {
                                        topic: Some(topic.clone()).into(),
                                        subscriber: Some(subscriber.clone()).into(),
                                        status: Some(SubscriptionStatus {
                                            state: (*state).into(),
                                            ..Default::default()
                                        })
                                        .into(),
                                        ..Default::default()
                                    };
                                    result_subs.push(subscription);
                                }

                                if let Some(offset) = offset {
                                    result_subs.drain(..offset as usize);
                                }

                                // split up result list, to make sense of has_more_records field
                                let mut has_more = false;
                                if result_subs.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
                                    result_subs.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
                                    has_more = true;
                                }

                                fetch_subscriptions_response = FetchSubscriptionsResponse {
                                    subscriptions: result_subs,
                                    has_more_records: Some(has_more),
                                    ..Default::default()
                                };
                            }
                            Request::Topic(topic) => {
                                if let Some(subs) = topic_subscribers.get(&topic) {
                                    let mut subscribers: Vec<&SubscriberInfo> =
                                        subs.iter().collect();

                                    if let Some(offset) = offset {
                                        subscribers.drain(..offset as usize);
                                    }

                                    // split up result list, to make sense of has_more_records field
                                    let mut has_more = false;
                                    if subscribers.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
                                        subscribers.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
                                        has_more = true;
                                    }

                                    let mut result_subs: Vec<Subscription> = Vec::new();
                                    for subscriber in subscribers {
                                        // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                                        // if nothing is available there we fall back to default assumption that any
                                        // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                                        let state =
                                            remote_topics.get(&topic).unwrap_or(&State::SUBSCRIBED);

                                        let subscription = Subscription {
                                            topic: Some(topic.clone()).into(),
                                            subscriber: Some(subscriber.clone()).into(),
                                            status: Some(SubscriptionStatus {
                                                state: (*state).into(),
                                                ..Default::default()
                                            })
                                            .into(),
                                            ..Default::default()
                                        };
                                        result_subs.push(subscription);
                                    }

                                    fetch_subscriptions_response = FetchSubscriptionsResponse {
                                        subscriptions: result_subs,
                                        has_more_records: Some(has_more),
                                        ..Default::default()
                                    };
                                }
                            }
                            _ => {
                                // This really shouldn't happen - also compare input validation in usubscription.rs
                            }
                        }
                    }
                    let _ = respond_to.send(fetch_subscriptions_response);
                }
                #[cfg(test)]
                SubscriptionEvent::GetTopicSubscribers { respond_to } => {
                    let _r = respond_to.send(topic_subscribers.clone());
                }
                #[cfg(test)]
                SubscriptionEvent::SetTopicSubscribers {
                    topic_subscribers_replacement,
                    respond_to,
                } => {
                    topic_subscribers = topic_subscribers_replacement;
                    let _r = respond_to.send(());
                }
                #[cfg(test)]
                SubscriptionEvent::GetRemoteTopics { respond_to } => {
                    let _r = respond_to.send(remote_topics.clone());
                }
                #[cfg(test)]
                SubscriptionEvent::SetRemoteTopics {
                    topic_subscribers_replacement: remote_topics_replacement,
                    respond_to,
                } => {
                    remote_topics = remote_topics_replacement;
                    let _r = respond_to.send(());
                }
            },
            // these deal with feedback/state updates from the remote subscription handlers
            Event::RemoteSubscription(event) => match event {
                RemoteSubscriptionEvent::RemoteSubscriptionStateUpdate { topic, state } => {
                    remote_topics.entry(topic).and_modify(|s| *s = state);
                }
            },
        }
    }
}

// Perform remote topic subscription
async fn remote_subscribe(
    own_uri: UUri,
    topic: UUri,
    up_client: Arc<dyn RpcClient>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
) -> Result<(), UStatus> {
    // build request
    let subscription_request = SubscriptionRequest {
        topic: Some(topic.clone()).into(),
        subscriber: Some(SubscriberInfo {
            uri: Some(own_uri.clone()).into(),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    };

    // send request
    let subscription_response: SubscriptionResponse = up_client
        .invoke_proto_method(
            make_remote_subscribe_uuri(&subscription_request.topic),
            CallOptions::for_rpc_request(
                UP_REMOTE_TTL,
                Some(UUID::new()),
                None,
                Some(UPriority::UPRIORITY_CS2),
            ),
            subscription_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error invoking remote subscription request: {e}"),
            )
        })?;

    // deal with response
    if subscription_response.is_state(State::SUBSCRIBED) {
        debug!("Got remote subscription response, state SUBSCRIBED");

        let _ = remote_sub_sender.send(RemoteSubscriptionEvent::RemoteSubscriptionStateUpdate {
            topic,
            state: State::SUBSCRIBED,
        });
    } else {
        debug!("Got remote subscription response, some other state");
    }

    Ok(())
}

// Perform remote topic unsubscription
async fn remote_unsubscribe(
    own_uri: UUri,
    topic: UUri,
    up_client: Arc<dyn RpcClient>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
) -> Result<(), UStatus> {
    // build request
    let unsubscribe_request = UnsubscribeRequest {
        topic: Some(topic.clone()).into(),
        subscriber: Some(SubscriberInfo {
            uri: Some(own_uri.clone()).into(),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    };

    // send request
    let unsubscribe_response: UStatus = up_client
        .invoke_proto_method(
            make_remote_unsubscribe_uuri(&unsubscribe_request.topic),
            CallOptions::for_rpc_request(
                UP_REMOTE_TTL,
                Some(UUID::new()),
                None,
                Some(UPriority::UPRIORITY_CS2),
            ),
            unsubscribe_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error invoking remote unsubscribe request: {e}"),
            )
        })?;

    // deal with response
    match unsubscribe_response.code.enum_value_or(UCode::UNKNOWN) {
        UCode::OK => {
            debug!("Got OK remote unsubscribe response");
            let _ =
                remote_sub_sender.send(RemoteSubscriptionEvent::RemoteSubscriptionStateUpdate {
                    topic,
                    state: State::UNSUBSCRIBED,
                });
        }
        code => {
            debug!("Got {:?} remote unsubscribe response", code);
            return Err(UStatus::fail_with_code(
                code,
                "Error during remote unsubscribe",
            ));
        }
    };

    Ok(())
}

// Create a remote Subscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Subscribe-endpoint properties
fn make_remote_subscribe_uuri(uri: &UUri) -> UUri {
    UUri {
        authority_name: uri.authority_name.clone(),
        ue_id: USUBSCRIPTION_SERVICE_ID,
        ue_version_major: USUBSCRIPTION_SERVICE_VERSION_MAJOR,
        resource_id: UP_SUBSCRIBE_ID as u32,
        ..Default::default()
    }
}

// Create a remote Unsubscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Unsubscribe-endpoint properties
fn make_remote_unsubscribe_uuri(uri: &UUri) -> UUri {
    UUri {
        authority_name: uri.authority_name.clone(),
        ue_id: USUBSCRIPTION_SERVICE_ID,
        ue_version_major: USUBSCRIPTION_SERVICE_VERSION_MAJOR,
        resource_id: UP_UNSUBSCRIBE_ID as u32,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use protobuf::MessageFull;
    use test_case::test_case;
    use tokio::sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    };
    use up_rust::{communication::UPayload, core::usubscription::UnsubscribeResponse};

    use crate::test_lib;

    use super::*;

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: UnboundedSender<SubscriptionEvent>,
    }

    impl CommandSender {
        fn new() -> Self {
            let (command_sender, command_receiver) = mpsc::unbounded_channel::<SubscriptionEvent>();
            let client_mock = test_lib::mocks::MockRpcClientMock::default();
            helpers::spawn_and_log_error(async move {
                handle_message(
                    test_lib::helpers::local_usubscription_service_uri(),
                    Arc::new(client_mock),
                    command_receiver,
                )
                .await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        fn new_with_client_options<R: MessageFull, S: MessageFull>(
            expected_remote_method: UUri,
            expected_call_options: CallOptions,
            expected_request: R,
            expected_response: S,
            times: usize,
        ) -> Self {
            let (command_sender, command_receiver) = mpsc::unbounded_channel::<SubscriptionEvent>();
            let mut client_mock = test_lib::mocks::MockRpcClientMock::default();

            let expected_request_payload =
                UPayload::try_from_protobuf(expected_request).expect("Test/mock request data bad");
            let expected_response_payload = UPayload::try_from_protobuf(expected_response)
                .expect("Test/mock response data bad");

            client_mock
                .expect_invoke_method()
                .times(times)
                .withf(move |remote_method, call_options, request_payload| {
                    *remote_method == expected_remote_method
                        && test_lib::is_equivalent_calloptions(call_options, &expected_call_options)
                        && *request_payload == Some(expected_request_payload.clone())
                })
                .returning(move |_u, _o, _p| Ok(Some(expected_response_payload.clone())));

            helpers::spawn_and_log_error(async move {
                handle_message(
                    test_lib::helpers::local_usubscription_service_uri(),
                    Arc::new(client_mock),
                    command_receiver,
                )
                .await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        async fn subscribe(
            &self,
            topic: UUri,
            subscriber: SubscriberInfo,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }

        async fn unsubscribe(
            &self,
            topic: UUri,
            subscriber: SubscriberInfo,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::RemoveSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }

        async fn get_topic_subscribers(
            &self,
        ) -> Result<HashMap<UUri, HashSet<SubscriberInfo>>, Box<dyn Error>> {
            let (respond_to, receive_from) =
                oneshot::channel::<HashMap<UUri, HashSet<SubscriberInfo>>>();
            let command = SubscriptionEvent::GetTopicSubscribers { respond_to };

            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_topic_subscribers(
            &self,
            topic_subscribers_replacement: HashMap<UUri, HashSet<SubscriberInfo>>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetTopicSubscribers {
                topic_subscribers_replacement,
                respond_to,
            };

            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }

        async fn get_remote_topics(&self) -> Result<HashMap<UUri, State>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<UUri, State>>();
            let command = SubscriptionEvent::GetRemoteTopics { respond_to };

            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_remote_topics(
            &self,
            remote_topics_replacement: HashMap<UUri, State>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetRemoteTopics {
                topic_subscribers_replacement: remote_topics_replacement,
                respond_to,
            };

            self.command_sender.send(command)?;
            Ok(receive_from.await?)
        }
    }

    #[test_case(vec![(UUri::default(), SubscriberInfo::default())]; "Default susbcriber-topic")]
    #[test_case(vec![(UUri::default(), SubscriberInfo::default()), (UUri::default(), SubscriberInfo::default())]; "Multiple default susbcriber-topic")]
    #[test_case(vec![
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_info1()),
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_info1())
         ]; "Multiple identical susbcriber-topic combinations")]
    #[test_case(vec![
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_info1()),
         (test_lib::helpers::local_topic2_uri(), test_lib::helpers::subscriber_info1()),
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_info2()),
         (test_lib::helpers::local_topic2_uri(), test_lib::helpers::subscriber_info2())
         ]; "Multiple susbcriber-topic combinations")]
    #[tokio::test]
    async fn test_subscribe(topic_subscribers: Vec<(UUri, SubscriberInfo)>) {
        test_lib::before_test();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();
        for (topic, subscriber) in topic_subscribers {
            desired_state
                .entry(topic.clone())
                .or_default()
                .insert(subscriber.clone());

            // Operation to test
            let result = command_sender.subscribe(topic, subscriber).await;
            assert!(result.is_ok());

            // Verify operation result content
            let subscription_status = result.unwrap();
            assert_eq!(subscription_status.state.unwrap(), State::SUBSCRIBED);
        }

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), desired_state.len());
        assert_eq!(topic_subscribers, desired_state);
    }

    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBE_PENDING; "Remote topic, remote state SUBSCRIBED_PENDING")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBED; "Remote topic, remote state SUBSCRIBED")]
    #[tokio::test]
    async fn test_remote_subscribe(remote_topic: UUri, remote_state: State) {
        test_lib::before_test();

        // Prepare things
        let remote_method = make_remote_subscribe_uuri(&remote_topic);
        let remote_subscription_request = SubscriptionRequest {
            topic: Some(remote_topic.clone()).into(),
            subscriber: Some(SubscriberInfo {
                uri: Some(test_lib::helpers::local_usubscription_service_uri()).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let remote_subscription_response = SubscriptionResponse {
            topic: Some(remote_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: remote_state.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let remote_call_options = CallOptions::for_rpc_request(
            UP_REMOTE_TTL,
            Some(UUID::new()),
            None,
            Some(UPriority::UPRIORITY_CS2),
        );
        let command_sender =
            CommandSender::new_with_client_options::<SubscriptionRequest, SubscriptionResponse>(
                remote_method,
                remote_call_options,
                remote_subscription_request,
                remote_subscription_response,
                1,
            );

        // Operation to test
        let result = command_sender
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_info1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status.state.unwrap(), State::SUBSCRIBE_PENDING);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
        assert_eq!(
            *remote_topics.get(&remote_topic.clone()).unwrap(),
            remote_state
        );
    }

    #[tokio::test]
    async fn test_repeated_remote_subscribe() {
        test_lib::before_test();
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things
        let remote_method = make_remote_subscribe_uuri(&remote_topic);
        let remote_subscription_request = SubscriptionRequest {
            topic: Some(remote_topic.clone()).into(),
            subscriber: Some(SubscriberInfo {
                uri: Some(test_lib::helpers::local_usubscription_service_uri()).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let remote_subscription_response = SubscriptionResponse {
            topic: Some(remote_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: State::SUBSCRIBED.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let remote_call_options = CallOptions::for_rpc_request(
            UP_REMOTE_TTL,
            Some(UUID::new()),
            None,
            Some(UPriority::UPRIORITY_CS2),
        );
        let command_sender =
            CommandSender::new_with_client_options::<SubscriptionRequest, SubscriptionResponse>(
                remote_method,
                remote_call_options,
                remote_subscription_request,
                remote_subscription_response,
                // We only expect 1 call to remote subscribe, as we're subscribing the same topic twice
                // (only the first operation should result in a remote subscription call)
                1,
            );

        // Operation to test
        let result = command_sender
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_info1())
            .await;
        assert!(result.is_ok());

        let result = command_sender
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_info2())
            .await;
        assert!(result.is_ok());

        // Assert we have to local topic-subscriber entries...
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        let entry = topic_subscribers.get(&remote_topic);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().len(), 2);

        // ... and one remote topic entry
        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
    }

    // All subscribers for a topic unsubscribe
    #[tokio::test]
    async fn test_final_unsubscribe() {
        test_lib::before_test();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_info1());

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_info1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status.state.unwrap(), State::UNSUBSCRIBED);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 0);
    }

    // Only some subscribers of a topic unsubscribe
    #[tokio::test]
    async fn test_partial_unsubscribe() {
        test_lib::before_test();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_info1());
        entry.insert(test_lib::helpers::subscriber_info2());

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_info1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status.state.unwrap(), State::UNSUBSCRIBED);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);
        assert_eq!(
            topic_subscribers
                .get(&test_lib::helpers::local_topic1_uri())
                .unwrap()
                .len(),
            1
        );
        assert!(topic_subscribers
            .get(&test_lib::helpers::local_topic1_uri())
            .unwrap()
            .contains(&test_lib::helpers::subscriber_info2()));
    }

    // All subscribers for a remote topic unsubscribe
    #[tokio::test]
    async fn test_final_remote_unsubscribe() {
        test_lib::before_test();
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things
        let remote_method = make_remote_unsubscribe_uuri(&remote_topic);
        let remote_unsubscribe_request = UnsubscribeRequest {
            topic: Some(remote_topic.clone()).into(),
            subscriber: Some(SubscriberInfo {
                uri: Some(test_lib::helpers::local_usubscription_service_uri()).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let remote_unsubscribe_response = UStatus {
            code: UCode::OK.into(),
            ..Default::default()
        };
        let remote_call_options = CallOptions::for_rpc_request(
            UP_REMOTE_TTL,
            Some(UUID::new()),
            None,
            Some(UPriority::UPRIORITY_CS2),
        );
        let command_sender = CommandSender::new_with_client_options::<UnsubscribeRequest, UStatus>(
            remote_method,
            remote_call_options,
            remote_unsubscribe_request,
            remote_unsubscribe_response,
            1,
        );

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_info1());

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<UUri, State> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), State::SUBSCRIBED);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_info1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(
            subscription_status.state.unwrap(),
            // initial return should just have set the state tracker to UNSUBSCRIBE_PENDING, then spun out a task to deal with remote unsubscribe
            State::UNSUBSCRIBE_PENDING
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        // We're expecting our local topic-subscriber tracker to be empty at this point
        assert_eq!(topic_subscribers.len(), 0);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let entry = remote_topics.get(&remote_topic);
        assert!(entry.is_some());
        let state = entry.unwrap();
        // by now the remote unsubscribe and subsequent state change to UNSUBSCRIBE has come through the command channels
        assert_eq!(*state, State::UNSUBSCRIBED);
    }
}

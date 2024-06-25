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

// This is the 'outside API' of subscription manager
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
}

// Internal subscription manager API - used to update on remote subscriptions (deal with _PENDING states)
enum RemoteSubscriptionEvent {
    RemoteSubscriptionStateUpdate { topic: UUri, state: State },
}

// This is supposed to give some test-only interaction, where test cases can directly access/set the internal subscription tracking data
#[cfg(test)]
enum TestInteractions {
    #[allow(dead_code)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<HashMap<UUri, HashSet<SubscriberInfo>>>,
    },
    #[allow(dead_code)]
    SetTopicSubscribers {
        topic_subscribers: HashMap<UUri, HashSet<SubscriberInfo>>,
    },
}

// Wrapper type, include all kinds of actions subscription manager knows
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(RemoteSubscriptionEvent),
    #[cfg(test)]
    TestInteractions(TestInteractions),
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

    #[cfg(test)]
    #[allow(unused_variables)]
    let (remote_test_sender, mut remote_test_receiver) =
        mpsc::unbounded_channel::<TestInteractions>();

    loop {
        #[cfg(not(test))]
        // This is not optimal, but the only option I could find at the moment - also refer to https://github.com/tokio-rs/tokio/issues/3974
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
        #[cfg(test)]
        // This is not optimal, but the only option I could find at the moment - also refer to https://github.com/tokio-rs/tokio/issues/3974
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
            // Testing
            event = remote_test_receiver.recv() => match event {
                None => break,
                Some(event) => Event::TestInteractions(event),
            },
        };
        match event {
            #[cfg(test)]
            Event::TestInteractions(event) => match event {
                #[allow(unused_variables)]
                TestInteractions::GetTopicSubscribers { respond_to } => {}
                #[allow(unused_variables)]
                TestInteractions::SetTopicSubscribers { topic_subscribers } => {}
            },
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

                            helpers::spawn_and_log_error(async move {
                                remote_unsubscribe(
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
    use test_case::test_case;

    use up_rust::core::usubscription::SubscriptionRequest;

    #[test_case(SubscriptionRequest::default(); "Default SubscriptionRequest")]
    #[tokio::test]
    async fn test_subscribe_input_validation(_subscription_request: SubscriptionRequest) {
        // test_lib::before_test();
    }
}

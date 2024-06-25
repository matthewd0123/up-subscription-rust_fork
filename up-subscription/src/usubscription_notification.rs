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
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

use up_rust::core::usubscription::{SubscriberInfo, SubscriptionStatus, Update};
use up_rust::{UMessageBuilder, UTransport, UUri, UUID};

use crate::usubscription;

// This is the core business logic for tracking and sending subscription update notifications. It is currently implemented as a single
// event-consuming function `notification_engine()`, which is supposed to be spawned into a task, and process the various notification
// `Events` that it can receive via tokio mpsc channel.

// This is my interpretation of the new-UUri version of what the proto definition says (up:/core.usubscription/3/subscriptions#Update)
const UP_NOTIFICATION_CHANNEL: &str = "up:/0/3/8000";

// This is the 'outside API' of notification handler
#[derive(Debug)]
pub(crate) enum Event {
    AddNotifyee {
        subscriber: UUri,
        topic: UUri,
    },
    RemoveNotifyee {
        subscriber: UUri,
    },
    StateChange {
        subscriber: SubscriberInfo,
        topic: UUri,
        status: SubscriptionStatus,
    },
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
// Interfacing with this purely works via channels.
pub(crate) async fn notification_engine(
    up_transport: Arc<dyn UTransport>,
    mut events: UnboundedReceiver<Event>,
) {
    // keep track of which subscriber wants to be notified on which topic
    #[allow(clippy::mutable_key_type)]
    let mut notification_topics: HashMap<UUri, UUri> = HashMap::new();

    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                None => break,
                Some(event) => event,
            },
        };
        match event {
            Event::AddNotifyee { subscriber, topic } => {
                if topic.is_event() {
                    notification_topics.insert(subscriber, topic);
                } else {
                    error!("Topic UUri is not a valid event target");
                }
            }
            Event::RemoveNotifyee { subscriber } => {
                notification_topics.remove(&subscriber);
            }
            Event::StateChange {
                subscriber,
                topic,
                status,
            } => {
                let update = Update {
                    topic: Some(topic).into(),
                    subscriber: Some(subscriber.clone()).into(),
                    status: Some(status).into(),
                    ..Default::default()
                };

                // Send Update message to general notification channel
                // as per usubscription.proto RegisterForNotifications(NotificationsRequest)
                match UMessageBuilder::publish(
                    UUri::from_str(UP_NOTIFICATION_CHANNEL)
                        .expect("This really should have worked"),
                )
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&update)
                {
                    Err(e) => {
                        error!("Error building global update notification message: {e}");
                    }
                    Ok(update_msg) => {
                        if let Err(e) = up_transport.send(update_msg).await {
                            error!(
                                "Error sending global subscription-change update notification: {e}"
                            );
                        }
                    }
                }

                // Send Update message to any dedicated registered notification-subscribers
                for notification_topic in notification_topics.values() {
                    debug!(
                        "Sending notification to ({}): topic {}, subscriber {}, status {}",
                        notification_topic.to_uri(usubscription::INCLUDE_SCHEMA),
                        update
                            .topic
                            .as_ref()
                            .unwrap_or_default()
                            .to_uri(usubscription::INCLUDE_SCHEMA),
                        update
                            .subscriber
                            .uri
                            .as_ref()
                            .unwrap_or_default()
                            .to_uri(usubscription::INCLUDE_SCHEMA),
                        update.status.as_ref().unwrap_or_default()
                    );
                    match UMessageBuilder::publish(notification_topic.clone())
                        .with_message_id(UUID::build())
                        .build_with_protobuf_payload(&update)
                    {
                        Err(e) => {
                            error!("Error building susbcriber-specific update notification message: {e}");
                        }
                        Ok(update_msg) => {
                            if let Err(e) = up_transport.send(update_msg).await {
                                error!(
                                    "Error sending susbcriber-specific subscription-change update notification: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

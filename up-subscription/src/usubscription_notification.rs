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
use tokio::sync::oneshot;

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
pub(crate) enum NotificationEvent {
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
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current notifyee ledger
    #[cfg(test)]
    GetNotificationTopics {
        respond_to: oneshot::Sender<HashMap<UUri, UUri>>,
    },
    // Purely for use during testing: force-set new notifyees ledger
    #[cfg(test)]
    SetNotificationTopics {
        notification_topics_replacement: HashMap<UUri, UUri>,
        respond_to: oneshot::Sender<()>,
    },
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
// Interfacing with this purely works via channels.
pub(crate) async fn notification_engine(
    up_transport: Arc<dyn UTransport>,
    mut events: UnboundedReceiver<NotificationEvent>,
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
            NotificationEvent::AddNotifyee { subscriber, topic } => {
                if topic.is_event() {
                    notification_topics.insert(subscriber, topic);
                } else {
                    error!("Topic UUri is not a valid event target");
                }
            }
            NotificationEvent::RemoveNotifyee { subscriber } => {
                notification_topics.remove(&subscriber);
            }
            NotificationEvent::StateChange {
                subscriber,
                topic,
                status,
                respond_to,
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
                let _r = respond_to.send(());
            }
            #[cfg(test)]
            NotificationEvent::GetNotificationTopics { respond_to } => {
                let _r = respond_to.send(notification_topics.clone());
            }
            #[cfg(test)]
            NotificationEvent::SetNotificationTopics {
                notification_topics_replacement,
                respond_to,
            } => {
                notification_topics = notification_topics_replacement;
                let _r = respond_to.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use tokio::sync::mpsc::{self, UnboundedSender};
    use up_rust::core::usubscription::State;
    use up_rust::UMessage;

    use crate::helpers;
    use crate::test_lib;

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: UnboundedSender<NotificationEvent>,
    }

    impl CommandSender {
        fn new(expected_message: Vec<UMessage>) -> Self {
            let (command_sender, command_receiver) = mpsc::unbounded_channel::<NotificationEvent>();
            let transport_mock =
                test_lib::mocks::utransport_mock_for_notification_manager(expected_message);

            helpers::spawn_and_log_error(async move {
                notification_engine(Arc::new(transport_mock), command_receiver).await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        async fn add_notifyee(&self, subscriber: UUri, topic: UUri) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::AddNotifyee { subscriber, topic })?)
        }

        async fn remove_notifyee(&self, subscriber: UUri) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::RemoveNotifyee { subscriber })?)
        }

        async fn state_change(
            &self,
            subscriber: SubscriberInfo,
            topic: UUri,
            status: SubscriptionStatus,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let _r = self.command_sender.send(NotificationEvent::StateChange {
                subscriber,
                topic,
                status,
                respond_to,
            });
            Ok(receive_from.await?)
        }

        async fn get_notification_topics(&self) -> Result<HashMap<UUri, UUri>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<UUri, UUri>>();
            let _r = self
                .command_sender
                .send(NotificationEvent::GetNotificationTopics { respond_to });

            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_notification_topics(
            &self,
            notification_topics_replacement: HashMap<UUri, UUri>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let _r = self
                .command_sender
                .send(NotificationEvent::SetNotificationTopics {
                    respond_to,
                    notification_topics_replacement,
                });

            Ok(receive_from.await?)
        }
    }

    #[tokio::test]
    async fn test_add_notifyee() {
        test_lib::before_test();
        let command_sender = CommandSender::new(vec![]);

        let expected_subscriber = test_lib::helpers::subscriber_info1().uri.unwrap();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        command_sender
            .add_notifyee(expected_subscriber.clone(), expected_topic.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 1);
        assert!(notification_topics.contains_key(&expected_subscriber));
        assert_eq!(
            *notification_topics.get(&expected_subscriber).unwrap(),
            expected_topic
        );
    }

    #[tokio::test]
    async fn test_remove_notifyee() {
        test_lib::before_test();
        let command_sender = CommandSender::new(vec![]);

        // prepare things
        let expected_subscriber = test_lib::helpers::subscriber_info1().uri.unwrap();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        #[allow(clippy::mutable_key_type)]
        let mut notification_topics_replacement: HashMap<UUri, UUri> = HashMap::new();
        notification_topics_replacement.insert(expected_subscriber.clone(), expected_topic.clone());

        command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .expect("Error communicating with subscription manager");

        // operation to test
        command_sender
            .remove_notifyee(expected_subscriber.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 0);
    }

    // This test expects a state change notification to be send to the generic Notification Update channel
    #[tokio::test]
    async fn test_state_change() {
        test_lib::before_test();

        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus {
            state: State::SUBSCRIBED.into(),
            ..Default::default()
        };
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let changing_subscriber = test_lib::helpers::subscriber_info1();

        // the update message that we're expecting
        let expected_update = Update {
            topic: Some(changing_topic.clone()).into(),
            subscriber: Some(changing_subscriber.clone()).into(),
            status: Some(changing_status.clone()).into(),
            ..Default::default()
        };

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(UUri::from_str(UP_NOTIFICATION_CHANNEL).unwrap())
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        let command_sender = CommandSender::new(vec![expected_message_general_channel]);

        // operation to test
        let r = command_sender
            .state_change(
                changing_subscriber.clone(),
                changing_topic.clone(),
                changing_status,
            )
            .await;
        assert!(r.is_ok())
    }

    // This test expects a state change notification to be send to the generic Notification Update channel,
    // as well as to the custom notification topics registered by susbcribers who like things complicated.
    #[tokio::test]
    async fn test_state_change_custom() {
        test_lib::before_test();

        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus {
            state: State::SUBSCRIBED.into(),
            ..Default::default()
        };
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let changing_subscriber = test_lib::helpers::subscriber_info1();

        // first subscriber that set a custom notification topic
        let expected_subscriber_1 = test_lib::helpers::subscriber_info2();
        let expected_topic_1 = test_lib::helpers::local_topic2_uri();

        // second subscriber that set a custom notification topic
        let expected_subscriber_2 = test_lib::helpers::subscriber_info3();
        let expected_topic_2 = test_lib::helpers::local_topic3_uri();

        // custom notification expectations
        #[allow(clippy::mutable_key_type)]
        let mut notification_topics_replacement: HashMap<UUri, UUri> = HashMap::new();
        notification_topics_replacement.insert(
            expected_subscriber_1.uri.clone().unwrap(),
            expected_topic_1.clone(),
        );
        notification_topics_replacement.insert(
            expected_subscriber_2.uri.clone().unwrap(),
            expected_topic_2.clone(),
        );

        // the update message that we're expecting
        let expected_update = Update {
            topic: Some(changing_topic.clone()).into(),
            subscriber: Some(changing_subscriber.clone()).into(),
            status: Some(changing_status.clone()).into(),
            ..Default::default()
        };

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(UUri::from_str(UP_NOTIFICATION_CHANNEL).unwrap())
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        // custom update messages, expected by subscribers who registered for notification on custom topics
        let expected_message_custom_1 = UMessageBuilder::publish(expected_topic_1)
            .with_message_id(UUID::build())
            .build_with_protobuf_payload(&expected_update)
            .unwrap();
        let expected_message_custom_2 = UMessageBuilder::publish(expected_topic_2)
            .with_message_id(UUID::build())
            .build_with_protobuf_payload(&expected_update)
            .unwrap();

        // put all of this into our mock
        let command_sender = CommandSender::new(vec![
            expected_message_general_channel,
            expected_message_custom_1,
            expected_message_custom_2,
        ]);

        // set custom notification config
        command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .expect("Error communicating with subscription manager");

        // operation to test
        let r = command_sender
            .state_change(
                changing_subscriber.clone(),
                changing_topic.clone(),
                changing_status,
            )
            .await;
        assert!(r.is_ok())
    }
}

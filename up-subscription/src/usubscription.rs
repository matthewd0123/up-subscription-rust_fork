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

use async_trait::async_trait;
use log::*;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot,
};

use crate::notification_manager::{self, NotificationEvent};
use crate::{
    helpers,
    subscription_manager::{self, SubscriptionEvent},
};

use up_rust::core::usubscription::{
    FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
    FetchSubscriptionsResponse, NotificationsRequest, Request, SubscriptionRequest,
    SubscriptionResponse, SubscriptionStatus, USubscription, UnsubscribeRequest,
};
use up_rust::{communication::RpcClient, LocalUriProvider, UCode, UStatus, UTransport, UUri};

// Version used in USubscriptionService UEntity
const USUBSCRIPTION_NAME: &str = "core.usubscription";

/// Whether to include 'up:'  uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

/// This trait (and the comprised UTransportHolder trait) is simply there to have a generic type that
/// usubscription Listeners deal with, so that USubscriptionService can be properly mocked.
pub trait USubscriptionServiceAbstract:
    USubscription + LocalUriProvider + UTransportHolder
{
}

// implement SubscriptionServiceAbstract
impl USubscriptionServiceAbstract for USubscriptionService {}

/// This trait primarily serves to provide a hook-point for using the mockall crate, for mocking USubscriptionService objects
/// where we also need/want to inject custom/mock UTransport implementations that subsequently get used in test cases.
pub trait UTransportHolder {
    fn get_transport(&self) -> Arc<dyn UTransport>;
}

impl UTransportHolder for USubscriptionService {
    fn get_transport(&self) -> Arc<dyn UTransport> {
        self.up_transport.clone()
    }
}

/// Core landing point and coordination of business logic of the uProtocol USubscription service. This implementation usually would be
/// front-ended by the various `listeners` to connect with corresponding uProtocol RPC server endpoints.
///
/// Functionally, the code in this context primarily cares about:
/// - input validation
/// - interaction with / orchestration of backends for managing subscriptions (`usubscription_manager.rs`) and dealing with notifications (`usubscription_notification.rs`)
#[derive(Clone)]
pub struct USubscriptionService {
    pub name: String,
    pub own_uri: UUri,

    pub(crate) up_transport: Arc<dyn UTransport>,
    subscription_sender: UnboundedSender<SubscriptionEvent>,
    notification_sender: UnboundedSender<notification_manager::NotificationEvent>,
}

/// Implementation of uProtocol L3 USubscription service
impl USubscriptionService {
    /// Construct a new USubscriptionService
    /// Atm this will directly spin up two tasks which deal with subscription and notification management, with no further action
    /// required, but also no explicit shutdown operation yet - that's a TODO.
    ///
    /// # Arguments
    ///
    /// * `name` - An optional name for the USubscription service, might be used in logging (TODO)
    /// * `own_uri` - The UUri of this USUbscription service (setting a Authority might be sufficient - TODO)
    /// * `up_transport` - Implementation of UTransport to be used by this USUbscription instance, for sending Listener-responses and Notifications
    /// * `up_client` - Implementation of RpcClient to be used by this USUbscription instance, for performing remote-subscribe operations
    ///
    pub fn new(
        name: Option<String>,
        own_uri: UUri,
        up_transport: Arc<dyn UTransport>,
        up_client: Arc<dyn RpcClient>,
    ) -> USubscriptionService {
        helpers::init_once();

        // Set up subscription manager actor
        let up_client_cloned = up_client.clone();
        let own_uri_cloned = own_uri.clone();
        let (subscription_sender, subscription_receiver) =
            mpsc::unbounded_channel::<SubscriptionEvent>();
        helpers::spawn_and_log_error(async move {
            subscription_manager::handle_message(
                own_uri_cloned,
                up_client_cloned,
                subscription_receiver,
            )
            .await;
            Ok(())
        });

        // Set up notification service actor
        let up_transport_cloned = up_transport.clone();
        let (notification_sender, notification_receiver) =
            mpsc::unbounded_channel::<notification_manager::NotificationEvent>();
        helpers::spawn_and_log_error(async move {
            notification_manager::notification_engine(up_transport_cloned, notification_receiver)
                .await;
            Ok(())
        });

        USubscriptionService {
            name: name.unwrap_or(USUBSCRIPTION_NAME.into()),
            up_transport,
            own_uri,
            subscription_sender,
            notification_sender,
        }
    }
}

/// Implementation of <https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc#usubscription>
#[async_trait]
impl USubscription for USubscriptionService {
    /// Implementation of <https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#51-subscription>
    async fn subscribe(
        &self,
        subscription_request: SubscriptionRequest,
    ) -> Result<SubscriptionResponse, UStatus> {
        let SubscriptionRequest {
            subscriber, topic, ..
        } = subscription_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got SubscriptionRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::AddSubscription {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se) {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(status) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error communicating with subscription management",
            ));
        };

        // Notify update channel
        let (respond_to, receive_from) = oneshot::channel::<()>();
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::StateChange {
                subscriber,
                topic: topic.clone(),
                status: status.clone(),
                respond_to,
            })
        {
            error!("Error initiating subscription-change update notification: {e}");
        }
        if let Err(e) = receive_from.await {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error sending subscription-change update notification: {e}");
        };

        // Build and return result
        Ok(SubscriptionResponse {
            topic: Some(topic).into(),
            status: Some(status).into(),
            ..Default::default()
        })
    }

    /// Implementation of <https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#52-unsubscribe>
    async fn unsubscribe(&self, unsubscribe_request: UnsubscribeRequest) -> Result<(), UStatus> {
        let UnsubscribeRequest {
            subscriber, topic, ..
        } = unsubscribe_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got UnsubscribeRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::RemoveSubscription {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se) {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(status) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error communicating with subscription management",
            ));
        };

        // Notify update channel
        let (respond_to, receive_from) = oneshot::channel::<()>();
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::StateChange {
                subscriber,
                topic: topic.clone(),
                status: status.clone(),
                respond_to,
            })
        {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error initiating subscription-change update notification: {e}");
        }
        if let Err(e) = receive_from.await {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error sending subscription-change update notification: {e}");
        };

        // Return result
        Ok(())
    }

    async fn register_for_notifications(
        &self,
        notifications_register_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        let NotificationsRequest {
            subscriber, topic, ..
        } = notifications_register_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty notification UUri",
            ));
        }
        if !topic.is_event() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "UUri not a valid event destination",
            ));
        }
        if self.own_uri.is_remote_authority(&topic) {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Cannot use remote topic for notifications",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo",
            ));
        }
        let Some(subscriber_uri) = subscriber.uri.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No subscriber UUri",
            ));
        };
        if subscriber_uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty subscriber UUri",
            ));
        }

        debug!(
            "Got RegisterForNotifications for notification topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber_uri.to_uri(INCLUDE_SCHEMA)
        );

        // Perform notification management
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::AddNotifyee {
                subscriber: subscriber_uri,
                topic,
            })
        {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to update notification settings: {e}"),
            ));
        }

        // Return result
        Ok(())
    }

    async fn unregister_for_notifications(
        &self,
        notifications_unregister_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        // Current implementation, we only track one notification channel/topic per subscriber, so ignore topic here
        let NotificationsRequest { subscriber, .. } = notifications_unregister_request;

        // Basic input validation
        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo",
            ));
        }
        let Some(subscriber_uri) = subscriber.uri.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No subscriber UUri",
            ));
        };
        if subscriber_uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty subscriber UUri",
            ));
        }

        debug!(
            "Got UnregisterForNotifications for notification from subscriber {}",
            subscriber_uri.to_uri(INCLUDE_SCHEMA)
        );

        // Perform notification management
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::RemoveNotifyee {
                subscriber: subscriber_uri,
            })
        {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to update notification settings: {e}"),
            ));
        }

        // Return result
        Ok(())
    }

    async fn fetch_subscribers(
        &self,
        fetch_subscribers_request: FetchSubscribersRequest,
    ) -> Result<FetchSubscribersResponse, UStatus> {
        // Basic input validation
        let Some(topic) = fetch_subscribers_request.topic.as_ref() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        debug!(
            "Got FetchSubscribersRequest for topic {}",
            topic.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscribersResponse>();
        let se = SubscriptionEvent::FetchSubscribers {
            request: fetch_subscribers_request,
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se) {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(response) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error receiving response from subscription management",
            ));
        };

        // Return result
        debug!(
            "Returning {} subscriber entries",
            response.subscribers.len()
        );
        Ok(response)
    }

    async fn fetch_subscriptions(
        &self,
        fetch_subscriptions_request: FetchSubscriptionsRequest,
    ) -> Result<FetchSubscriptionsResponse, UStatus> {
        // Basic input validation
        let Some(request) = fetch_subscriptions_request.request.as_ref() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Missing Request property",
            ));
        };
        match request {
            Request::Topic(topic) => {
                if topic.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request topic UUri",
                    ));
                }
            }
            Request::Subscriber(subscriber) => {
                if subscriber.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request SubscriberInfo",
                    ));
                }
                let Some(subscriber_uri) = subscriber.uri.as_ref() else {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "No request subscriber UUri",
                    ));
                };
                if subscriber_uri.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request subscriber UUri",
                    ));
                }
            }
            _ => {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Invalid/unknown Request variant",
                ));
            }
        }

        debug!("Got FetchSubscriptionsRequest");

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscriptionsResponse>();
        let se = SubscriptionEvent::FetchSubscriptions {
            request: fetch_subscriptions_request,
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se) {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(response) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error receiving response from subscription management",
            ));
        };

        // Return result
        debug!(
            "Returning {} Subscription entries",
            response.subscriptions.len()
        );
        Ok(response)
    }
}

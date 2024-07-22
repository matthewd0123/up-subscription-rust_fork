/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * SPDX-FileType: SOURCE
 * SPDX-License-Identifier: Apache-2.0
 */

use std::str::FromStr;
use std::sync::Arc;
use up_rust::{communication::InMemoryRpcClient, LocalUriProvider, UTransport, UUri};
use up_subscription::listeners::fetch_subscribers::{self, FetchSubscribersListener};
use up_subscription::listeners::fetch_subscriptions::FetchSubscriptionsListener;
use up_subscription::listeners::register_for_notifications::RegisterForNotificationsListener;
use up_subscription::listeners::subscribe::SubscribeListener;
use up_subscription::listeners::unregister_for_notifications::UnregisterForNotificationsListener;
use up_subscription::listeners::unsubscribe::UnsubscribeListener;
use up_subscription::USubscriptionService;
use up_transport_socket::UTransportSocket;

fn any_uuri_val() -> UUri {
    UUri {
        authority_name: "*".to_string(),
        ue_id: 0x0000_FFFF,     // any instance, any service
        ue_version_major: 0xFF, // any
        resource_id: 0xFFFF,    // any
        ..Default::default()
    }
}

fn subscribe_uri_val() -> UUri {
    UUri {
        resource_id: 1,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

fn unsubscribe_uri_val() -> UUri {
    UUri {
        resource_id: 2,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

fn fetch_subscriptions_uri_val() -> UUri {
    UUri {
        resource_id: 3,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

fn fetch_subscribers_uri_val() -> UUri {
    UUri {
        resource_id: 8,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

fn register_for_notifications_uri_val() -> UUri {
    UUri {
        resource_id: 6,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

fn unregister_for_notifications_uri_val() -> UUri {
    UUri {
        resource_id: 7,
        ue_id: 0,
        ue_version_major: 3,
        ..Default::default()
    }
}

pub struct SubscriptionRpcClientUriProvider {
    own_uri: UUri,
}

impl LocalUriProvider for SubscriptionRpcClientUriProvider {
    fn get_authority(&self) -> String {
        self.own_uri.authority_name.clone()
    }
    fn get_resource_uri(&self, resource_id: u16) -> UUri {
        let mut uri = self.own_uri.clone();
        uri.resource_id = resource_id as u32;
        uri
    }
    fn get_source_uri(&self) -> UUri {
        self.own_uri.clone()
    }
}

impl SubscriptionRpcClientUriProvider {
    pub fn new(own_uri: UUri) -> Self {
        SubscriptionRpcClientUriProvider { own_uri }
    }
}

#[tokio::main]
async fn main() {
    let name = "usubscription-service".to_string();

    let own_uuri = UUri::from_str("//subscription/1234/1/2345").expect("BAD UURI");

    let transport_result = UTransportSocket::new();
    let transport = Arc::new(transport_result.expect("Failed to create UTransportSocket"));
    println!("Transport initialized");

    let local_uri_provider = Arc::new(SubscriptionRpcClientUriProvider::new(own_uuri.clone()));

    let rpc_client = Arc::new(
        InMemoryRpcClient::new(transport.clone(), local_uri_provider)
            .await
            .expect("AH"),
    );

    let usubscription = Arc::new(USubscriptionService::new(
        Some(name),
        own_uuri,
        transport.clone(),
        rpc_client,
    ));
    println!("usubscription initialized");

    let subscribe_listener = Arc::new(SubscribeListener::new(usubscription.clone()));
    let unsubscribe_listener = Arc::new(UnsubscribeListener::new(usubscription.clone()));
    let fetch_subscribers_listener = Arc::new(FetchSubscribersListener::new(usubscription.clone()));
    let fetch_subscriptions_listener =
        Arc::new(FetchSubscriptionsListener::new(usubscription.clone()));
    let register_for_notifications_listener =
        Arc::new(RegisterForNotificationsListener::new(usubscription.clone()));
    let unregister_for_notifications_listener = Arc::new(UnregisterForNotificationsListener::new(
        usubscription.clone(),
    ));

    let _ = transport.register_listener(&any_uuri_val(), Some(&subscribe_uri_val()), subscribe_listener).await;
    
    let _ = transport.register_listener(&any_uuri_val(), Some(&unsubscribe_uri_val()), unsubscribe_listener).await;
    let _ = transport.register_listener(
        &any_uuri_val(),
        Some(&fetch_subscribers_uri_val()),
        fetch_subscribers_listener,
    ).await;
    let _ =transport.register_listener(
        &any_uuri_val(),
        Some(&fetch_subscriptions_uri_val()),
        fetch_subscriptions_listener,
    ).await;
    let _ = transport.register_listener(
        &any_uuri_val(),
        Some(&register_for_notifications_uri_val()),
        register_for_notifications_listener,
    ).await;
    let _ = transport.register_listener(
        &any_uuri_val(),
        Some(&unregister_for_notifications_uri_val()),
        unregister_for_notifications_listener,
    ).await;

    println!("Subscription service is running!");
     // Keep the main function alive to let the background tasks complete
     tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c signal");
}

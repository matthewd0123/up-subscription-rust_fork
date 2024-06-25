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

use up_rust::{LocalUriProvider, UUri};

use crate::usubscription::USubscriptionService;

// Constants for building UUris etc
pub(crate) const UP_SUBSCRIBE_ID: u16 = 1;
pub(crate) const UP_UNSUBSCRIBE_ID: u16 = 2;
pub(crate) const UP_FETCH_SUBSCRIPTIONS_ID: u16 = 3;
pub(crate) const UP_REGISTER_FOR_NOTIFICATONS_ID: u16 = 6;
pub(crate) const UP_UNREGISTER_FOR_NOTIFICATONS_ID: u16 = 7;
pub(crate) const UP_FETCH_SUBSCRIBERS_ID: u16 = 8;

pub(crate) const UP_NOTIFICATION_ID: u16 = 0x8000;
pub(crate) const USUBSCRIPTION_SERVICE_ID: u32 = 0;
pub(crate) const USUBSCRIPTION_SERVICE_VERSION_MAJOR: u32 = 3;

impl LocalUriProvider for USubscriptionService {
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

impl USubscriptionService {
    // Return UUri for a RPC Subscribe request to this service
    pub fn notification_uri(&self) -> UUri {
        self.get_resource_uri(UP_NOTIFICATION_ID)
    }

    // Return UUri for a RPC Subscribe request to this service
    pub fn subscribe_uuri(&self) -> UUri {
        self.get_resource_uri(UP_SUBSCRIBE_ID)
    }

    // Return UUri for a RPC Unsubscribe request to this service
    pub fn unsubscribe_uuri(&self) -> UUri {
        self.get_resource_uri(UP_UNSUBSCRIBE_ID)
    }

    // Return UUri for a RPC FetchSubscriptions request to this service
    pub fn fetch_subscriptions_uuri(&self) -> UUri {
        self.get_resource_uri(UP_FETCH_SUBSCRIPTIONS_ID)
    }

    // Return UUri for a RPC FetchSubscribers request to this service
    pub fn fetch_subscribers_uuri(&self) -> UUri {
        self.get_resource_uri(UP_FETCH_SUBSCRIBERS_ID)
    }

    // Return UUri for a RPC RegisterForNotifications request to this service
    pub fn register_for_notifications_uuri(&self) -> UUri {
        self.get_resource_uri(UP_REGISTER_FOR_NOTIFICATONS_ID)
    }

    // Return UUri for a RPC UnregisterForNotifications request to this service
    pub fn unregister_for_notifications_uuri(&self) -> UUri {
        self.get_resource_uri(UP_UNREGISTER_FOR_NOTIFICATONS_ID)
    }
}

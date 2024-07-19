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

mod common;
pub(crate) use common::*;

mod notification_manager;
mod subscription_manager;

mod usubscription;
mod usubscription_uris;
pub use usubscription::*;

pub mod listeners {
    pub mod fetch_subscribers;
    pub mod fetch_subscriptions;
    pub mod register_for_notifications;
    pub mod subscribe;
    pub mod unregister_for_notifications;
    pub mod unsubscribe;
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::*;

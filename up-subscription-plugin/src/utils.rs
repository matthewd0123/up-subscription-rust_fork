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

use up_rust::UUri;

pub const ANY_UURI: UUri = UUri {
    authority_name: "*".to_string(),
    ue_id: 0x0000_FFFF,     // any instance, any service
    ue_version_major: 0xFF, // any
    resource_id: 0xFFFF,    // any
    ..Default::default()
};

pub const SUBSCRIBE_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 1,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

pub const UNSUBSCRIBE_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 2,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

pub const FETCH_SUBSCRIPTIONS_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 3,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

pub const REGISTER_FOR_NOTIFICATIONS_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 6,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

pub const UNREGISTER_FOR_NOTIFICATIONS_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 7,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

pub const FETCH_SUBSCRIBERS_URI: UUri = UUri {
    authority_name: "core.usubscription".to_string(),
    resource_id: 8,
    ue_id: 0,
    ue_version_major: 3,
    ..Default::default()
};

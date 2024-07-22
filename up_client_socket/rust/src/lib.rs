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

use protobuf::MessageField;
use up_rust::UAttributes;
use up_rust::UListener;
use up_rust::{UCode, UMessage, UStatus, UTransport, UUri};

use log::{debug, error};
use protobuf::Message;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::task;

pub struct UTransportSocket {
    socket_sync: TcpStream,
    uri_to_listener: Arc<Mutex<HashMap<(UUri, UUri), Arc<dyn UListener>>>>,
}

// Define constants for addresses and maximum message length
const DISPATCHER_ADDR: (&str, u16) = ("127.0.0.1", 44444);
const BYTES_MSG_LENGTH: usize = 32_767;
fn matches(source: &UUri, sink: &UUri, attributes: MessageField<UAttributes>) -> bool {
    debug!("attributes");
    if source == &UUri::any() {
        *&UUri::matches(&sink, &attributes.sink)
    } else if sink == &UUri::any() {
        *&UUri::matches(&source, &attributes.source)
    } else {
        *&UUri::matches(&source, &attributes.source) && *&UUri::matches(&sink, &attributes.sink)
    }
}
impl UTransportSocket {
    pub fn new() -> Result<Self, UStatus> {
        let socket_sync = TcpStream::connect(DISPATCHER_ADDR).map_err(|e| {
            error!("Error connecting sync socket: {:?}", e);
            UStatus::fail_with_code(UCode::INTERNAL, "Issue in connecting sync socket")
        })?;
        println!("socket connected");

        let uri_to_listener: Arc<Mutex<HashMap<(UUri, UUri), Arc<dyn UListener>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let socket_clone = match socket_sync.try_clone() {
            Ok(socket) => socket,
            Err(err) => {
                error!("Issue in cloning sync socket: {}", err);
                return Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Issue in cloning socket_sync",
                ));
            }
        };
        let mut transport_socket = UTransportSocket {
            socket_sync: socket_clone,
            uri_to_listener: uri_to_listener.clone(),
        };
        if let Err(err) = transport_socket.socket_init() {
            let err_string = format!("Socket transport initialization failed: {err}");
            error!("{err_string}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, err_string));
        }
        debug!("Socket transport initialized successfully");
        println!("Socket transport initialized successfully");

        Ok(UTransportSocket {
            socket_sync,
            uri_to_listener,
        })
    }

    fn socket_init(&mut self) -> Result<(), UStatus> {
        let socket_clone = match self.socket_sync.try_clone() {
            Ok(socket) => socket,
            Err(err) => {
                error!("Issue in cloning sync socket: {}", err);
                return Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Issue in cloning socket_sync",
                ));
            }
        };

        let uri_to_listener_clone = self.uri_to_listener.clone();

        let mut self_copy = Self {
            socket_sync: socket_clone,
            uri_to_listener: uri_to_listener_clone,
        };
        task::spawn(async move { self_copy.dispatcher_listener().await });

        Ok(())
    }
    async fn dispatcher_listener(&mut self) {
        debug!("Started listener for dispatcher");
        println!("Started listener for dispatcher");

        let mut recv_data = [0; BYTES_MSG_LENGTH];

        while let Ok(bytes_received) = self.socket_sync.read(&mut recv_data) {
            // Check if no data is received
            if bytes_received == 0 {
                continue;
            }

            let umessage_result = UMessage::parse_from_bytes(&recv_data[..bytes_received]);
            let umessage = match umessage_result {
                Ok(umsg) => umsg,
                Err(err) => {
                    error!("Failed to parse message: {}", err);
                    continue;
                }
            };
            println!("Received message from dispatcher :{:?}", umessage);

            let _ = self.notify_listener(umessage).await;
        }
    }

    pub async fn notify_listener(&self, umsg: UMessage) -> Result<(), UStatus> {
        println!("notify_listener");

        let uri_to_listener = self.uri_to_listener.lock().map_err(|err| {
            error!("Error acquiring lock: {}", err);
            UStatus::fail_with_code(UCode::INTERNAL, "Issue in acquiring lock".to_string())
        })?;

        let listener_to_invoke = uri_to_listener
            .iter()
            .find_map(|((source_uri, sink_uri), listener)| {
                let attributes = umsg.attributes.clone();
                let is_match = matches(source_uri, sink_uri, attributes);

                if is_match {
                    println!("Found listener registered against source and sink filter");
                    Some((Arc::clone(listener), umsg.clone()))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    "No listeners registered for URIs".to_string(),
                )
            })?;

        let (listener, umsg) = listener_to_invoke;

        // Spawn an async task to handle the listener
        tokio::spawn(async move {
            listener.on_receive(umsg).await;
        });
        tokio::spawn(async move {
            println!("Testing ");
        });
        Ok(())
    }
}

#[async_trait]
impl UTransport for UTransportSocket {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        println!("Send api called");

        let mut socket_clone = match self.socket_sync.try_clone() {
            Ok(socket) => socket,
            Err(err) => {
                error!("Error cloning socket: {}", err);

                return Err(UStatus::fail_with_code(
                    UCode::UNAVAILABLE,
                    format!("socket_sync cloning issue: {err:?}"),
                ));
            }
        };

        let umsg_serialized_result = message.clone().write_to_bytes();
        let umsg_serialized = match umsg_serialized_result {
            Ok(serialized) => serialized,
            Err(err) => {
                error!("Send Serialization Issue: {}", err);
                return Err(UStatus::fail_with_code(
                    UCode::UNAVAILABLE,
                    format!("umsg_serilization  issue: {err:?}"),
                ));
            }
        };

        match socket_clone.write_all(&umsg_serialized) {
            Ok(()) => Ok(()),
            Err(err) => Err(UStatus::fail_with_code(
                UCode::UNAVAILABLE,
                format!("Dispatcher communication issue: {err:?}"),
            )),
        }
    }

    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "Not implemented",
        ))
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        debug!("Register listener called");
        let mut uri_to_listener: std::sync::MutexGuard<HashMap<(_, _), Arc<dyn UListener>>> =
            match self.uri_to_listener.lock() {
                Ok(lock) => lock,
                Err(err) => {
                    error!("Error acquiring lock: {}", err);
                    return Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Issue in acquiring lock",
                    ));
                }
            };
        // Handle default UUri for sink_filter
        let sink_uri = sink_filter.cloned().unwrap_or_default();
        uri_to_listener.insert((source_filter.clone(), sink_uri), listener.clone());
        println!("Size of uri_to_listener: {}", uri_to_listener.len());

        return Ok(());
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        debug!("UnRegister listener called");

        let mut uri_to_listener: std::sync::MutexGuard<HashMap<(_, _), Arc<dyn UListener>>> =
            match self.uri_to_listener.lock() {
                Ok(lock) => lock,
                Err(err) => {
                    error!("Error acquiring lock: {}", err);
                    return Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Issue in acquiring lock",
                    ));
                }
            };
        // Handle default UUri for sink_filter
        let sink_uri = sink_filter.cloned().unwrap_or_default();
        // Attempt to remove the listener
        if uri_to_listener
            .remove(&(source_filter.clone(), sink_uri))
            .is_some()
        {
            return Ok(());
        } else {
            return Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                "UListener not found for given uuri",
            ));
        }
    }
}

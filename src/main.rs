// SPDX-License-Identifier: MIT OR Apache-2.0

//! Example chat CLI app using the high-level p2panda API.
//!
//! ## Usage
//!
//! Run the example in one terminal to create a new chat:
//!
//! `cargo run --example chat`
//!
//! Then using the CHAT_ID output from the first instance for the <CHAT_ID> argument, run as many
//! more instances as you like. The <BOOTSTRAP> argument is optional and only required if
//! discovery should run over the internet. Any other member's MEMBER_ID can be used as a
//! bootstrap.
//!
//! `cargo run --example chat <CHAT_ID> <BOOTSTRAP>`
//!
//! The example can also be run with INFO logging to see ephemeral heartbeat messages and sync
//! session info:
//!
//! `RUST_LOG=chat_example=info cargo run`
//!
//! Once the chat is running, you can optionally set your nickname:
//!
//! `/nick penguin_lover72`
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use p2panda::streams::StreamEvent;
use p2panda::RelayUrl;
use p2panda_core::test_utils::setup_logging;
use p2panda_core::{cbor, Hash, Topic, VerifyingKey};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, Instant};
use tracing::info;

const RELAY_URL: &str = "https://euc1-1.relay.n0.iroh.link/.";

const NETWORK_ID: &str = "chat";

type Message = String;

/// Heartbeat message to be sent over ephemeral stream.
#[derive(Debug, Serialize, Deserialize)]
struct Heartbeat {
    sender: VerifyingKey,
    online: bool,
    rnd: u64,
}

impl Heartbeat {
    fn new(sender: VerifyingKey, online: bool) -> Self {
        Self {
            sender,
            online,
            rnd: rand::random(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    let args: Vec<String> = std::env::args().collect();

    let chat_id = if args.len() > 1 {
        let topic = Topic::from_str(&args[1]).map_err(|err| format!("invalid chat id: {err}"))?;
        Some(topic)
    } else {
        None
    };

    let bootstrap = if args.len() > 2 {
        let bootstrap =
            VerifyingKey::from_str(&args[2]).map_err(|err| format!("invalid bootstrap: {err}"))?;
        Some(bootstrap)
    } else {
        None
    };

    // Build and spawn the node, adding an optional bootstrap which is required for discovery over
    // the internet with the help of a relay. Any <MEMBER_ID> can be used as the bootstrap.
    let node = {
        let mut builder = p2panda::builder().network_id(Hash::digest(NETWORK_ID).into());

        if let Some(bootstrap) = bootstrap {
            let relay_url: RelayUrl = RELAY_URL.parse().unwrap();
            builder = builder
                .relay_url(relay_url.clone())
                .bootstrap(bootstrap, relay_url);
        }

        builder.spawn().await?
    };

    println!("[ node id ]: {}", node.id().to_hex());

    // Use the provided chat id or create a new, random topic to serve as the chat id.
    let chat_id = chat_id.unwrap_or(Topic::random());

    println!("[ chat id ]: {}", chat_id.to_hex());

    // Subscribe to ephemeral stream to publish and receive heartbeat messages.
    let (heartbeat_tx, mut heartbeat_rx) = node.ephemeral_stream::<Vec<u8>>(chat_id).await?;

    let local_verifying_key = node.id();

    // Mapping of public key to nickname.
    let nicknames = Arc::new(RwLock::new(HashMap::<VerifyingKey, String>::new()));

    // Mapping of public key to the instant that the last heartbeat message was received.
    let status = Arc::new(RwLock::new(HashMap::new()));

    // Publish (ephemeral) heartbeat messages.
    tokio::task::spawn(async move {
        loop {
            // Create and serialize a heartbeat message.
            let msg = Heartbeat::new(local_verifying_key, true);
            let encoded_msg = cbor::encode_cbor(&msg).unwrap();

            // Publish the message to the gossip topic.
            heartbeat_tx.publish(encoded_msg).await.unwrap();

            time::sleep(Duration::from_secs(rand::random_range(20..30))).await;
        }
    });

    // Receive and log each (ephemeral) heartbeat message.
    {
        let nicknames = Arc::clone(&nicknames);
        let status = Arc::clone(&status);

        tokio::spawn(async move {
            loop {
                if let Some(message) = heartbeat_rx.next().await {
                    let msg: Heartbeat =
                        cbor::decode_cbor(&message.body()[..]).expect("valid cbor encoding");

                    info!("received heartbeat: {:?}", msg);

                    // Look up nickname for sender.
                    let name = if let Some(nickname) = nicknames.read().await.get(&msg.sender) {
                        nickname.to_owned()
                    } else {
                        msg.sender.fmt_short()
                    };

                    // Update status hashmap.
                    if status
                        .write()
                        .await
                        .insert(msg.sender, Instant::now())
                        .is_none()
                    {
                        println!("-> {} came online", name)
                    }

                    if !msg.online {
                        status.write().await.remove(&msg.sender);
                        println!("-> {} went offline", name)
                    }
                }
            }
        });
    }

    // Subscribe to the chat id if one was provided.
    //
    // Share the id <CHAT ID> with other instances to allow them to join.
    let (chat_tx, mut chat_rx) = node.stream::<Message>(chat_id).await?;

    {
        tokio::task::spawn(async move {
            while let Some(event) = chat_rx.next().await {
                match event {
                    StreamEvent::SyncEnded {
                        remote_node_id,
                        received_operations,
                        received_bytes,
                        sent_bytes,
                        ..
                    } => {
                        info!(
                            "finished sync session with {}, operations received = {}, bytes received = {}, bytes sent = {}",
                            remote_node_id.fmt_short(),
                            received_operations,
                            received_bytes,
                            sent_bytes
                        );
                    }
                    StreamEvent::Processed {
                        operation,
                        source: _,
                    } if operation.author() != local_verifying_key => {
                        // Only print messages from remote nodes.
                        let text = operation.message();
                        let remote_verifying_key = operation.author();
                        let remote_id = remote_verifying_key.fmt_short();

                        // Check if the text of this operation is setting a nickname.
                        if let Some(nick) = text.strip_prefix("/nick ") {
                            if let Some(previous_nick) =
                                nicknames.read().await.get(&remote_verifying_key)
                            {
                                print!("-> {} is now known as: {}", previous_nick, nick);
                            } else {
                                print!("-> {} is now known as: {}", remote_id, nick);
                            }

                            // Update the nicknames map.
                            nicknames
                                .write()
                                .await
                                .insert(remote_verifying_key, nick.trim().to_owned());
                        } else {
                            // Print a regular chat message.
                            print!(
                                "{}: {}",
                                nicknames
                                    .read()
                                    .await
                                    .get(&remote_verifying_key)
                                    .unwrap_or(&remote_id),
                                text
                            )
                        }
                    }
                    _ => (),
                }
            }
        });
    }

    // Listen for text input via the terminal.
    let (line_tx, mut line_rx) = mpsc::channel(1);
    std::thread::spawn(move || input_loop(line_tx));

    // Publish each line of text on the chat topic.
    tokio::task::spawn(async move {
        while let Some(text) = line_rx.recv().await {
            // Update the nickname mapping for the local node.
            if let Some(nick) = text.strip_prefix("/nick ") {
                print!("-> changed nick to: {}", nick);
            }

            chat_tx.publish(text).await.expect("publisher is working");
        }
    });

    // Listen for `Ctrl+c` and shutdown the node.
    tokio::signal::ctrl_c().await?;

    Ok(())
}

fn input_loop(line_tx: mpsc::Sender<String>) -> Result<(), std::io::Error> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    loop {
        stdin.read_line(&mut buffer)?;
        line_tx
            .blocking_send(buffer.clone())
            .map_err(std::io::Error::other)?;
        buffer.clear();
    }
}

pub trait ShortFormat {
    fn fmt_short(&self) -> String;
}

impl ShortFormat for VerifyingKey {
    fn fmt_short(&self) -> String {
        self.to_hex()[0..10].to_string()
    }
}

//! The Fluxum side of the parity comparison (TST-090a): the demo module
//! (chat + presence + tasks) served by a real `fluxum-server`, driven through
//! the published Rust SDK — the same client a user would write, not a
//! privileged in-process shortcut.

use std::sync::Arc;

use fluxum_sdk::protocol::{FluxBinReader, FluxValue};
use fluxum_sdk::{Connection, TableSchema};

use crate::workload::{BenchClient, Side};

/// Primary-key projections for the demo schema (SDK-040): every demo table
/// keys on its first column, so the projection is "the encoded prefix up to
/// and including it".
fn demo_schemas() -> Vec<TableSchema> {
    // (table, reader for the pk column) — the prefix consumed by the reader
    // IS the key, byte-stable and collision-free.
    fn prefix_key(consume: impl Fn(&mut FluxBinReader<'_>) + 'static) -> impl Fn(&[u8]) -> Vec<u8> {
        move |bytes: &[u8]| {
            let mut reader = FluxBinReader::new(bytes);
            consume(&mut reader);
            let consumed = bytes.len() - reader.remaining();
            bytes[..consumed].to_vec()
        }
    }
    let u64_pk = || prefix_key(|r| drop(r.read_u64()));
    let conn_pk = || prefix_key(|r| drop(r.read_connection_id()));

    vec![
        TableSchema {
            name: "ChatMessage".to_owned(),
            pk_of_row: Box::new(u64_pk()),
            pk_of_delete: Box::new(u64_pk()),
        },
        TableSchema {
            name: "Task".to_owned(),
            pk_of_row: Box::new(u64_pk()),
            pk_of_delete: Box::new(u64_pk()),
        },
        TableSchema {
            name: "OnlineUser".to_owned(),
            pk_of_row: Box::new(conn_pk()),
            pk_of_delete: Box::new(conn_pk()),
        },
    ]
}

/// The Fluxum [`Side`]: a running server's URL (`fluxum://` TCP or `http://`
/// Streamable HTTP — the transport is part of the recorded configuration).
pub struct FluxumSide {
    url: String,
}

impl FluxumSide {
    /// A side talking to the server at `url`.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        FluxumSide { url: url.into() }
    }
}

impl Side for FluxumSide {
    fn name(&self) -> &'static str {
        "fluxum"
    }

    fn client(&self, seed: u64) -> Result<Box<dyn BenchClient>, String> {
        // Distinct token → distinct identity under the dev provider, so
        // `seed` names the same logical user across runs.
        let token = format!("bench-user-{seed}");
        let connection = Connection::connect(&self.url, token.as_bytes(), demo_schemas())
            .map_err(|e| format!("fluxum connect: {e}"))?;
        Ok(Box::new(FluxumClient {
            connection: Arc::new(connection),
        }))
    }
}

struct FluxumClient {
    connection: Arc<Connection>,
}

impl BenchClient for FluxumClient {
    fn add_task(&mut self, title: &str) -> Result<(), String> {
        self.connection
            .call_reducer("add_task", vec![FluxValue::Str(title.to_owned())])
            .map_err(|e| format!("add_task: {e}"))
    }

    fn send_chat(&mut self, channel: u32, content: &str) -> Result<(), String> {
        self.connection
            .call_reducer(
                "send_chat",
                vec![
                    FluxValue::I64(i64::from(channel)),
                    FluxValue::Str(content.to_owned()),
                ],
            )
            .map_err(|e| format!("send_chat: {e}"))
    }

    fn subscribe_chat(
        &mut self,
        channel: u32,
        on_message: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<(), String> {
        // Register the listener BEFORE the subscription: `InitialData` and
        // the first `TxUpdate`s must not race past it.
        self.connection.on(
            "ChatMessage:insert",
            Box::new(move |row, _old| {
                if let Some(content) = chat_content(row) {
                    on_message(content);
                }
            }),
        );
        self.connection
            .subscribe(&[&format!("SELECT * FROM ChatMessage WHERE channel = {channel}")])
            .map_err(|e| format!("subscribe_chat: {e}"))?;
        Ok(())
    }
}

/// Decode `content` out of a `ChatMessage` row (id, sender, channel,
/// content, sent_at). `None` if the row does not decode — the caller treats
/// that as "not a bench message" rather than a panic inside a listener.
fn chat_content(row: &[u8]) -> Option<&str> {
    let mut reader = FluxBinReader::new(row);
    reader.read_u64().ok()?; // id
    reader.read_identity().ok()?; // sender
    reader.read_u32().ok()?; // channel
    reader.read_str().ok()
}

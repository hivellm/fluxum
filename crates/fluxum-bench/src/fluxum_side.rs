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
            tasks: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            read_keys: Vec::new(),
            read_cursor: 0,
        }))
    }
}

struct FluxumClient {
    connection: Arc<Connection>,
    /// The app-side live view of this user's tasks (id → title), fed by row
    /// listeners — what a real Fluxum application holds, and what the
    /// NFR-11 "in-process hot read" reads from.
    tasks: Arc<std::sync::Mutex<std::collections::HashMap<u64, String>>>,
    /// Round-robin cursor + key snapshot for the read loop.
    read_keys: Vec<u64>,
    read_cursor: usize,
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

    fn prepare_reads(&mut self, rows: u32) -> Result<(), String> {
        // Materialize the live view BEFORE subscribing: InitialData and the
        // inserts below all flow through the listener into the map.
        let tasks = Arc::clone(&self.tasks);
        self.connection.on(
            "Task:insert",
            Box::new(move |row, _old| {
                if let Some((id, title)) = task_row(row) {
                    tasks
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(id, title.to_owned());
                }
            }),
        );
        // owner_only visibility (DM-060): this subscription delivers only
        // this user's rows, server-side.
        self.connection
            .subscribe(&["SELECT * FROM Task"])
            .map_err(|e| format!("subscribe Task: {e}"))?;
        for i in 0..rows {
            self.add_task(&format!("seed {i}"))?;
        }
        // The acked inserts' TxUpdates may still be in flight; wait for the
        // view to catch up.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let seen = self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            if seen >= rows as usize {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(format!("live view has {seen}/{rows} rows after 10 s"));
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        self.read_keys = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        self.read_keys.sort_unstable();
        Ok(())
    }

    fn hot_read(&mut self) -> Result<String, String> {
        let Some(&key) = self.read_keys.get(self.read_cursor % self.read_keys.len().max(1))
        else {
            return Err("hot_read before prepare_reads".to_owned());
        };
        self.read_cursor = self.read_cursor.wrapping_add(1);
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("task {key} vanished from the live view"))
    }

    fn load_my_data(&mut self) -> Result<u32, String> {
        // `subscribe` returns once `InitialData` is applied to the local
        // cache — the whole "open the app" operation, timed by the caller.
        self.connection
            .subscribe(&["SELECT * FROM Task"])
            .map_err(|e| format!("subscribe Task: {e}"))?;
        Ok(self.connection.rows("Task").len() as u32)
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

/// Decode `(id, title)` out of a `Task` row (id, owner, title, done).
fn task_row(row: &[u8]) -> Option<(u64, &str)> {
    let mut reader = FluxBinReader::new(row);
    let id = reader.read_u64().ok()?;
    reader.read_identity().ok()?; // owner
    let title = reader.read_str().ok()?;
    Some((id, title))
}

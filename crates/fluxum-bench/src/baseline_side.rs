//! The workload driver's client for the baseline stack: plain HTTP for
//! writes, a WebSocket for the live subscription — exactly what an incumbent
//! app's frontend speaks, driven through the same [`BenchClient`] trait as
//! the Fluxum side so the behavior is identical by construction.

use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::baseline::protocol::{AddTask, ChatPush, SendChat};
use crate::workload::{BenchClient, Side};

/// The baseline [`Side`]: a running `fluxum-bench baseline-server`.
pub struct BaselineSide {
    /// `http://127.0.0.1:<port>`.
    base_url: String,
    /// `"postgres"` or `"sqlite"` — which database backs the server.
    kind: &'static str,
}

impl BaselineSide {
    /// A side talking to the baseline server at `base_url`, backed by `kind`.
    #[must_use]
    pub fn new(base_url: impl Into<String>, kind: &'static str) -> Self {
        BaselineSide {
            base_url: base_url.into(),
            kind,
        }
    }
}

impl Side for BaselineSide {
    fn name(&self) -> &'static str {
        self.kind
    }

    fn client(&self, seed: u64) -> Result<Box<dyn BenchClient>, String> {
        Ok(Box::new(BaselineClient {
            // One agent per client session: keep-alive connection reuse,
            // like any competent HTTP client (TST-091).
            agent: ureq::agent(),
            base_url: self.base_url.clone(),
            user: format!("bench-user-{seed}"),
            sockets: Vec::new(),
        }))
    }
}

struct BaselineClient {
    agent: ureq::Agent,
    base_url: String,
    user: String,
    /// Streams of live subscriptions, shut down on drop so their reader
    /// threads exit.
    sockets: Vec<(TcpStream, Arc<AtomicBool>)>,
}

impl BaselineClient {
    fn post(&self, path: &str, body: impl serde::Serialize) -> Result<(), String> {
        let response = self
            .agent
            .post(&format!("{}{path}", self.base_url))
            .send_json(body)
            .map_err(|e| format!("POST {path}: {e}"))?;
        if response.status() >= 300 {
            return Err(format!("POST {path}: HTTP {}", response.status()));
        }
        Ok(())
    }
}

impl BenchClient for BaselineClient {
    fn add_task(&mut self, title: &str) -> Result<(), String> {
        self.post(
            "/tasks",
            AddTask {
                user: self.user.clone(),
                title: title.to_owned(),
            },
        )
    }

    fn send_chat(&mut self, channel: u32, content: &str) -> Result<(), String> {
        self.post(
            "/chat",
            SendChat {
                user: self.user.clone(),
                channel,
                content: content.to_owned(),
            },
        )
    }

    fn subscribe_chat(
        &mut self,
        channel: u32,
        on_message: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<(), String> {
        let host = self
            .base_url
            .strip_prefix("http://")
            .ok_or_else(|| format!("baseline url {} is not http://", self.base_url))?
            .to_owned();
        let stream = TcpStream::connect(&host).map_err(|e| format!("ws connect {host}: {e}"))?;
        let ws_url = format!("ws://{host}/subscribe?channel={channel}");
        // The server subscribes the socket to the fan-out BEFORE completing
        // the upgrade, so once this handshake returns no message is missed.
        let (mut socket, _response) =
            tungstenite::client(&ws_url, stream.try_clone().map_err(|e| e.to_string())?)
                .map_err(|e| format!("ws handshake {ws_url}: {e}"))?;

        let closing = Arc::new(AtomicBool::new(false));
        let closed = Arc::clone(&closing);
        std::thread::spawn(move || {
            loop {
                match socket.read() {
                    Ok(tungstenite::Message::Text(frame)) => {
                        if let Ok(push) = serde_json::from_str::<ChatPush>(frame.as_str()) {
                            on_message(&push.content);
                        }
                    }
                    Ok(tungstenite::Message::Close(_)) | Err(_) => return,
                    Ok(_) => {}
                }
                if closed.load(Ordering::Relaxed) {
                    return;
                }
            }
        });
        self.sockets.push((stream, closing));
        Ok(())
    }

    fn prepare_reads(&mut self, rows: u32) -> Result<(), String> {
        // The SQL side needs the rows to exist; the "view" is the database.
        for i in 0..rows {
            self.add_task(&format!("seed {i}"))?;
        }
        Ok(())
    }

    fn hot_read(&mut self) -> Result<String, String> {
        let response = self
            .agent
            .get(&format!("{}/task", self.base_url))
            .query("user", &self.user)
            .call()
            .map_err(|e| format!("GET /task: {e}"))?;
        let body: serde_json::Value = response
            .into_json()
            .map_err(|e| format!("GET /task body: {e}"))?;
        body.get("title")
            .and_then(|t| t.as_str())
            .map(str::to_owned)
            .ok_or_else(|| "GET /task: no title in response".to_owned())
    }

    fn load_my_data(&mut self) -> Result<u32, String> {
        let response = self
            .agent
            .get(&format!("{}/tasks", self.base_url))
            .query("user", &self.user)
            .call()
            .map_err(|e| format!("GET /tasks: {e}"))?;
        let body: serde_json::Value = response
            .into_json()
            .map_err(|e| format!("GET /tasks body: {e}"))?;
        body.as_array()
            .map(|rows| rows.len() as u32)
            .ok_or_else(|| "GET /tasks: response is not an array".to_owned())
    }
}

impl Drop for BaselineClient {
    fn drop(&mut self) {
        for (stream, closing) in &self.sockets {
            closing.store(true, Ordering::Relaxed);
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

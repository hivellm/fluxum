//! The connection runtime: wire encoding, initial/TxUpdate application,
//! optimistic resolution, offline replay, the reconnect supervisor and
//! session establishment — split from the parent module to honour the
//! file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Send one message over whichever write half is live. On HTTP the POST
/// response carries this request's replies — they are routed exactly as the
/// push stream's frames are, into the pending map the caller is waiting on.
pub(super) fn send_message(shared: &Shared, message: &ClientMessage) -> Result<(), Error> {
    let framed = encode_framed(message)?;
    let session = {
        let mut guard = shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.as_mut() {
            // `None` means the connection dropped and the reconnect loop has
            // not re-established it yet: fail fast, not into the void.
            None => return Err(Error::Disconnected),
            Some(WriteHalf::Tcp(stream)) => {
                stream.write_all(&framed)?;
                stream.flush()?;
                return Ok(());
            }
            Some(WriteHalf::Http { session }) => session.clone(),
        }
        // The lock is released here: an HTTP round-trip must not serialize
        // every other sender behind it.
    };
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;
    let response = endpoint.post(Some(&session), &framed).map_err(Error::Io)?;
    match response.status {
        200 => {
            for message in response.messages {
                route(shared, message);
            }
            Ok(())
        }
        // RPC-007: an unknown/expired session is a 404; the push-stream loop
        // notices the same death and re-establishes.
        404 => Err(Error::Disconnected),
        status => Err(Error::Http(status)),
    }
}

/// Encode a client message into one length-prefixed frame.
pub(super) fn encode_framed(message: &ClientMessage) -> Result<Vec<u8>, Error> {
    let body = message.encode()?;
    let mut framed = Vec::with_capacity(body.len() + 4);
    // A message body is far under the 16 MB frame cap; a `TooLarge` here
    // would be a client-side bug, surfaced rather than unwrapped.
    FrameCodec::default().encode_into(&body, &mut framed)?;
    Ok(framed)
}

/// Group a `TableUpdate` list into per-`query_id` cache diffs (SUB-001).
pub(super) fn group_by_query(tables: &[TableUpdate]) -> Vec<(u32, Vec<TableDiff>)> {
    let mut by_query: Vec<(u32, Vec<TableDiff>)> = Vec::new();
    for table in tables {
        let diff = TableDiff {
            table: table.table_name.clone(),
            inserts: table.inserts.iter().map(<[u8]>::to_vec).collect(),
            deletes: table.deletes.iter().map(<[u8]>::to_vec).collect(),
        };
        match by_query.iter_mut().find(|(id, _)| *id == table.query_id) {
            Some((_, diffs)) => diffs.push(diff),
            None => by_query.push((table.query_id, vec![diff])),
        }
    }
    by_query
}

/// Apply an `InitialData` snapshot to the cache, feeding the resume tracker
/// (SPEC-021 CS-020) and honouring a `cache_reset` (CS-022): when the server
/// answered a `Resume` whose offset predated its retained window, the snapshot
/// REPLACES the query's rows rather than merging, so the query's cached rows
/// are cleared before it is applied.
pub(super) fn apply_initial(shared: &Shared, initial: &InitialData) -> Vec<RowEvent> {
    let reset = shared
        .resume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_initial(initial);

    let mut cache = shared
        .cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut events = Vec::new();
    let by_query = group_by_query(&initial.tables);
    if reset {
        // CS-022: drop each query's prior rows before the fresh snapshot.
        for (query_id, _) in &by_query {
            events.extend(cache.release_query(*query_id));
        }
    }
    events.extend(cache.apply_tx(&by_query, None));
    events
}

/// Apply a server-initiated `TxUpdate` to the cache, attributing rows by their
/// stamped `query_id` (SDK-044) and advancing the resume offsets (CS-020).
/// When the commit is this client's own — `caller` matches the session
/// identity — the matching optimistic overlay drops in the same batch
/// (SPEC-021 CS-011), which is what makes the swap flicker-free.
pub(super) fn apply_tx_update(shared: &Shared, update: &TxUpdate) -> Vec<RowEvent> {
    shared
        .resume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_update(update);

    let own = {
        let identity = shared
            .identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update.caller == *identity && update.caller != [0u8; 32]
    };
    let mut cache = shared
        .cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cache.apply_tx(
        &group_by_query(&update.tables),
        own.then_some(update.reducer_name.as_str()),
    )
}

/// Resolve an optimistic call's outcome (SPEC-021 CS-011): `Ok` acknowledges
/// the queued call and confirms its overlay — dropping it now or holding it
/// until the authoritative update lands; `Err` removes the call (a rejection
/// is definitive, never retried) and rolls the overlay back, then tells the
/// rejected listeners. Results for non-optimistic calls pass through
/// untouched.
pub(super) fn resolve_optimistic(shared: &Shared, result: &ReducerResult) {
    let no_subscriptions = shared
        .subs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty();
    let Some((key, layer, reducer)) = ({
        let mut optimistic = shared
            .optimistic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        optimistic.in_flight.remove(&result.id).map(|key| {
            let reducer = optimistic
                .queue
                .pending()
                .iter()
                .find(|c| c.idempotency_key == key)
                .map(|c| c.reducer.clone())
                .unwrap_or_default();
            optimistic.queue.acknowledge(&key);
            let layer = optimistic.layers.remove(&key);
            (key, layer, reducer)
        })
    }) else {
        return;
    };

    let events = {
        let mut cache = shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (&result.outcome, layer) {
            (Ok(()), Some(layer)) => cache.confirm(layer, no_subscriptions),
            (Err(_), Some(layer)) => cache.rollback(layer),
            (_, None) => Vec::new(),
        }
    };
    dispatch_shared(shared, events);
    persist_state(shared);

    if let Err(error) = &result.outcome {
        let listeners = shared
            .rejected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for listener in listeners.iter() {
            listener(&reducer, &key, error);
        }
    }
}

/// Replay every queued optimistic call on a fresh session, in submission
/// order, each under its ORIGINAL idempotency key (SPEC-021 CS-032): a call
/// whose first send actually applied before the session died is deduplicated
/// by the server, so the replay is exactly-once.
pub(super) fn replay_offline(shared: &Arc<Shared>) {
    // Hydration guard (CS-040 identity keying): if the persisted state
    // belonged to a DIFFERENT identity than this session authenticated as,
    // its queued mutations must not replay as the new user. Discard them
    // and clear the store; the cache reconcile has already limited rows to
    // what the new identity may see.
    let hydrated = shared
        .hydrated_identity
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(expected) = hydrated {
        let current = *shared
            .identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if expected != current {
            {
                let mut optimistic = shared
                    .optimistic
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let keys: Vec<String> = optimistic
                    .queue
                    .pending()
                    .iter()
                    .map(|c| c.idempotency_key.clone())
                    .collect();
                for key in keys {
                    optimistic.queue.acknowledge(&key);
                }
                optimistic.layers.clear();
            }
            if let Some(store) = &shared.persist {
                store.clear();
            }
            return;
        }
    }
    let attempts: Vec<ClientMessage> = {
        let mut optimistic = shared
            .optimistic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys: Vec<String> = optimistic
            .queue
            .pending()
            .iter()
            .map(|c| c.idempotency_key.clone())
            .collect();
        keys.into_iter()
            .filter_map(|key| {
                let id = shared.alloc_id();
                let message = optimistic.queue.attempt(&key, id)?;
                optimistic.in_flight.insert(id, key);
                Some(message)
            })
            .collect()
    };
    for message in attempts {
        if send_message(shared, &message).is_err() {
            // The fresh session died mid-replay: the rest stay queued, keys
            // untouched, and the next reconnect picks them up.
            break;
        }
    }
    // A fresh session just adopted new query ids and rows (and the queue's
    // attempt counts moved): capture the post-establishment state.
    persist_state(shared);
}

/// Write the client's durable state through to the local store (CS-040):
/// the meta blob (identity + offline queue) and one blob per live
/// subscription (SQL, applied offset, held rows). A no-op without
/// persistence.
///
/// Locks are taken ONE at a time, copying out — never nested — so this can
/// run from any thread without ordering constraints. A write racing a
/// concurrent update may persist a snapshot a moment old; hydration always
/// reconciles against fresh server data, so staleness costs a slightly
/// larger net-diff, never correctness.
pub(super) fn persist_state(shared: &Shared) {
    let Some(store) = &shared.persist else { return };
    let identity = *shared
        .identity
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let queue = shared
        .optimistic
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .queue
        .snapshot();
    store.save_meta(&PersistedMeta {
        identity: identity.to_vec(),
        queue,
    });

    let subs: Vec<(String, u32)> = shared
        .subs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|e| (e.sql.clone(), e.server_id))
        .collect();
    for (sql, server_id) in subs {
        let tx_offset = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied_offset(server_id)
            .unwrap_or(0);
        let snapshots = shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .query_snapshot(server_id);
        let tables = snapshots
            .into_iter()
            .map(|snapshot| {
                (
                    snapshot.table,
                    snapshot
                        .rows
                        .into_iter()
                        .map(serde_bytes::ByteBuf::from)
                        .collect(),
                )
            })
            .collect();
        store.save_query(&PersistedQuery {
            sql,
            tx_offset,
            tables,
        });
    }
}

/// Dispatch events to listeners without a `Connection` handle (reader thread).
pub(super) fn dispatch_shared(shared: &Shared, events: Vec<RowEvent>) {
    if events.is_empty() {
        return;
    }
    let listeners = shared
        .listeners
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for event in events {
        let (table, kind): (&str, &str) = match &event {
            RowEvent::Insert { table, .. } => (table, "insert"),
            RowEvent::Delete { table, .. } => (table, "delete"),
            RowEvent::Update { table, .. } => (table, "update"),
        };
        if let Some(set) = listeners.get(&format!("{table}:{kind}")) {
            for listener in set {
                match &event {
                    RowEvent::Insert { row, .. } | RowEvent::Delete { row, .. } => {
                        listener(row, None)
                    }
                    RowEvent::Update { old, row, .. } => listener(row, Some(old)),
                }
            }
        }
    }
}

// --- The session stream ------------------------------------------------------

/// A blocking, buffered decoder of server messages off one TCP socket. Owned
/// by whichever code is currently reading — the handshake reads it inline,
/// then hands it (buffer and all) to the read loop, so no bytes are lost at
/// the transition.
pub(super) struct MessageStream {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl MessageStream {
    pub(super) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            codec: FrameCodec::default(),
            buf: Vec::new(),
        }
    }

    /// The next decodable server message; `None` on EOF, socket error, or a
    /// framing violation (which desynchronizes the stream — stop reading).
    pub(super) fn next(&mut self) -> Option<ServerMessage> {
        let mut chunk = [0u8; 8192];
        loop {
            // Drain every complete frame currently buffered before reading.
            loop {
                let (frame_body, consumed) = match self.codec.decode(&self.buf) {
                    Ok(Some((Frame::Body(body), consumed))) => (Some(body.to_vec()), consumed),
                    Ok(Some((Frame::KeepAlive, consumed))) => (None, consumed),
                    Ok(None) => break,
                    Err(_) => return None,
                };
                self.buf.drain(..consumed);
                if let Some(body) = frame_body
                    && let Ok(message) = ServerMessage::decode(&body)
                {
                    return Some(message);
                }
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return None, // clean EOF
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }
}

/// The background thread: read the session until it drops, then — policy
/// permitting — bring it back and carry on, forever, until the `Connection`
/// is dropped.
pub(super) fn supervise(mut messages: ReadHalf, shared: &Arc<Shared>) {
    loop {
        while let Some(message) = messages.next() {
            route(shared, message);
        }
        let was_http = messages.is_http();

        // Over TCP the session died with the socket: fail senders fast and
        // unblock in-flight callers. Over HTTP only the PUSH STREAM died —
        // the session may be fine and POSTs keep working, so nothing is
        // failed unless recovery below gives up.
        if !was_http {
            shared.set_writer(None);
            fail_all(shared);
        }

        if shared.is_closed() || !shared.policy.enabled {
            shared.set_writer(None);
            fail_all(shared);
            return;
        }
        let next = if was_http {
            recover_http(shared)
        } else {
            reestablish_tcp(shared)
        };
        match next {
            Some(live) => messages = live,
            None => {
                shared.set_writer(None);
                fail_all(shared);
                return;
            }
        }
    }
}

/// The TCP reconnect loop: connect, authenticate, resubscribe, reconcile —
/// with exponential backoff between attempts (SDK-047). `None` when the
/// client was closed or the policy's attempt budget ran out.
pub(super) fn reestablish_tcp(shared: &Arc<Shared>) -> Option<ReadHalf> {
    let mut attempt: u32 = 0;
    loop {
        if let Some(max) = shared.policy.max_attempts
            && attempt >= max
        {
            return None;
        }
        let delay = if attempt == 0 {
            Duration::ZERO
        } else {
            backoff_delay(attempt - 1, &shared.policy)
        };
        if !sleep_unless_closed(shared, delay) {
            return None;
        }
        match try_tcp_session(shared) {
            Ok(messages) => return Some(messages),
            Err(_) => {
                attempt += 1;
                // REP-033: that endpoint did not yield a session — rotate to
                // the next replica-set member so the next attempt discovers
                // the new primary after a failover.
                if shared.endpoints.len() > 1 {
                    shared.endpoint_ix.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
}

/// The HTTP push-stream recovery loop. Each attempt first tries the BLIP
/// path — reattach the GET stream under the surviving session and `Resume`
/// each subscription from its applied offset (SPEC-021 CS-021) — and falls
/// back to a full re-establishment (new session, resubscribe, reconcile)
/// when the session is gone.
pub(super) fn recover_http(shared: &Arc<Shared>) -> Option<ReadHalf> {
    let mut attempt: u32 = 0;
    loop {
        if let Some(max) = shared.policy.max_attempts
            && attempt >= max
        {
            return None;
        }
        let delay = if attempt == 0 {
            Duration::ZERO
        } else {
            backoff_delay(attempt - 1, &shared.policy)
        };
        if !sleep_unless_closed(shared, delay) {
            return None;
        }

        let endpoint = shared.http.as_ref()?;
        let session = {
            let guard = shared
                .writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match guard.as_ref() {
                Some(WriteHalf::Http { session }) => Some(session.clone()),
                _ => None,
            }
        };

        if let Some(session) = session {
            match endpoint.open_stream(&session) {
                Ok((200, Some(stream))) => {
                    shared.set_push_socket(stream.socket().ok());
                    if resume_subscriptions(shared, &session).is_ok() {
                        // Calls queued during the blip never went out (their
                        // POST failed): send them now, same keys.
                        replay_offline(shared);
                        return Some(ReadHalf::Http(stream));
                    }
                    // The session survived but a subscription did not
                    // (SUB unknown query) — rebuild from scratch below.
                }
                // The server still counts the previous stream (409) or is not
                // reachable: back off and retry the blip before giving the
                // session up for dead.
                Ok((409, _)) | Err(_) => {
                    attempt += 1;
                    continue;
                }
                // 404: the session is gone — full re-establishment.
                Ok((_, _)) => {}
            }
        }

        match try_http_session(shared) {
            Ok(messages) => return Some(messages),
            Err(_) => attempt += 1,
        }
    }
}

/// Sleep for `delay`, waking early if the connection is closed. Returns
/// whether the caller should proceed (false = closed).
pub(super) fn sleep_unless_closed(shared: &Shared, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    let mut closed = shared
        .closed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !*closed {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        closed = shared
            .wake
            .wait_timeout(closed, deadline - now)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
    }
    false
}

/// One TCP reconnect attempt: a full session bring-up. Any failure aborts the
/// attempt; the loop backs off and tries again.
pub(super) fn try_tcp_session(shared: &Arc<Shared>) -> Result<ReadHalf, Error> {
    // REP-033: dial the endpoint the reconnect loop currently points at —
    // after a failover this rotates until it lands on the new primary.
    let endpoint = {
        let ix = shared.endpoint_ix.load(Ordering::SeqCst) % shared.endpoints.len();
        &shared.endpoints[ix]
    };
    let stream = TcpStream::connect(endpoint)?;
    let _ = stream.set_nodelay(true);
    // A half-dead handshake must not wedge `Drop`: bound reads until the
    // session is live, then go back to blocking indefinitely.
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut writer = stream.try_clone()?;
    let mut messages = MessageStream::new(stream);

    // 1. Authenticate (the shared writer is still None — send directly).
    let (auth_id, auth) = shared.authenticate_message();
    writer.write_all(&encode_framed(&auth)?)?;
    writer.flush()?;
    let identity = loop {
        match messages.next() {
            None => return Err(Error::Disconnected),
            Some(ServerMessage::AuthResult(result)) if result.id == auth_id => {
                break result.identity;
            }
            Some(ServerMessage::Error(err)) if err.id == Some(auth_id) => {
                return Err(err.into());
            }
            Some(_) => {} // nothing else belongs to a session this young
        }
    };

    // 2. Resubscribe every active query, in registration order — always
    // BEFORE reconcile: InitialData must cover every active query, or
    // reconciliation reads the gap as rows having been deleted.
    let sqls = shared.replay_sqls();
    let mut initials: Vec<InitialData> = Vec::new();
    if !sqls.is_empty() {
        let sub_id = shared.alloc_id();
        let subscribe = Subscribe {
            id: sub_id,
            queries: sqls.clone(),
        };
        writer.write_all(&encode_framed(&ClientMessage::Subscribe(subscribe))?)?;
        writer.flush()?;
        while initials.len() < sqls.len() {
            match messages.next() {
                None => return Err(Error::Disconnected),
                Some(ServerMessage::InitialData(initial)) if initial.id == sub_id => {
                    initials.push(initial);
                }
                Some(ServerMessage::Error(err)) if err.id == Some(sub_id) => {
                    return Err(err.into());
                }
                Some(_) => {}
            }
        }
    }

    // 3. Reconcile under the new session's ids.
    let events = adopt_session(shared, &initials, sqls.len(), identity)?;

    // Session live: back to blocking reads, reopen the shared writer, and only
    // then tell the application what changed while it was away.
    messages.stream.set_read_timeout(None)?;
    shared.set_push_socket(messages.stream.try_clone().ok());
    shared.set_writer(Some(WriteHalf::Tcp(writer)));
    dispatch_shared(shared, events);
    replay_offline(shared);
    Ok(ReadHalf::Tcp(messages))
}

/// One full HTTP session bring-up: authenticate a fresh session over POST,
/// resubscribe over POST, reconcile, then open the push stream. Also the
/// FIRST session's path, where the replay set is simply empty.
pub(super) fn try_http_session(shared: &Arc<Shared>) -> Result<ReadHalf, Error> {
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;

    // 1. Authenticate: the response carries the AuthResult and mints the
    // session token (RPC-007).
    let (_, auth) = shared.authenticate_message();
    let response = endpoint
        .post(None, &encode_framed(&auth)?)
        .map_err(Error::Io)?;
    if response.status != 200 {
        return Err(Error::Http(response.status));
    }
    let session = response.session.clone();
    let mut identity: Option<[u8; 32]> = None;
    for message in response.messages {
        match message {
            ServerMessage::AuthResult(result) => identity = Some(result.identity),
            ServerMessage::Error(err) => return Err(err.into()),
            _ => {}
        }
    }
    let identity = identity.ok_or(Error::Disconnected)?;
    let session = session.ok_or(Error::Disconnected)?;

    // 2. Resubscribe the replay set in one POST; its response body carries
    // every InitialData.
    let sqls = shared.replay_sqls();
    let mut initials: Vec<InitialData> = Vec::new();
    if !sqls.is_empty() {
        let sub_id = shared.alloc_id();
        let subscribe = ClientMessage::Subscribe(Subscribe {
            id: sub_id,
            queries: sqls.clone(),
        });
        let response = endpoint
            .post(Some(&session), &encode_framed(&subscribe)?)
            .map_err(Error::Io)?;
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        for message in response.messages {
            match message {
                ServerMessage::InitialData(initial) if initial.id == sub_id => {
                    initials.push(initial);
                }
                ServerMessage::Error(err) if err.id == Some(sub_id) => {
                    return Err(err.into());
                }
                _ => {}
            }
        }
    }

    // 3. Reconcile under the new session's ids.
    let events = adopt_session(shared, &initials, sqls.len(), identity)?;

    // 4. Open the push stream; anything committed between the subscribe POST
    // and here sits in the session's outbound queue and arrives on attach.
    let (status, stream) = endpoint.open_stream(&session).map_err(Error::Io)?;
    let Some(stream) = stream else {
        return Err(Error::Http(status));
    };
    shared.set_push_socket(stream.socket().ok());
    shared.set_writer(Some(WriteHalf::Http { session }));
    dispatch_shared(shared, events);
    replay_offline(shared);
    Ok(ReadHalf::Http(stream))
}

/// The HTTP blip path (SPEC-021 CS-021): the session survived a dropped push
/// stream, so ask the server to replay what each subscription missed, from
/// its highest APPLIED offset. Deltas come back as `TxUpdate`s and apply on
/// the normal path; a compacted-away offset comes back as a `cache_reset`
/// snapshot (CS-022) which `apply_initial` already honours. Any error —
/// typically SUB unknown query, a session that did not really survive —
/// tells the caller to rebuild from scratch.
pub(super) fn resume_subscriptions(shared: &Arc<Shared>, session: &str) -> Result<(), Error> {
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;
    let targets: Vec<(u32, Option<u64>)> = {
        let subs = shared
            .subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let resume = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subs.iter()
            .map(|e| (e.server_id, resume.applied_offset(e.server_id)))
            .collect()
    };
    for (server_id, offset) in targets {
        // Nothing applied yet — nothing to resume; the stream reattach alone
        // covers it.
        let Some(from_offset) = offset else { continue };
        let id = shared.alloc_id();
        let resume = ClientMessage::Resume(Resume {
            id,
            query_id: server_id,
            from_offset,
        });
        let response = endpoint
            .post(Some(session), &encode_framed(&resume)?)
            .map_err(Error::Io)?;
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        for message in response.messages {
            match message {
                ServerMessage::InitialData(initial) => {
                    let events = apply_initial(shared, &initial);
                    dispatch_shared(shared, events);
                }
                ServerMessage::Error(_) => return Err(Error::Disconnected),
                other => route(shared, other),
            }
        }
    }
    persist_state(shared);
    Ok(())
}

/// Adopt a fresh session's `InitialData` set: rebuild the resume tracker,
/// reconcile the cache to the net difference (SDK-047), re-attribute rows to
/// the NEW query ids, re-point the application handles, and store the
/// re-derived identity. Returns the events to dispatch once the writer is
/// live.
pub(super) fn adopt_session(
    shared: &Shared,
    initials: &[InitialData],
    expected_queries: usize,
    identity: [u8; 32],
) -> Result<Vec<RowEvent>, Error> {
    // The fresh server-assigned ids, in reply order — one per query, matching
    // the registry's order because the Subscribe listed them in that order.
    let mut new_ids: Vec<u32> = Vec::new();
    let mut per_query: Vec<(u32, Vec<TableSnapshot>)> = Vec::new();
    let mut merged: Vec<TableSnapshot> = Vec::new();
    for initial in initials {
        for table in &initial.tables {
            let rows: Vec<Vec<u8>> = table.inserts.iter().map(<[u8]>::to_vec).collect();
            let snapshot = TableSnapshot {
                table: table.table_name.clone(),
                rows: rows.clone(),
            };
            match per_query.iter_mut().find(|(id, _)| *id == table.query_id) {
                Some((_, snaps)) => snaps.push(snapshot),
                None => {
                    new_ids.push(table.query_id);
                    per_query.push((table.query_id, vec![snapshot]));
                }
            }
            match merged.iter_mut().find(|s| s.table == table.table_name) {
                Some(existing) => existing.rows.extend(rows),
                None => merged.push(TableSnapshot {
                    table: table.table_name.clone(),
                    rows,
                }),
            }
        }
    }
    if new_ids.len() != expected_queries {
        // The reply shape does not match the replay set; treat the attempt as
        // failed rather than mis-binding handles.
        return Err(Error::Disconnected);
    }

    {
        let mut resume = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *resume = ResumeTracker::new();
        for initial in initials {
            let _ = resume.apply_initial(initial);
        }
    }
    let events = {
        let mut cache = shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.reset_queries();
        let events = cache.reconcile(&merged);
        for (query_id, snapshots) in &per_query {
            cache.track_query(*query_id, snapshots);
        }
        events
    };
    {
        let mut subs = shared
            .subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (entry, new_id) in subs.iter_mut().zip(&new_ids) {
            entry.server_id = *new_id;
        }
    }
    *shared
        .identity
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = identity;
    Ok(events)
}

pub(super) fn route(shared: &Shared, message: ServerMessage) {
    match message {
        ServerMessage::TxUpdate(update) => {
            let events = apply_tx_update(shared, &update);
            dispatch_shared(shared, events);
            persist_state(shared);
        }
        ServerMessage::ReducerResult(result) => {
            // Optimistic calls resolve here, on the reader, not in a waiter:
            // their submitter got a key back, not a handle to await.
            resolve_optimistic(shared, &result);
            if let Some(tx) = shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&result.id)
            {
                let _ = tx.send(Ok(ServerMessage::ReducerResult(result)));
            }
        }
        ServerMessage::TxUpdateLight(_) => {}
        ServerMessage::Error(err) => {
            // A null-id error is server-initiated and belongs to nobody.
            if let Some(id) = err.id
                && let Some(tx) = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&id)
            {
                let _ = tx.send(Err(err));
            }
        }
        other => {
            if let Some(id) = reply_id(&other)
                && let Some(tx) = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&id)
            {
                let _ = tx.send(Ok(other));
            }
        }
    }
}

/// The echoed request id of a correlated server reply.
pub(super) fn reply_id(message: &ServerMessage) -> Option<u32> {
    match message {
        ServerMessage::AuthResult(m) => Some(m.id),
        ServerMessage::ReducerResult(m) => Some(m.id),
        ServerMessage::InitialData(m) => Some(m.id),
        _ => None,
    }
}

/// Authenticate a brand-new first TCP session, reading the stream inline (the
/// reader thread does not exist yet).
pub(super) fn tcp_authenticate(
    shared: &Shared,
    messages: &mut MessageStream,
) -> Result<[u8; 32], Error> {
    let (auth_id, auth) = shared.authenticate_message();
    send_message(shared, &auth)?;
    loop {
        match messages.next() {
            None => return Err(Error::Disconnected),
            Some(ServerMessage::AuthResult(result)) if result.id == auth_id => {
                return Ok(result.identity);
            }
            Some(ServerMessage::Error(err)) if err.id == Some(auth_id) => {
                return Err(err.into());
            }
            Some(_) => {} // nothing else belongs to a session this young
        }
    }
}

/// Fail every in-flight request when the connection drops, so no caller hangs.
///
/// Clearing the pending map drops each request's `Sender`; the waiting
/// `recv()` then returns `Err`, which [`Connection::request`] maps to
/// [`Error::Disconnected`]. No sentinel message is needed.
///
/// Optimistic calls are NOT failed — that is their point. The dead session's
/// request ids are forgotten (nothing will ever answer them), but the calls
/// stay queued under their stable keys and their overlays stay rendered; the
/// reconnect replays them exactly-once (CS-032).
pub(super) fn fail_all(shared: &Shared) {
    shared
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    shared
        .optimistic
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .in_flight
        .clear();
}

/// Parse a client URL: `fluxum://host:port` (TCP) or `http://host:port`
/// (Streamable HTTP), both with an explicit port.
pub(super) fn parse_url(url: &str) -> Result<Target, Error> {
    let (rest, is_http) = if let Some(rest) = url.strip_prefix("fluxum://") {
        (rest, false)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (rest, true)
    } else {
        return Err(Error::Url(format!(
            "expected fluxum://host:port or http://host:port, got {url}"
        )));
    };
    let addr = rest.trim_end_matches('/');
    if !addr.contains(':') {
        return Err(Error::Url(format!("missing port in {url}")));
    }
    Ok(if is_http {
        Target::Http(addr.to_owned())
    } else {
        Target::Tcp(addr.to_owned())
    })
}

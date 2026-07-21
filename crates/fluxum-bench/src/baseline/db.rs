//! Database layer of the baseline app: PostgreSQL or SQLite behind one enum.
//!
//! TST-091 requires the incumbent competently tuned, and the tuning lives
//! here where it cannot be lost: every benchmark query runs as a prepared
//! statement over a pooled connection (sqlx prepares-and-caches per
//! connection automatically), and the schema below carries the indexes the
//! benchmark queries need. What is deliberately NOT here: exotic tuning no
//! ordinary deployment ships with.

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

/// PostgreSQL DDL: covering indexes for every benchmark query (TST-091).
const PG_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS task (
    id     BIGSERIAL PRIMARY KEY,
    owner  TEXT NOT NULL,
    title  TEXT NOT NULL,
    done   BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS task_owner ON task(owner);
CREATE TABLE IF NOT EXISTS chat_message (
    id       BIGSERIAL PRIMARY KEY,
    sender   TEXT NOT NULL,
    channel  INTEGER NOT NULL,
    content  TEXT NOT NULL,
    sent_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS chat_channel ON chat_message(channel, id);
";

/// SQLite DDL — same shape, SQLite spellings.
const SQLITE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS task (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    owner  TEXT NOT NULL,
    title  TEXT NOT NULL,
    done   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS task_owner ON task(owner);
CREATE TABLE IF NOT EXISTS chat_message (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sender   TEXT NOT NULL,
    channel  INTEGER NOT NULL,
    content  TEXT NOT NULL,
    sent_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS chat_channel ON chat_message(channel, id);
";

/// The baseline's database: PostgreSQL (client/server) or SQLite (embedded).
#[derive(Clone)]
pub enum Db {
    /// A PostgreSQL pool; change fan-out rides `NOTIFY`.
    Postgres(PgPool),
    /// An embedded SQLite pool (WAL mode); fan-out is app-side post-commit.
    Sqlite(SqlitePool),
}

impl Db {
    /// Connect and apply the schema. `url` is `postgres://…` or
    /// `sqlite://<path>` (`sqlite::memory:` for a throwaway).
    pub async fn connect(url: &str, max_connections: u32) -> Result<Db, String> {
        if url.starts_with("postgres") {
            let pool = connect_pg_with_retry(url, max_connections).await?;
            for statement in split_ddl(PG_SCHEMA) {
                sqlx::query(&statement)
                    .execute(&pool)
                    .await
                    .map_err(|e| format!("postgres DDL: {e}"))?;
            }
            Ok(Db::Postgres(pool))
        } else if url.starts_with("sqlite") {
            // WAL journal is SQLite's honest server-ish configuration:
            // readers do not block the writer and commits are group-durable
            // — the tuning any competent embedded deployment uses.
            let options: SqliteConnectOptions = url
                .parse::<SqliteConnectOptions>()
                .map_err(|e| format!("sqlite url {url}: {e}"))?
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
            let pool = SqlitePoolOptions::new()
                .max_connections(max_connections)
                .connect_with(options)
                .await
                .map_err(|e| format!("sqlite connect {url}: {e}"))?;
            for statement in split_ddl(SQLITE_SCHEMA) {
                sqlx::query(&statement)
                    .execute(&pool)
                    .await
                    .map_err(|e| format!("sqlite DDL: {e}"))?;
            }
            Ok(Db::Sqlite(pool))
        } else {
            Err(format!("unsupported database url: {url}"))
        }
    }

    /// Short stable name for reports.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Db::Postgres(_) => "postgres",
            Db::Sqlite(_) => "sqlite",
        }
    }

    /// `INSERT` one task, committed before returning (the acked small write).
    pub async fn add_task(&self, owner: &str, title: &str) -> Result<(), String> {
        match self {
            Db::Postgres(pool) => sqlx::query("INSERT INTO task (owner, title) VALUES ($1, $2)")
                .bind(owner)
                .bind(title)
                .execute(pool)
                .await
                .map(drop)
                .map_err(|e| format!("add_task: {e}")),
            Db::Sqlite(pool) => sqlx::query("INSERT INTO task (owner, title) VALUES (?1, ?2)")
                .bind(owner)
                .bind(title)
                .execute(pool)
                .await
                .map(drop)
                .map_err(|e| format!("add_task: {e}")),
        }
    }

    /// One hot single-row read: the newest task for `owner`, over the
    /// `task_owner` index — the incumbent's indexed point SELECT on a
    /// cached page (TST-092 c).
    pub async fn task_title(&self, owner: &str) -> Result<Option<String>, String> {
        use sqlx::Row as _;
        let row = match self {
            Db::Postgres(pool) => {
                sqlx::query("SELECT title FROM task WHERE owner = $1 ORDER BY id DESC LIMIT 1")
                    .bind(owner)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| format!("task_title: {e}"))?
                    .map(|r| r.get::<String, _>(0))
            }
            Db::Sqlite(pool) => {
                sqlx::query("SELECT title FROM task WHERE owner = ?1 ORDER BY id DESC LIMIT 1")
                    .bind(owner)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| format!("task_title: {e}"))?
                    .map(|r| r.get::<String, _>(0))
            }
        };
        Ok(row)
    }

    /// Every task for `owner` (id, title, done) — the "load my data" read
    /// (TST-092 d), over the `task_owner` index.
    pub async fn tasks_for(&self, owner: &str) -> Result<Vec<(i64, String, bool)>, String> {
        use sqlx::Row as _;
        match self {
            Db::Postgres(pool) => sqlx::query("SELECT id, title, done FROM task WHERE owner = $1")
                .bind(owner)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("tasks_for: {e}"))?
                .iter()
                .map(|r| Ok((r.get(0), r.get(1), r.get(2))))
                .collect(),
            Db::Sqlite(pool) => sqlx::query("SELECT id, title, done FROM task WHERE owner = ?1")
                .bind(owner)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("tasks_for: {e}"))?
                .iter()
                .map(|r| Ok((r.get(0), r.get(1), r.get::<i64, _>(2) != 0)))
                .collect(),
        }
    }

    /// `INSERT` one chat message, committed before returning. On PostgreSQL
    /// the same statement issues the `NOTIFY` (`pg_notify`) **inside the
    /// insert's transaction** — delivery is tied to the commit exactly like
    /// a trigger-based production setup, and the app server hears it through
    /// its `LISTEN` connection.
    pub async fn send_chat(&self, sender: &str, channel: u32, content: &str) -> Result<(), String> {
        match self {
            Db::Postgres(pool) => sqlx::query(
                "WITH ins AS (
                     INSERT INTO chat_message (sender, channel, content) VALUES ($1, $2, $3)
                 )
                 SELECT pg_notify('chat', json_build_object('channel', $2::int4, 'content', $3::text)::text)",
            )
            .bind(sender)
            .bind(i64::from(channel))
            .bind(content)
            .execute(pool)
            .await
            .map(drop)
            .map_err(|e| format!("send_chat: {e}")),
            Db::Sqlite(pool) => {
                sqlx::query("INSERT INTO chat_message (sender, channel, content) VALUES (?1, ?2, ?3)")
                    .bind(sender)
                    .bind(i64::from(channel))
                    .bind(content)
                    .execute(pool)
                    .await
                    .map(drop)
                    .map_err(|e| format!("send_chat: {e}"))
            }
        }
    }
}

/// Connect to PostgreSQL, retrying for up to ~30 s. The retry is not a
/// benchmark kindness — it is what any competent incumbent deployment does,
/// and the cold-read workload restarts the database out from under the app
/// server, exactly the window where a first attempt lands on a PostgreSQL
/// that is listening (Docker's proxy accepts early) but not yet ready.
pub async fn connect_pg_with_retry(url: &str, max_connections: u32) -> Result<PgPool, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match PgPoolOptions::new()
            .max_connections(max_connections)
            // The pool hands out connections lazily; a health check here
            // makes "connected" mean "the server answers queries".
            .test_before_acquire(true)
            .connect(url)
            .await
        {
            Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                Ok(_) => return Ok(pool),
                Err(e) if std::time::Instant::now() < deadline => {
                    drop(pool);
                    tracing_note(&e);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => return Err(format!("postgres probe {url}: {e}")),
            },
            Err(e) if std::time::Instant::now() < deadline => {
                tracing_note(&e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(format!("postgres connect {url}: {e}")),
        }
    }
}

/// Startup retries are normal during a cold restart; keep them visible
/// without failing anything.
fn tracing_note(error: &sqlx::Error) {
    eprintln!("baseline: postgres not ready yet ({error}); retrying");
}

/// Split a DDL blob into statements (sqlx prepared queries are one statement
/// each). Naive on purpose: the schemas above contain no literal `;`.
fn split_ddl(ddl: &str) -> Vec<String> {
    ddl.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_splits_into_clean_statements() {
        let statements = split_ddl(PG_SCHEMA);
        assert_eq!(statements.len(), 4);
        assert!(statements[0].starts_with("CREATE TABLE"));
        assert!(statements[1].starts_with("CREATE INDEX"));
        assert_eq!(split_ddl(SQLITE_SCHEMA).len(), 4);
    }
}

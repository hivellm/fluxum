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
            let pool = PgPoolOptions::new()
                .max_connections(max_connections)
                .connect(url)
                .await
                .map_err(|e| format!("postgres connect {url}: {e}"))?;
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

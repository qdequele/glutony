//! Postgres connection pool and embedded migrations.
//!
//! Queries in this crate are runtime-checked (`sqlx::query`, `sqlx::query_as`) so the
//! crate builds without a `DATABASE_URL` at compile time.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Maximum number of pooled connections.
pub const MAX_CONNECTIONS: u32 = 10;

/// How long a request waits for a free connection before giving up.
///
/// sqlx defaults to 30 s, which is far too long for a request an operator is
/// watching: when Postgres is unreachable or failing over, every call to the
/// gateway hangs for half a minute and the admin UI simply appears frozen. Failing
/// in a few seconds with a clear 500 is the better behaviour, and the gateway
/// surfaces it as an upstream error.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Recycle a connection that has been idle this long.
///
/// Postgres restarts, failovers and container replacements leave the pool holding
/// sockets to a server that no longer exists. Combined with `test_before_acquire`,
/// this means a dead connection is discovered and replaced rather than handed to a
/// request.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

fn options() -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .idle_timeout(Some(IDLE_TIMEOUT))
        // Cheap round-trip that turns "the server went away" into a transparent
        // reconnect instead of an error surfaced to the caller.
        .test_before_acquire(true)
}

/// Connect to Postgres eagerly (fails fast when the database is unreachable).
pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = options().connect(database_url).await?;
    Ok(pool)
}

/// Build a pool that only opens connections when the first query runs.
///
/// Useful for tests that exercise DB-free code paths and for tools that may never
/// touch the database.
pub fn connect_lazy(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = options().connect_lazy(database_url)?;
    Ok(pool)
}

/// The migrations embedded from the repository's `migrations/` directory.
pub fn migrator() -> &'static sqlx::migrate::Migrator {
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
    &MIGRATOR
}

/// Apply pending migrations from `/migrations/*.sql`.
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    migrator().run(pool).await?;
    Ok(())
}

/// Cheap liveness probe (`SELECT 1`).
pub async fn ping(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

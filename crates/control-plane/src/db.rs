//! Postgres connection pool and embedded migrations.
//!
//! Queries in this crate are runtime-checked (`sqlx::query`, `sqlx::query_as`) so the
//! crate builds without a `DATABASE_URL` at compile time.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Maximum number of pooled connections.
pub const MAX_CONNECTIONS: u32 = 10;

/// Connect to Postgres eagerly (fails fast when the database is unreachable).
pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect(database_url)
        .await?;
    Ok(pool)
}

/// Build a pool that only opens connections when the first query runs.
///
/// Useful for tests that exercise DB-free code paths and for tools that may never
/// touch the database.
pub fn connect_lazy(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect_lazy(database_url)?;
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

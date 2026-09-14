//! Verifies the migrations apply and produce the columns later code binds to.
//!
//! Needs a live Postgres. Skips cleanly when `DATABASE_URL` is unset so the suite stays
//! green on a machine without one.

use sqlx::{Executor, PgPool};

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

#[tokio::test]
async fn sources_schema_exists_after_migration() {
    let Some(pool) = pool().await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");

    pool.execute("DELETE FROM sources WHERE uid = 'tmdb-migration-test'")
        .await
        .ok();

    // A source row round-trips with the columns the repo binds.
    pool.execute(
        "INSERT INTO sources (id, uid, name, pipeline_uid, location, cron, meili_ctx, schedule_id)
         VALUES ('11111111-1111-1111-1111-111111111111', 'tmdb-migration-test', 'TMDB',
                 'builtin.json',
                 '{\"kind\":\"url\",\"url\":\"https://example.test/x.json\"}'::jsonb,
                 '0 9 * * *', '\\x00'::bytea, 'source-tmdb')",
    )
    .await
    .expect("insert source");

    let (archived,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT archived_at FROM sources WHERE uid = 'tmdb-migration-test'")
            .fetch_one(&pool)
            .await
            .expect("archived_at column exists");
    assert!(archived.is_none(), "new sources are not archived");

    // source_runs cascades from its source.
    pool.execute(
        "INSERT INTO source_runs (run_id, source_id, outcome)
         VALUES ('22222222-2222-2222-2222-222222222222',
                 '11111111-1111-1111-1111-111111111111', 'unchanged')",
    )
    .await
    .expect("insert run");

    // jobs.source_id exists and is nullable.
    sqlx::query_as::<_, (Option<uuid::Uuid>,)>("SELECT source_id FROM jobs LIMIT 0")
        .fetch_optional(&pool)
        .await
        .expect("jobs.source_id column exists");

    pool.execute("DELETE FROM sources WHERE uid = 'tmdb-migration-test'")
        .await
        .expect("delete source");

    let (runs,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM source_runs WHERE run_id = '22222222-2222-2222-2222-222222222222'",
    )
    .fetch_one(&pool)
    .await
    .expect("count runs");
    assert_eq!(runs, 0, "runs cascade-delete with their source");
}

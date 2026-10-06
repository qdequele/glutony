//! Rollback script for migrations 0003 and 0004. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55433/postgres cargo test -p meili-ingest-control-plane --test rollback`.

use std::str::FromStr;

use meili_ingest_control_plane::db;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

struct TestDb {
    pool: PgPool,
    schema: String,
    admin: PgPool,
}

impl TestDb {
    async fn drop_schema(self) {
        self.pool.close().await;
        let _ = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await;
        self.admin.close().await;
    }
}

async fn setup() -> Option<TestDb> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set");
        return None;
    };
    let schema = format!("cp_rb_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    Some(TestDb {
        pool,
        schema,
        admin,
    })
}

const SCRIPT: &str = include_str!("../../../scripts/rollback-lab-seams.sql");

#[tokio::test]
async fn rollback_refuses_undelivered_events_then_restores_the_old_schema() {
    let Some(t) = setup().await else { return };
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO lab_events (id, body) VALUES ($1, '{}'::jsonb)")
        .bind(id)
        .execute(&t.pool)
        .await
        .unwrap();

    let err = sqlx::raw_sql(AssertSqlSafe(SCRIPT))
        .execute(&t.pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("undelivered"), "{err}");
    let still: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns \
        WHERE table_schema = current_schema() AND column_name = 'tenant_id'",
    )
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(still, 4, "a refused rollback changes nothing");

    sqlx::query("UPDATE lab_events SET delivered_at = now()")
        .execute(&t.pool)
        .await
        .unwrap();
    sqlx::raw_sql(AssertSqlSafe(SCRIPT))
        .execute(&t.pool)
        .await
        .unwrap();

    let old: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns \
        WHERE table_schema = current_schema() AND column_name = 'project_id'",
    )
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(old, 4);
    let lab: Option<String> = sqlx::query_scalar("SELECT to_regclass('lab_events')::text")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(lab, None);
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&t.pool)
            .await
            .unwrap();
    assert_eq!(
        versions,
        vec![1, 2],
        "the previous binary sees only the migrations it knows"
    );
    let index: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes \
        WHERE schemaname = current_schema() AND indexname = 'pipelines_uid_project'",
    )
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(index, 1);
    t.drop_schema().await;
}

#[tokio::test]
async fn force_rolls_back_despite_undelivered_events() {
    let Some(t) = setup().await else { return };
    sqlx::query("INSERT INTO lab_events (id, body) VALUES ($1, '{}'::jsonb)")
        .bind(Uuid::new_v4())
        .execute(&t.pool)
        .await
        .unwrap();
    let mut conn = t.pool.acquire().await.unwrap();
    sqlx::query("SET glutony.rollback_force = 'on'")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(AssertSqlSafe(SCRIPT))
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    t.drop_schema().await;
}

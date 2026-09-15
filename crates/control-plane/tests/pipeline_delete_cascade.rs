//! Deleting a pipeline archives the sources that feed from it, and is never blocked.
//!
//! Skips cleanly when `DATABASE_URL` is unset.

use meili_ingest_control_plane::pipelines::PipelineRepo;
use meili_ingest_control_plane::sources::{NewSource, SourceRepo};
use meili_ingest_plugin_sdk::{PipelineDefinition, StepDefinition};
use meili_ingest_source::model::Location;
use sqlx::{Executor, PgPool};
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");
    pool.execute("DELETE FROM sources WHERE uid LIKE 'cascade-%'")
        .await
        .expect("clean sources");
    pool.execute("DELETE FROM pipelines WHERE uid LIKE 'cascade-%'")
        .await
        .expect("clean pipelines");
    Some(pool)
}

fn pipeline(uid: &str) -> PipelineDefinition {
    PipelineDefinition {
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        version: 1,
        trigger: None,
        steps: vec![StepDefinition::new("parse", "json_parser")],
        builtin: false,
        project_id: Some("cascade-proj".into()),
    }
}

fn source(uid: &str, pipeline_uid: &str) -> NewSource {
    NewSource {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        project_id: Some("cascade-proj".into()),
        pipeline_uid: pipeline_uid.to_string(),
        location: Location::Url {
            url: "https://example.test/feed.json".into(),
            method: None,
            headers: Default::default(),
        },
        cron: "0 3 * * *".into(),
        timezone: "UTC".into(),
        index_name: None,
        fetch_auth: Some(vec![9, 9, 9]),
        meili_ctx: vec![8, 8, 8],
        schedule_id: format!("source-{uid}"),
    }
}

#[tokio::test]
async fn deleting_a_pipeline_archives_its_sources_and_still_succeeds() {
    let Some(pool) = pool().await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    let pipelines = PipelineRepo::new(pool.clone());
    let sources = SourceRepo::new(pool.clone());

    pipelines
        .upsert(&pipeline("cascade-doomed"))
        .await
        .expect("create pipeline");
    pipelines
        .upsert(&pipeline("cascade-safe"))
        .await
        .expect("create pipeline");
    sources
        .insert(&source("cascade-s1", "cascade-doomed"))
        .await
        .expect("insert");
    sources
        .insert(&source("cascade-s2", "cascade-doomed"))
        .await
        .expect("insert");
    sources
        .insert(&source("cascade-s3", "cascade-safe"))
        .await
        .expect("insert");

    // The delete must succeed: a source referencing the pipeline never blocks it.
    let (deleted, archived) = pipelines
        .delete_cascading("cascade-doomed", Some("cascade-proj"))
        .await
        .expect("delete");
    assert!(deleted, "the pipeline is deleted, not refused");
    assert_eq!(archived.len(), 2, "both dependent sources archived");

    let visible: Vec<String> = sources
        .list(Some("cascade-proj"), false)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.definition.uid)
        .collect();
    assert!(!visible.contains(&"cascade-s1".to_string()));
    assert!(!visible.contains(&"cascade-s2".to_string()));
    assert!(
        visible.contains(&"cascade-s3".to_string()),
        "a source on another pipeline is untouched"
    );

    // Credentials survive so the source can be repointed and unarchived.
    let kept = sources
        .get("cascade-s1", Some("cascade-proj"))
        .await
        .expect("get")
        .expect("still exists");
    assert_eq!(kept.meili_ctx, vec![8, 8, 8]);
    assert_eq!(kept.fetch_auth.as_deref(), Some(&[9u8, 9, 9][..]));
    assert!(kept.definition.archived_at.is_some());
    assert!(kept.definition.paused, "an archived source must not fire");
}

#[tokio::test]
async fn deleting_a_pipeline_with_no_sources_reports_nothing_archived() {
    let Some(pool) = pool().await else {
        return;
    };
    let pipelines = PipelineRepo::new(pool.clone());
    pipelines
        .upsert(&pipeline("cascade-lonely"))
        .await
        .expect("create pipeline");

    let (deleted, archived) = pipelines
        .delete_cascading("cascade-lonely", Some("cascade-proj"))
        .await
        .expect("delete");
    assert!(deleted);
    assert!(archived.is_empty());
}

#[tokio::test]
async fn deleting_a_missing_pipeline_reports_false() {
    let Some(pool) = pool().await else {
        return;
    };
    let (deleted, archived) = PipelineRepo::new(pool)
        .delete_cascading("cascade-never-existed", Some("cascade-proj"))
        .await
        .expect("delete");
    assert!(!deleted);
    assert!(archived.is_empty());
}

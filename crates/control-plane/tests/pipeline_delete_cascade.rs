//! Deleting a pipeline archives the sources that feed from it, and is never blocked.
//!
//! Skips cleanly when `DATABASE_URL` is unset.

use meili_ingest_control_plane::pipelines::PipelineRepo;
use meili_ingest_control_plane::sources::{NewSource, SourceRepo};
use meili_ingest_plugin_sdk::{PipelineDefinition, StepDefinition};
use meili_ingest_source::model::Location;
use sqlx::PgPool;
use uuid::Uuid;

/// Tests in one file run concurrently against the same database, so each owns a name
/// namespace and cleans only that.
///
/// The trailing dash in the `LIKE` pattern is load-bearing: a bare `casc-a%` would also
/// match `casc-arch-…`, and one test would wipe another's rows mid-run.
async fn pool(prefix: &str) -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");
    for table in ["sources", "pipelines"] {
        let sql = match table {
            "sources" => "DELETE FROM sources WHERE uid LIKE $1",
            _ => "DELETE FROM pipelines WHERE uid LIKE $1",
        };
        sqlx::query(sql)
            .bind(format!("{prefix}-%"))
            .execute(&pool)
            .await
            .expect("clean");
    }
    Some(pool)
}

fn pipeline(uid: &str, project: &str) -> PipelineDefinition {
    PipelineDefinition {
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        version: 1,
        trigger: None,
        steps: vec![StepDefinition::new("parse", "json_parser")],
        builtin: false,
        tenant_id: Some(project.to_string()),
    }
}

fn source(uid: &str, pipeline_uid: &str, project: &str) -> NewSource {
    NewSource {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        tenant_id: Some(project.to_string()),
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
        schedule_id: format!("source-{uid}"),
    }
}

#[tokio::test]
async fn deleting_a_pipeline_archives_its_sources_and_still_succeeds() {
    let Some(pool) = pool("casc-arch").await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    // A project of its own, so a concurrent test's archive can never match these rows.
    let proj = "casc-arch-proj";
    let pipelines = PipelineRepo::new(pool.clone());
    let sources = SourceRepo::new(pool.clone());

    pipelines
        .upsert(&pipeline("casc-arch-doomed", proj))
        .await
        .expect("create pipeline");
    pipelines
        .upsert(&pipeline("casc-arch-safe", proj))
        .await
        .expect("create pipeline");
    sources
        .insert(&source("casc-arch-s1", "casc-arch-doomed", proj))
        .await
        .expect("insert");
    sources
        .insert(&source("casc-arch-s2", "casc-arch-doomed", proj))
        .await
        .expect("insert");
    sources
        .insert(&source("casc-arch-s3", "casc-arch-safe", proj))
        .await
        .expect("insert");

    // The delete must succeed: a source referencing the pipeline never blocks it.
    let (deleted, archived) = pipelines
        .delete_cascading("casc-arch-doomed", Some(proj))
        .await
        .expect("delete");
    assert!(deleted, "the pipeline is deleted, not refused");
    assert_eq!(archived.len(), 2, "both dependent sources archived");

    let visible: Vec<String> = sources
        .list(Some(proj), false)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.definition.uid)
        .collect();
    assert!(!visible.contains(&"casc-arch-s1".to_string()));
    assert!(!visible.contains(&"casc-arch-s2".to_string()));
    assert!(
        visible.contains(&"casc-arch-s3".to_string()),
        "a source on another pipeline is untouched"
    );

    // Credentials survive with the row (archive is one-way: it never runs again).
    let kept = sources
        .get("casc-arch-s1", Some(proj))
        .await
        .expect("get")
        .expect("still exists");
    assert_eq!(kept.fetch_auth.as_deref(), Some(&[9u8, 9, 9][..]));
    assert!(kept.definition.archived_at.is_some());
    assert!(kept.definition.paused, "an archived source must not fire");
}

#[tokio::test]
async fn deleting_a_pipeline_with_no_sources_reports_nothing_archived() {
    let Some(pool) = pool("casc-lonely").await else {
        return;
    };
    let proj = "casc-lonely-proj";
    let pipelines = PipelineRepo::new(pool.clone());
    pipelines
        .upsert(&pipeline("casc-lonely-p", proj))
        .await
        .expect("create pipeline");

    let (deleted, archived) = pipelines
        .delete_cascading("casc-lonely-p", Some(proj))
        .await
        .expect("delete");
    assert!(deleted);
    assert!(archived.is_empty());
}

#[tokio::test]
async fn deleting_a_missing_pipeline_reports_false() {
    let Some(pool) = pool("casc-missing").await else {
        return;
    };
    let (deleted, archived) = PipelineRepo::new(pool)
        .delete_cascading("casc-missing-never-existed", Some("casc-missing-proj"))
        .await
        .expect("delete");
    assert!(!deleted);
    assert!(archived.is_empty());
}

#[tokio::test]
async fn deleting_a_global_pipeline_archives_the_tenant_sources_that_fall_back_to_it() {
    let Some(pool) = pool("casc-glob").await else {
        return;
    };
    let pipelines = PipelineRepo::new(pool.clone());
    let sources = SourceRepo::new(pool.clone());
    let (falls_back, shadows) = ("casc-glob-proj-a", "casc-glob-proj-b");

    // A global pipeline, and tenant B's own pipeline of the same uid.
    let mut global = pipeline("casc-glob-feed", "unused");
    global.tenant_id = None;
    pipelines.upsert(&global).await.expect("create global");
    pipelines
        .upsert(&pipeline("casc-glob-feed", shadows))
        .await
        .expect("create tenant copy");

    // A global source, tenant A's source (resolves to the global pipeline by
    // fallback) and tenant B's source (resolves to B's own pipeline).
    let mut global_source = source("casc-glob-s-global", "casc-glob-feed", "unused");
    global_source.tenant_id = None;
    let fallback_source = source("casc-glob-s-a", "casc-glob-feed", falls_back);
    let shadowed_source = source("casc-glob-s-b", "casc-glob-feed", shadows);
    for s in [&global_source, &fallback_source, &shadowed_source] {
        sources.insert(s).await.expect("insert");
        // A source starts paused; these are running, as after the gateway created
        // their schedules.
        assert!(sources.set_paused(s.id, false).await.expect("unpause"));
    }

    let (deleted, mut archived) = pipelines
        .delete_cascading("casc-glob-feed", None)
        .await
        .expect("delete");
    assert!(deleted);
    archived.sort();
    let mut expected = vec![global_source.id, fallback_source.id];
    expected.sort();
    assert_eq!(
        archived, expected,
        "the global source and the tenant source without a pipeline of its own are \
         archived, so the gateway deletes both schedules"
    );

    let a = sources
        .get("casc-glob-s-a", Some(falls_back))
        .await
        .expect("get")
        .expect("still exists");
    assert!(a.definition.archived_at.is_some());
    assert!(a.definition.paused, "an archived source must not fire");

    // Tenant B's pipeline still exists, so its source keeps running.
    let b = sources
        .get("casc-glob-s-b", Some(shadows))
        .await
        .expect("get")
        .expect("still exists");
    assert!(b.definition.archived_at.is_none());
    assert!(!b.definition.paused);
    assert!(
        pipelines
            .get("casc-glob-feed", Some(shadows))
            .await
            .expect("get")
            .is_some(),
        "the tenant's own pipeline is untouched by the global delete"
    );
}

//! `SourceRepo` against a live Postgres.
//!
//! Skips cleanly when `DATABASE_URL` is unset so the suite stays green without a server.

use meili_ingest_control_plane::sources::{NewSource, RunRecord, SourceRepo};
use meili_ingest_source::model::{IncrementalState, Location, RunOutcome};
use sqlx::PgPool;
use uuid::Uuid;

/// Tests in one file run concurrently against the same database, so each takes its own
/// uid namespace and cleans only that. A shared `LIKE 'test-repo-%'` wipe would delete
/// rows out from under a neighbouring test.
async fn repo(prefix: &str) -> Option<SourceRepo> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");
    // The trailing dash matters: a bare `tr-a%` also matches `tr-arch-p1`, so one test
    // would wipe another's rows. Every uid in this file is `<prefix>-<suffix>`.
    sqlx::query("DELETE FROM sources WHERE uid LIKE $1")
        .bind(format!("{prefix}-%"))
        .execute(&pool)
        .await
        .expect("clean");
    Some(SourceRepo::new(pool))
}

fn new_source(uid: &str, project_id: Option<&str>, pipeline_uid: &str) -> NewSource {
    NewSource {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: format!("source {uid}"),
        description: None,
        project_id: project_id.map(str::to_owned),
        pipeline_uid: pipeline_uid.to_string(),
        location: Location::Url {
            url: "https://example.test/feed.json".into(),
            method: None,
            headers: Default::default(),
        },
        cron: "30 0 * * *".into(),
        timezone: "UTC".into(),
        index_name: None,
        fetch_auth: Some(vec![1, 2, 3]),
        schedule_id: format!("source-{uid}"),
    }
}

#[tokio::test]
async fn insert_then_get_roundtrips() {
    let Some(repo) = repo("tr-a").await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    let new = new_source("tr-a-1", None, "builtin.json");
    let id = new.id;
    let stored = repo.insert(&new).await.expect("insert");
    assert_eq!(stored.definition.uid, "tr-a-1");
    assert_eq!(stored.definition.id, id);
    assert!(
        stored.definition.paused,
        "a source is created paused, then unpaused once its schedule exists"
    );

    let got = repo
        .get("tr-a-1", None)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(got.fetch_auth.as_deref(), Some(&[1u8, 2, 3][..]));
    assert!(got.state.etag.is_none(), "a fresh source has no state");
}

#[tokio::test]
async fn the_same_uid_may_exist_globally_and_per_tenant() {
    let Some(repo) = repo("tr-dup").await else {
        return;
    };
    repo.insert(&new_source("tr-dup-1", None, "builtin.json"))
        .await
        .expect("global insert");
    repo.insert(&new_source("tr-dup-1", Some("proj-1"), "builtin.json"))
        .await
        .expect("tenant insert");

    let global = repo
        .get("tr-dup-1", None)
        .await
        .expect("get")
        .expect("exists");
    let tenant = repo
        .get("tr-dup-1", Some("proj-1"))
        .await
        .expect("get")
        .expect("exists");
    assert_ne!(global.definition.id, tenant.definition.id);
    assert_eq!(tenant.definition.project_id.as_deref(), Some("proj-1"));
}

#[tokio::test]
async fn list_scopes_to_the_tenant_and_hides_archived() {
    let Some(repo) = repo("tr-list").await else {
        return;
    };
    repo.insert(&new_source("tr-list-global", None, "builtin.json"))
        .await
        .expect("insert");
    repo.insert(&new_source(
        "tr-list-tenant",
        Some("proj-2"),
        "builtin.json",
    ))
    .await
    .expect("insert");
    repo.insert(&new_source("tr-list-other", Some("proj-3"), "builtin.json"))
        .await
        .expect("insert");

    let listed = repo.list(Some("proj-2"), false).await.expect("list");
    let uids: Vec<&str> = listed.iter().map(|s| s.definition.uid.as_str()).collect();
    assert!(uids.contains(&"tr-list-tenant"), "tenant's own source");
    assert!(
        uids.contains(&"tr-list-global"),
        "global sources are visible"
    );
    assert!(
        !uids.contains(&"tr-list-other"),
        "another tenant's source must not leak"
    );
}

#[tokio::test]
async fn archive_for_pipeline_stamps_and_hides() {
    let Some(repo) = repo("tr-arch").await else {
        return;
    };
    repo.insert(&new_source("tr-arch-p1", Some("proj-4"), "doomed.pipeline"))
        .await
        .expect("insert");
    repo.insert(&new_source("tr-arch-p2", Some("proj-4"), "doomed.pipeline"))
        .await
        .expect("insert");
    repo.insert(&new_source("tr-arch-p3", Some("proj-4"), "safe.pipeline"))
        .await
        .expect("insert");

    let archived = repo
        .archive_for_pipeline("doomed.pipeline", Some("proj-4"))
        .await
        .expect("archive");
    assert_eq!(archived.len(), 2, "both dependents archived");

    let visible: Vec<String> = repo
        .list(Some("proj-4"), false)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.definition.uid)
        .collect();
    assert!(!visible.contains(&"tr-arch-p1".to_string()));
    assert!(visible.contains(&"tr-arch-p3".to_string()), "unaffected");

    let with_archived: Vec<String> = repo
        .list(Some("proj-4"), true)
        .await
        .expect("list")
        .into_iter()
        .map(|s| s.definition.uid)
        .collect();
    assert!(with_archived.contains(&"tr-arch-p1".to_string()));

    // The credentials survive, so the source can be repointed and unarchived.
    let kept = repo
        .get("tr-arch-p1", Some("proj-4"))
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        kept.fetch_auth.as_deref(),
        Some(&[1u8, 2, 3][..]),
        "the sealed credential is retained"
    );
    assert!(kept.definition.archived_at.is_some());
}

#[tokio::test]
async fn save_state_advances_the_incremental_state() {
    let Some(repo) = repo("tr-state").await else {
        return;
    };
    let new = new_source("tr-state-1", None, "builtin.json");
    let id = new.id;
    repo.insert(&new).await.expect("insert");

    repo.save_state(
        id,
        &IncrementalState {
            etag: Some("\"v9\"".into()),
            last_modified: None,
            hash: Some("abc123".into()),
        },
    )
    .await
    .expect("save state");

    let got = repo
        .get("tr-state-1", None)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(got.state.etag.as_deref(), Some("\"v9\""));
    assert_eq!(got.state.hash.as_deref(), Some("abc123"));
}

#[tokio::test]
async fn runs_are_recorded_and_listed_newest_first() {
    let Some(repo) = repo("tr-runs").await else {
        return;
    };
    let new = new_source("tr-runs-1", None, "builtin.json");
    let source_id = new.id;
    repo.insert(&new).await.expect("insert");

    for (outcome, items) in [
        (RunOutcome::Unchanged, 0),
        (RunOutcome::Ingested, 3),
        (RunOutcome::Failed, 0),
    ] {
        repo.record_run(&RunRecord {
            run_id: Uuid::new_v4(),
            source_id,
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            outcome,
            items,
            job_ids: if items > 0 {
                vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()]
            } else {
                vec![]
            },
            error: (outcome == RunOutcome::Failed).then(|| "boom".to_string()),
        })
        .await
        .expect("record run");
    }

    let runs = repo.list_runs(source_id, 10).await.expect("list runs");
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0].outcome, RunOutcome::Failed, "newest first");
    assert_eq!(runs[0].error.as_deref(), Some("boom"));
    let ingested = runs
        .iter()
        .find(|r| r.outcome == RunOutcome::Ingested)
        .expect("ingested run");
    assert_eq!(ingested.job_ids.len(), 3, "job ids round-trip");

    // The source's denormalized status mirrors the newest run.
    let got = repo
        .get("tr-runs-1", None)
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(got.last_status.as_deref(), Some("failed"));
    assert!(got.last_run_at.is_some());
}

#[tokio::test]
async fn delete_removes_the_source_and_its_runs() {
    let Some(repo) = repo("tr-del").await else {
        return;
    };
    let new = new_source("tr-del-1", None, "builtin.json");
    let source_id = new.id;
    repo.insert(&new).await.expect("insert");
    repo.record_run(&RunRecord {
        run_id: Uuid::new_v4(),
        source_id,
        started_at: chrono::Utc::now(),
        finished_at: None,
        outcome: RunOutcome::Unchanged,
        items: 0,
        job_ids: vec![],
        error: None,
    })
    .await
    .expect("record run");

    assert!(repo.delete("tr-del-1", None).await.expect("delete"));
    assert!(
        repo.get("tr-del-1", None).await.expect("get").is_none(),
        "gone"
    );
    assert!(
        repo.list_runs(source_id, 10)
            .await
            .expect("runs")
            .is_empty(),
        "runs cascade"
    );
    assert!(
        !repo.delete("tr-del-1", None).await.expect("delete"),
        "deleting twice reports false"
    );
}

#[tokio::test]
async fn load_for_run_finds_by_id_and_skips_archived() {
    let Some(repo) = repo("tr-load").await else {
        return;
    };
    let new = new_source("tr-load-1", Some("proj-5"), "doomed2.pipeline");
    let id = new.id;
    repo.insert(&new).await.expect("insert");

    let loaded = repo.load_for_run(id).await.expect("load").expect("exists");
    assert_eq!(loaded.definition.uid, "tr-load-1");
    assert_eq!(loaded.fetch_auth.as_deref(), Some(&[1u8, 2, 3][..]));

    repo.archive_for_pipeline("doomed2.pipeline", Some("proj-5"))
        .await
        .expect("archive");
    assert!(
        repo.load_for_run(id).await.expect("load").is_none(),
        "an archived source must never be run"
    );
}

#[tokio::test]
async fn a_tenant_cannot_modify_or_delete_a_global_source() {
    let Some(repo) = repo("tr-scope").await else {
        return;
    };
    // The same uid exists globally and for one tenant.
    repo.insert(&new_source("tr-scope-1", None, "builtin.json"))
        .await
        .expect("global insert");
    repo.insert(&new_source("tr-scope-1", Some("proj-6"), "builtin.json"))
        .await
        .expect("tenant insert");

    // The tenant renames and deletes "its" source...
    let patch = meili_ingest_control_plane::sources::SourcePatch {
        name: Some("renamed by tenant".into()),
        ..Default::default()
    };
    repo.update("tr-scope-1", Some("proj-6"), &patch)
        .await
        .expect("update")
        .expect("tenant row exists");
    assert!(
        repo.delete("tr-scope-1", Some("proj-6"))
            .await
            .expect("delete")
    );

    // ...and the global row is untouched.
    let global = repo
        .get("tr-scope-1", None)
        .await
        .expect("get")
        .expect("the global source must survive a tenant delete");
    assert_eq!(global.definition.name, "source tr-scope-1", "not renamed");
    assert!(global.definition.project_id.is_none());

    // A tenant with no row of its own cannot reach the global one either.
    assert!(
        repo.update("tr-scope-1", Some("proj-7"), &patch)
            .await
            .expect("update")
            .is_none(),
        "no tenant row, so nothing to update"
    );
    assert!(
        !repo
            .delete("tr-scope-1", Some("proj-7"))
            .await
            .expect("delete")
    );
    assert!(repo.get("tr-scope-1", None).await.expect("get").is_some());
}

#[tokio::test]
async fn archiving_a_tenant_pipeline_leaves_global_sources_alone() {
    let Some(repo) = repo("tr-garch").await else {
        return;
    };
    repo.insert(&new_source("tr-garch-global", None, "tr-garch.pipeline"))
        .await
        .expect("global insert");
    repo.insert(&new_source(
        "tr-garch-tenant",
        Some("proj-8"),
        "tr-garch.pipeline",
    ))
    .await
    .expect("tenant insert");

    let archived = repo
        .archive_for_pipeline("tr-garch.pipeline", Some("proj-8"))
        .await
        .expect("archive");
    assert_eq!(archived.len(), 1, "only the tenant's own source");

    let global = repo
        .get("tr-garch-global", None)
        .await
        .expect("get")
        .expect("exists");
    assert!(
        global.definition.archived_at.is_none(),
        "a tenant deleting its pipeline must not archive a global source"
    );
}

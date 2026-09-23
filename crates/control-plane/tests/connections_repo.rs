//! `ConnectionRepo` against a live Postgres.
//!
//! Skips cleanly when `DATABASE_URL` is unset. Each test owns a `<prefix>-` namespace:
//! cargo runs a file's tests concurrently against one database, and the trailing dash
//! keeps `cn-a%` from matching `cn-arch-…`.

use meili_ingest_control_plane::connections::{ConnectionPatch, ConnectionRepo, NewConnection};
use meili_ingest_control_plane::pipelines::PipelineRepo;
use meili_ingest_plugin_sdk::{PipelineDefinition, StepDefinition};
use sqlx::PgPool;
use uuid::Uuid;

async fn pool(prefix: &str) -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");
    for sql in [
        "DELETE FROM meili_connections WHERE uid LIKE $1",
        "DELETE FROM pipelines WHERE uid LIKE $1",
    ] {
        sqlx::query(sql)
            .bind(format!("{prefix}-%"))
            .execute(&pool)
            .await
            .expect("clean");
    }
    Some(pool)
}

fn new_connection(uid: &str, project_id: Option<&str>) -> NewConnection {
    NewConnection {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: format!("connection {uid}"),
        project_id: project_id.map(str::to_owned),
        host: "https://movies.example".into(),
        api_key: vec![7, 7, 7],
    }
}

fn pipeline_using(uid: &str, project: Option<&str>, connection: &str) -> PipelineDefinition {
    PipelineDefinition {
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        version: 1,
        trigger: None,
        steps: vec![
            StepDefinition::new("parse", "json_parser"),
            StepDefinition::new("index", "meili_indexer")
                .depends_on(["parse"])
                .config(serde_json::json!({ "connection": connection })),
        ],
        builtin: false,
        project_id: project.map(str::to_owned),
    }
}

#[tokio::test]
async fn insert_then_get_roundtrips_the_sealed_key() {
    let Some(pool) = pool("cn-a").await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    let repo = ConnectionRepo::new(pool);
    let stored = repo
        .insert(&new_connection("cn-a-1", Some("cp-1")))
        .await
        .expect("insert");
    assert_eq!(stored.uid, "cn-a-1");
    assert_eq!(stored.host, "https://movies.example");

    let got = repo
        .get("cn-a-1", Some("cp-1"))
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        got.api_key,
        vec![7, 7, 7],
        "sealed bytes round-trip untouched"
    );
}

#[tokio::test]
async fn a_tenant_sees_its_own_connection_before_the_global_one() {
    let Some(pool) = pool("cn-shadow").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool);
    let mut global = new_connection("cn-shadow-1", None);
    global.host = "https://global.example".into();
    repo.insert(&global).await.expect("global");
    let mut tenant = new_connection("cn-shadow-1", Some("cp-2"));
    tenant.host = "https://tenant.example".into();
    repo.insert(&tenant).await.expect("tenant");

    let for_tenant = repo
        .get("cn-shadow-1", Some("cp-2"))
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(for_tenant.host, "https://tenant.example");

    let for_other = repo
        .get("cn-shadow-1", Some("cp-3"))
        .await
        .expect("get")
        .expect("falls back to global");
    assert_eq!(for_other.host, "https://global.example");
}

#[tokio::test]
async fn list_scopes_to_the_tenant() {
    let Some(pool) = pool("cn-list").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool);
    repo.insert(&new_connection("cn-list-global", None))
        .await
        .expect("insert");
    repo.insert(&new_connection("cn-list-mine", Some("cp-4")))
        .await
        .expect("insert");
    repo.insert(&new_connection("cn-list-theirs", Some("cp-5")))
        .await
        .expect("insert");

    let uids: Vec<String> = repo
        .list(Some("cp-4"))
        .await
        .expect("list")
        .into_iter()
        .map(|c| c.uid)
        .collect();
    assert!(uids.contains(&"cn-list-mine".to_string()));
    assert!(uids.contains(&"cn-list-global".to_string()));
    assert!(
        !uids.contains(&"cn-list-theirs".to_string()),
        "another tenant's connection must not leak"
    );
}

#[tokio::test]
async fn update_keeps_the_key_unless_a_new_one_is_given() {
    let Some(pool) = pool("cn-upd").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool);
    repo.insert(&new_connection("cn-upd-1", Some("cp-6")))
        .await
        .expect("insert");

    let renamed = repo
        .update(
            "cn-upd-1",
            Some("cp-6"),
            &ConnectionPatch {
                name: Some("Movies prod".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update")
        .expect("exists");
    assert_eq!(renamed.name, "Movies prod");
    assert_eq!(renamed.api_key, vec![7, 7, 7], "omitting the key keeps it");

    let rekeyed = repo
        .update(
            "cn-upd-1",
            Some("cp-6"),
            &ConnectionPatch {
                api_key: Some(vec![9, 9]),
                host: Some("https://new.example".into()),
                ..Default::default()
            },
        )
        .await
        .expect("update")
        .expect("exists");
    assert_eq!(rekeyed.api_key, vec![9, 9]);
    assert_eq!(rekeyed.host, "https://new.example");
    assert_eq!(rekeyed.name, "Movies prod", "untouched fields survive");
}

#[tokio::test]
async fn a_tenant_cannot_modify_or_delete_a_global_connection() {
    let Some(pool) = pool("cn-scope").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool);
    repo.insert(&new_connection("cn-scope-1", None))
        .await
        .expect("global insert");

    let patch = ConnectionPatch {
        host: Some("https://attacker.example".into()),
        ..Default::default()
    };
    assert!(
        repo.update("cn-scope-1", Some("cp-7"), &patch)
            .await
            .expect("update")
            .is_none(),
        "a tenant with no row of its own must not reach the global one"
    );
    assert!(
        !repo
            .delete("cn-scope-1", Some("cp-7"))
            .await
            .expect("delete")
    );

    let global = repo
        .get("cn-scope-1", None)
        .await
        .expect("get")
        .expect("still exists");
    assert_eq!(global.host, "https://movies.example", "not repointed");
}

#[tokio::test]
async fn delete_is_never_blocked_by_pipelines_using_it() {
    let Some(pool) = pool("cn-del").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool.clone());
    repo.insert(&new_connection("cn-del-1", Some("cp-8")))
        .await
        .expect("insert");
    PipelineRepo::new(pool)
        .upsert(&pipeline_using("cn-del-p", Some("cp-8"), "cn-del-1"))
        .await
        .expect("pipeline");

    assert!(
        repo.delete("cn-del-1", Some("cp-8")).await.expect("delete"),
        "spec Decision 15: deleting a referenced connection is allowed"
    );
    assert!(
        repo.get("cn-del-1", Some("cp-8"))
            .await
            .expect("get")
            .is_none()
    );
    assert!(
        !repo.delete("cn-del-1", Some("cp-8")).await.expect("delete"),
        "deleting twice reports false"
    );
}

#[tokio::test]
async fn used_by_lists_the_pipelines_whose_indexer_names_it() {
    let Some(pool) = pool("cn-used").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool.clone());
    let pipelines = PipelineRepo::new(pool);
    repo.insert(&new_connection("cn-used-1", Some("cp-9")))
        .await
        .expect("insert");

    pipelines
        .upsert(&pipeline_using("cn-used-a", Some("cp-9"), "cn-used-1"))
        .await
        .expect("pipeline a");
    pipelines
        .upsert(&pipeline_using("cn-used-b", Some("cp-9"), "cn-used-1"))
        .await
        .expect("pipeline b");
    pipelines
        .upsert(&pipeline_using("cn-used-other", Some("cp-9"), "cn-used-2"))
        .await
        .expect("pipeline on another connection");
    pipelines
        .upsert(&pipeline_using(
            "cn-used-elsewhere",
            Some("cp-10"),
            "cn-used-1",
        ))
        .await
        .expect("same connection name, another tenant");

    let used = repo
        .used_by("cn-used-1", Some("cp-9"))
        .await
        .expect("used_by");
    assert_eq!(used, vec!["cn-used-a".to_string(), "cn-used-b".to_string()]);
}

#[tokio::test]
async fn used_by_ignores_a_connection_key_on_a_non_indexer_step() {
    let Some(pool) = pool("cn-noidx").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool.clone());
    let mut def = pipeline_using("cn-noidx-p", Some("cp-11"), "cn-noidx-1");
    // Move the `connection` key onto the parser; only meili_indexer steps count.
    def.steps[1].config = serde_json::json!({});
    def.steps[0].config = serde_json::json!({ "connection": "cn-noidx-1" });
    PipelineRepo::new(pool)
        .upsert(&def)
        .await
        .expect("pipeline");

    assert!(
        repo.used_by("cn-noidx-1", Some("cp-11"))
            .await
            .expect("used_by")
            .is_empty()
    );
}

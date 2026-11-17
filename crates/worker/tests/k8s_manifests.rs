//! Checks on the reference Kubernetes manifests in `k8s/`.

use serde::Deserialize;
use serde_yaml::Value;

const MANIFESTS: &[(&str, &str)] = &[
    (
        "control-plane.yaml",
        include_str!("../../../k8s/control-plane.yaml"),
    ),
    ("gateway.yaml", include_str!("../../../k8s/gateway.yaml")),
    ("workers.yaml", include_str!("../../../k8s/workers.yaml")),
    ("temporal.yaml", include_str!("../../../k8s/temporal.yaml")),
];

fn documents() -> Vec<(&'static str, Value)> {
    let mut docs = Vec::new();
    for (file, text) in MANIFESTS {
        for doc in serde_yaml::Deserializer::from_str(text) {
            let value = Value::deserialize(doc).unwrap_or_else(|e| panic!("k8s/{file}: {e}"));
            if !value.is_null() {
                docs.push((*file, value));
            }
        }
    }
    docs
}

fn str_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(value, |v, key| v.get(*key))
        .and_then(Value::as_str)
}

/// A Deployment driven by an HPA or a KEDA ScaledObject must not set
/// `spec.replicas`: every `kubectl apply -k k8s/` would reset the count the
/// autoscaler chose back to the manifest value.
#[test]
fn autoscaled_deployments_leave_replicas_to_the_autoscaler() {
    let docs = documents();
    let targets: Vec<&str> = docs
        .iter()
        .filter(|(_, d)| {
            matches!(
                str_at(d, &["kind"]),
                Some("HorizontalPodAutoscaler" | "ScaledObject")
            )
        })
        .filter_map(|(_, d)| str_at(d, &["spec", "scaleTargetRef", "name"]))
        .collect();
    assert!(
        targets.contains(&"meili-gateway") && targets.contains(&"workers-general"),
        "expected the gateway HPA and the workers ScaledObject, found {targets:?}"
    );

    for (file, doc) in &docs {
        if str_at(doc, &["kind"]) != Some("Deployment") {
            continue;
        }
        let name = str_at(doc, &["metadata", "name"]).unwrap_or_default();
        if targets.contains(&name) {
            assert!(
                doc.get("spec").and_then(|s| s.get("replicas")).is_none(),
                "k8s/{file}: Deployment {name} is autoscaled but sets spec.replicas"
            );
        }
    }
}

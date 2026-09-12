//! meili-ingest worker library: the `PipelineWorkflow`, the `execute_step` activity,
//! the plugin registry and pure DAG-scheduling helpers.
//!
//! The binary in `main.rs` wires these to a Temporal worker polling one task queue.

pub mod activity;
pub mod config;
pub mod dag;
pub mod registry;
pub mod workflow;

pub use activity::StepActivities;
pub use config::WorkerConfig;
pub use registry::PluginRegistry;
pub use workflow::PipelineWorkflow;

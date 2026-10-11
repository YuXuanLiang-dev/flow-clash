pub mod catalog;
mod engine;
mod probes;

pub use engine::RoutingStatus;
pub use engine::{reevaluate, restart, start, status, stop, verified};
pub const SERVICES: [&str; 3] = ["OPENAI", "CLAUDE", "GEMINI"];

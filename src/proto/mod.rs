#[allow(clippy::all)]
#[allow(warnings)]
pub mod kubemq {
    // tonic-build 0.12 with proto package "kubemq" generates "kubemq.rs" by default.
    // The file is at src/proto/kubemq.rs (committed, not generated at build time).
    include!("kubemq.rs");
}

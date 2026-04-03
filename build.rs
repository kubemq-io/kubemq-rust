// build.rs -- used only during development for proto regeneration.
// Consumers use the committed generated code in src/proto/kubemq.rs.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Uncomment to regenerate proto:
    // tonic_build::configure()
    //     .out_dir("src/proto")
    //     .build_server(false)
    //     .build_client(true)
    //     .compile_protos(&["proto/kubemq.proto"], &["proto/"])?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // prost-build emits no rerun-if-changed for the protos it compiles, and
    // Cargo's default reruns this script only when a file inside this package
    // changes. The service proto lives in the SDK package, so list it here or
    // the mock server keeps the old schema until a clean build.
    println!("cargo:rerun-if-changed=../sdk/zerobus_service.proto");
    println!("cargo:rerun-if-changed=build.rs");
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../sdk/zerobus_service.proto"], &["../sdk"])?;

    Ok(())
}

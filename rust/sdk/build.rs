use std::env;

#[cfg(feature = "zeroparser")]
#[path = "src/zeroparser/proto_build.rs"]
mod zeroparser_proto_build;

fn main() {
    // prost-build emits no rerun-if-changed for the protos it compiles. Cargo
    // reruns a build script on any package change only while the script emits
    // no rerun-if-changed at all, and the zeroparser build emits some, so list
    // the service proto here or a proto change goes unnoticed.
    println!("cargo:rerun-if-changed=zerobus_service.proto");
    println!("cargo:rerun-if-changed=build.rs");
    env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["zerobus_service.proto"], &["."])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    #[cfg(feature = "zeroparser")]
    zeroparser_proto_build::compile();
}

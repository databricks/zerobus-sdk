fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("output")
        .file_descriptor_set_path("output/orders.descriptor")
        .compile_protos(&["output/orders.proto"], &["output"])?;
    Ok(())
}

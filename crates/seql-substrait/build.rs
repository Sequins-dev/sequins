fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/seql_extension.proto"], &["proto/"])?;
    println!("cargo:rerun-if-changed=proto/seql_extension.proto");
    Ok(())
}

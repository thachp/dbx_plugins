fn main() {
    println!("cargo:rerun-if-changed=schemas/plugin.capnp");
    println!("cargo:rerun-if-changed=schemas/control.capnp");
    println!("cargo:rerun-if-changed=protos/event_record.proto");
    println!("cargo:rerun-if-changed=protos");

    capnpc::CompilerCommand::new()
        .src_prefix("schemas")
        .file("schemas/plugin.capnp")
        .file("schemas/api.capnp")
        .file("schemas/control.capnp")
        .run()
        .expect("failed to compile Cap'n Proto schema");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to locate protoc binary");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    let protos = ["protos/event_record.proto"];
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&protos, &["protos"])
        .expect("failed to compile gRPC protos");
}

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to locate protoc binary");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    println!("cargo:rerun-if-changed=proto/api.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/api.proto"], &["proto"])
        .expect("failed to compile gRPC protos");
}

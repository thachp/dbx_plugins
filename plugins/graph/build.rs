fn main() {
    println!("cargo:rerun-if-changed=src/net/proto.capnp");
    if let Err(error) = capnpc::CompilerCommand::new()
        .file("src/net/proto.capnp")
        .run()
    {
        panic!("failed to compile Cap'n Proto schema: {error}");
    }
}

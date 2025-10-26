@0xdba3e73db4e5065f;

struct Edge {
  id         @0 :Text;      # stable edge identifier (snowflake or composite)
  from       @1 :Text;      # source node id
  to         @2 :Text;      # target node id
  relation   @3 :Text;      # relationship type label
  tags       @4 :List(Text);
  weight     @5 :Float64;   # optional strength; use NaN if unspecified
  confidence @6 :Float64;   # optional confidence; use NaN if unspecified
  propsJson  @7 :Text;      # serialized JSON map for extra metadata
  op         @8 :UInt16;    # 0 = UPSERT, 1 = DELETE
}

struct EdgeQuery {
  node        @0 :Text;
  direction   @1 :UInt16;   # 0 = OUT, 1 = IN, 2 = BOTH
  relation    @2 :Text;     # empty string means no filter
  maxDepth    @3 :UInt16;   # BFS depth (1=direct neighbors)
  limit       @4 :UInt32;   # 0 = no limit
}

struct GraphRequest {
  union {
    upsertEdges @0 :List(Edge);
    deleteEdges @1 :List(Text);
    queryEdges  @2 :EdgeQuery;
  }
}

struct GraphResponse {
  union {
    ack   @0 :Void;
    edges @1 :List(Edge);
    error @2 :Text;
  }
}

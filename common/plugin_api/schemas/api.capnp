@0xb9fc833508a403bf;

struct AppendEventRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  eventType @2 :Text;
  payloadJson @3 :Text;
  note @4 :Text;
  patchJson @5 :Text;
  metadataJson @6 :Text;
  hasMetadata @7 :Bool;
}

struct AppendEventResponse {
  event @0 :Event;
}

struct ListAggregatesRequest {
  skip @0 :UInt32;
  take @1 :UInt32;
}

struct ListAggregatesResponse {
  aggregates @0 :List(Aggregate);
}

struct GetAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
}

struct GetAggregateResponse {
  aggregate @0 :Aggregate;
}

struct ListEventsRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  skip @2 :UInt32;
  take @3 :UInt32;
}

struct ListEventsResponse {
  events @0 :List(Event);
}

struct VerifyAggregateRequest {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
}

struct VerifyAggregateResponse {
  merkleRoot @0 :Text;
}

struct Aggregate {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  version @2 :UInt64;
  state @3 :List(StateEntry);
  merkleRoot @4 :Text;
  archived @5 :Bool;
}

struct StateEntry {
  key @0 :Text;
  value @1 :Text;
}

struct Event {
  aggregateType @0 :Text;
  aggregateId @1 :Text;
  eventType @2 :Text;
  version @3 :UInt64;
  payloadJson @4 :Text;
  metadata @5 :EventMetadata;
  hash @6 :Text;
  merkleRoot @7 :Text;
  extensionsJson @8 :Text;
}

struct EventMetadata {
  eventId @0 :Text;
  createdAt @1 :Text;
  issuedBy @2 :ActorClaims;
  note @3 :Text;
}

struct ActorClaims {
  group @0 :Text;
  user @1 :Text;
}

interface EventService {
  health @0 () -> (status :Text);
  appendEvent @1 (request :AppendEventRequest) -> (response :AppendEventResponse);
  listAggregates @2 (request :ListAggregatesRequest) -> (response :ListAggregatesResponse);
  getAggregate @3 (request :GetAggregateRequest) -> (response :GetAggregateResponse);
  listEvents @4 (request :ListEventsRequest) -> (response :ListEventsResponse);
  verifyAggregate @5 (request :VerifyAggregateRequest) -> (response :VerifyAggregateResponse);
}

const protobuf = require('protocol-buffers')

module.exports = protobuf(`
message Node {
  required uint32 change   = 1;
  required string key      = 2;
  required string log      = 3;
  optional uint32 seq      = 4;
  optional bytes identity  = 7;
  optional bytes signature = 8;
  required bytes value     = 5;
  repeated string links    = 6;
}

message Entry {
  required uint32 change = 1;
  required string node   = 2;
  repeated string links  = 3;
  optional string log = 4;
  optional uint32 seq = 5;
}

message Log {
  required string log = 1;
  required uint32 seq = 2;
}

message Handshake {
  required uint32 version = 1;
  optional string mode    = 2 [default = "sync"];
  optional bytes metadata = 3;
  optional bool live      = 4;
}
`)

# NETIX Protocol Core

Shared protocol-adapter libraries for the NETIX industrial-protocol tools. This
is a Cargo workspace of reusable crates; the runnable binaries live in separate
repositories and depend on these crates as git dependencies:

- [`netix-simulator`](https://github.com/NETIX-AI-OSS/netix-simulator) — the
  config-driven device simulator.
- [`netix-republisher`](https://github.com/NETIX-AI-OSS/netix-republisher) — the
  discover/browse/poll → MQTT republisher.

## Crates

```
crates/
  proto-api          neutral types: PointKind, PointValue, Addressing, Capabilities, FieldSpec
  sim-core           protocol-agnostic simulator: engine, TUI, app lifecycle; SimProtocol trait + registry
  republish-core     protocol-agnostic republisher: MQTT/TLS, worker, iced GUI; RepublishProtocol trait + registry
  proto-bacnet       BACnet/IP adapter   (features: sim, republish)   [reference implementation]
  proto-modbus       Modbus TCP adapter  (features: sim, republish)
  proto-opcua        OPC UA adapter      (features: sim, republish)   [uses async-opcua, MPL-2.0]
```

Protocols are **compile-time trait adapters**. Each `proto-<x>` crate implements
the simulator-side trait (`sim-core::SimProtocol`, behind feature `sim`) and/or
the republisher-side trait (`republish-core::RepublishProtocol`, behind feature
`republish`). A binary selects adapters by enabling features and calling each
crate's `register_sim` / `register_republish`.

## Consuming these crates

The binary repos depend on the relevant crates by git, pinning the commit in
their own `Cargo.lock`:

```toml
[dependencies]
sim-core     = { git = "https://github.com/NETIX-AI-OSS/netix-protocol-core", branch = "main" }
proto-opcua  = { git = "https://github.com/NETIX-AI-OSS/netix-protocol-core", branch = "main", features = ["sim"] }
```

The republisher additionally re-applies the patched `bacnet-transport` (see
`NOTICE`); it vendors its own copy and patches `crates-io` to it.

## Tests

```bash
cargo test -p proto-api -p sim-core -p republish-core
cargo test -p proto-bacnet --features sim
cargo test -p proto-modbus --features sim,republish   # Modbus republisher↔simulator loopback
cargo test -p proto-opcua  --features sim,republish   # OPC UA republisher↔simulator loopback
```

## Licensing

Apache-2.0 (see `LICENSE`). The OPC UA adapter depends on `async-opcua`
(MPL-2.0) and the BACnet adapter vendors a patched `bacnet-transport` (MIT);
see `NOTICE`.

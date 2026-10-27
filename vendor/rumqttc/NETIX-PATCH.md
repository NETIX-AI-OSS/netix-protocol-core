# rumqttc dependency patch

Source: crates.io `rumqttc` 0.25.1, upstream commit
`f1e9e8d558783f942993046679cdf3c8c3a3d36b` in
<https://github.com/bytebeamio/rumqtt/tree/f1e9e8d558783f942993046679cdf3c8c3a3d36b/rumqttc>.
The Apache-2.0 license is included as `LICENSE`.

The only functional change is the `rustls-webpki` dependency requirement:
`0.102.8` becomes `0.103.16` in both Cargo manifests. The MQTT implementation
and TLS verification behavior are otherwise upstream. The old webpki version
is exposed by rumqttc's public TLS error variant; actual certificate verification
is performed by rustls through tokio-rustls. Both now resolve to the patched
webpki line instead of retaining the vulnerable 0.102 dependency.

This fixes RUSTSEC-2026-0382 without disabling TLS or ignoring the advisory.
Remove this vendor patch when a published compatible rumqttc release updates
the requirement. Registry packaging metadata and the upstream standalone lockfile
are omitted; the workspace Cargo.lock controls resolution. Upstream examples,
integration-test targets, their fixtures and development-only dependencies are
also omitted from the normalized Cargo.toml. They are not part of the library
used by republish-core. Cargo.toml.orig preserves the upstream manifest with
the verifier requirement updated.

An empty `[workspace]` keeps the upstream dependency isolated from the
first-party workspace's test and lint targets. The root workspace also excludes
this directory. First-party lint, test and coverage gates are unchanged.

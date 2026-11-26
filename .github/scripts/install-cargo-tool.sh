#!/usr/bin/env bash
set -euo pipefail

tool=$1
arch=$(uname -m)
case "$tool:$arch" in
  cargo-deny:x86_64) checksum=f1f8eedc2a3ac297c540873f93785d4104b102c0079506b2a6b3221b7ec956af ;;
  cargo-deny:aarch64) checksum=32580dcc2bc13fbeeb7c50edc38ef99c3b9ce9569a1d07a96e11b3b941f3e72e ;;
  cargo-llvm-cov:x86_64) checksum=8d863ab2df1fb53d4f41052b3fa4627ac003d00c46a6388ce037d17472c504ef ;;
  cargo-llvm-cov:aarch64) checksum=dab1bfd1f25f7fcd3d6fc6abf9d71b40217a7a103e1da88c1a23111960aa4a40 ;;
  *) echo "No pinned binary for $tool on $arch" >&2; exit 1 ;;
esac

case "$tool" in
  cargo-deny)
    release=0.19.9
    directory="cargo-deny-$release-$arch-unknown-linux-musl"
    url="https://github.com/EmbarkStudios/cargo-deny/releases/download/$release/$directory.tar.gz"
    entry="$directory/cargo-deny"
    ;;
  cargo-llvm-cov)
    release=v0.9.2
    url="https://github.com/taiki-e/cargo-llvm-cov/releases/download/$release/cargo-llvm-cov-$arch-unknown-linux-musl.tar.gz"
    entry=cargo-llvm-cov
    ;;
esac

archive=$(mktemp "$RUNNER_TEMP/$tool.XXXXXX")
trap 'rm -f "$archive"' EXIT
destination="$RUNNER_TEMP/netix-cargo-tools"
mkdir -p "$destination"
curl --fail --silent --show-error --location --retry 2 "$url" --output "$archive"
printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check --strict
tar -xOzf "$archive" "$entry" > "$destination/$tool"
chmod 755 "$destination/$tool"
"$destination/$tool" --version
printf '%s\n' "$destination" >> "$GITHUB_PATH"

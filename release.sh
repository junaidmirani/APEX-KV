#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# release.sh  —  Build static release binaries for Linux x86_64 and ARM64
#
#   ./release.sh [version]
#
#   Outputs:
#     dist/apex-kv-v{version}-linux-x86_64
#     dist/apex-kv-v{version}-linux-arm64
#     dist/apex-kv-v{version}.tar.gz
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

VERSION="${1:-$(git describe --tags --always 2>/dev/null || echo 'dev')}"
DIST="dist"
IMAGE_X64="apex-kv-builder-x64"
IMAGE_ARM="apex-kv-builder-arm64"

GRN="\033[32m" RST="\033[0m"
info() { echo -e "${GRN}▶${RST} $*"; }

mkdir -p "${DIST}"

info "Building APEX-KV ${VERSION}"

# ── x86_64 static (musl/Alpine) ───────────────────────────────────────────────
info "Building linux/amd64..."
docker build \
    --platform linux/amd64 \
    --target builder \
    -t "${IMAGE_X64}" \
    --build-arg CMAKE_CXX_FLAGS="-O3 -march=x86-64" \
    . -q

docker run --rm "${IMAGE_X64}" cat /src/build/kv \
    > "${DIST}/apex-kv-v${VERSION}-linux-x86_64"
chmod +x "${DIST}/apex-kv-v${VERSION}-linux-x86_64"
info "  $(du -sh "${DIST}/apex-kv-v${VERSION}-linux-x86_64" | cut -f1)  linux/x86_64"

# ── arm64 static ─────────────────────────────────────────────────────────────
info "Building linux/arm64..."
docker build \
    --platform linux/arm64 \
    --target builder \
    -t "${IMAGE_ARM}" \
    --build-arg CMAKE_CXX_FLAGS="-O3 -march=armv8-a" \
    . -q

docker run --rm "${IMAGE_ARM}" cat /src/build/kv \
    > "${DIST}/apex-kv-v${VERSION}-linux-arm64"
chmod +x "${DIST}/apex-kv-v${VERSION}-linux-arm64"
info "  $(du -sh "${DIST}/apex-kv-v${VERSION}-linux-arm64" | cut -f1)  linux/arm64"

# ── Docker image ──────────────────────────────────────────────────────────────
info "Building Docker image..."
docker build \
    --target runtime \
    -t "apex-kv:${VERSION}" \
    -t "apex-kv:latest" \
    .
info "  Docker image: apex-kv:${VERSION}"

# ── Archive ───────────────────────────────────────────────────────────────────
info "Creating archive..."
tar -czf "${DIST}/apex-kv-v${VERSION}.tar.gz" \
    -C "${DIST}" \
    "apex-kv-v${VERSION}-linux-x86_64" \
    "apex-kv-v${VERSION}-linux-arm64"

# ── SHA256 checksums ──────────────────────────────────────────────────────────
(cd "${DIST}" && sha256sum apex-kv-v"${VERSION}"* > apex-kv-v"${VERSION}".sha256)
info "Checksums:"
cat "${DIST}/apex-kv-v${VERSION}.sha256"

echo ""
echo "Release artifacts:"
ls -lh "${DIST}"/apex-kv-v"${VERSION}"*
echo ""
info "Done!  Version ${VERSION}"

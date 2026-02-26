# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Builder — static binary via musl/Alpine
# ─────────────────────────────────────────────────────────────────────────────
FROM alpine:3.19 AS builder

RUN apk add --no-cache \
    build-base cmake git \
    linux-headers musl-dev \
    g++ make

WORKDIR /src
COPY . .

RUN cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DAPEX_STATIC=ON \
    -DCMAKE_CXX_FLAGS="-O3 -march=x86-64" \
    && cmake --build build -j$(nproc)

# Verify the binary is static
RUN file build/kv && ldd build/kv 2>&1 | grep -q "statically linked"

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: Minimal runtime image (~5 MB)
# ─────────────────────────────────────────────────────────────────────────────
FROM scratch AS runtime

COPY --from=builder /src/build/kv /kv

# WAL directory
VOLUME /data

# KV port (TCP) + Gossip port (UDP, KV port + 1000)
EXPOSE 7001 8001/udp

ENV APEX_ID=1
ENV APEX_PORT=7001
ENV APEX_WAL_DIR=/data
ENV APEX_PEERS=""

ENTRYPOINT ["/kv"]
CMD ["--id", "1", "--port", "7001", "--wal-dir", "/data"]

# ─────────────────────────────────────────────────────────────────────────────
# Convenience: full image with shell for debugging
# ─────────────────────────────────────────────────────────────────────────────
FROM alpine:3.19 AS debug

COPY --from=builder /src/build/kv /usr/local/bin/kv
RUN apk add --no-cache bash curl

VOLUME /data
EXPOSE 7001 8001/udp
ENTRYPOINT ["/usr/local/bin/kv"]
CMD ["--id", "1", "--port", "7001", "--wal-dir", "/data"]

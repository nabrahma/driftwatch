# Multi-stage, distroless static, nonroot (§18).
#
# The final image has no shell, no package manager and no libc. That is not
# minimalism for its own sake: driftwatch holds a connection to a datastore and
# credentials for it, so an attacker who reaches code execution inside this
# container should find nothing to pivot with. There is no /bin/sh to exec, no
# apt to install one, and the root filesystem is mounted read-only by the
# manifests in config/manager.

# --- build ------------------------------------------------------------------
#
# Pinned to the minimum Go the module declares rather than the newest release.
# §8.5 holds the toolchain at 1.23 deliberately, and building with a newer one
# here would let a 1.24-only construct into the tree that CI would then reject.
FROM golang:1.23-bookworm AS build

WORKDIR /src

# Dependencies first, as their own layer: they change far less often than the
# source, so an edit to a controller does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# CGO off, so the binary has no dynamic linkage at all — which is what makes
# `distroless/static` a legal base rather than one that fails at exec time with
# a missing loader.
#
# -trimpath keeps build paths out of the binary, and -s -w drops the symbol and
# DWARF tables: smaller image, and nothing for a reader of the image layer to
# learn about the build host.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Version=${VERSION} \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Commit=${COMMIT} \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Date=${DATE}" \
      -o /out/driftwatch-manager ./cmd/driftwatch-manager \
 && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Version=${VERSION} \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Commit=${COMMIT} \
        -X github.com/nabrahma/driftwatch/internal/buildinfo.Date=${DATE}" \
      -o /out/driftwatch ./cmd/driftwatch

# --- runtime ----------------------------------------------------------------
#
# static-debian12:nonroot carries CA certificates, /etc/passwd and tzdata, and
# nothing else. The nonroot tag pins UID and GID 65532, which is what the
# runAsUser in config/manager/manager.yaml has to match — a mismatch there and
# the container cannot read its own webhook certificate.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=build /out/driftwatch-manager /driftwatch-manager
# The CLI ships in the same image so that `kubectl exec` is not the only way to
# run `driftwatch explain` against a check — and because a debug container built
# from this image then has the tool already in it.
COPY --from=build /out/driftwatch /driftwatch

# Numeric rather than the `nonroot` name: a Pod that sets runAsNonRoot requires
# the kubelet to be able to tell the UID is not zero before it starts the
# container, and it cannot resolve a name to do that.
USER 65532:65532

EXPOSE 8080 8081 9443

ENTRYPOINT ["/driftwatch-manager"]

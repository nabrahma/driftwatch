# The e2e and demo harness: the publisher and the materializer.
#
# A separate image from the release one, deliberately. §18 requires the shipped
# image to hold nothing but the two production binaries, and a test harness
# living inside it would be a permanent invitation to point fault injection at
# production. Only the e2e suite and `make demo` ever load this.
#
# It is otherwise built to the same rules: distroless static, nonroot, UID
# 65532, no shell. That is not ceremony — the health check in the compose file
# has to be the binary itself precisely because there is no curl to call, and
# discovering that here rather than in production is part of the point.
FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY test/ test/

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" \
      -o /out/e2eharness ./test/e2e/cmd/e2eharness

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/e2eharness /e2eharness

USER 65532:65532

EXPOSE 5557 8090

ENTRYPOINT ["/e2eharness"]

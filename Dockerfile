# Build on the native platform and cross-compile for the target, so
# multi-arch builds don't pay for QEMU emulation of the Go compiler.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/geoip-processor ./cmd/geoip-processor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/geoip-processor /geoip-processor
ENTRYPOINT ["/geoip-processor"]

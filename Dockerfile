FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/geoip-processor ./cmd/geoip-processor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/geoip-processor /geoip-processor
ENTRYPOINT ["/geoip-processor"]

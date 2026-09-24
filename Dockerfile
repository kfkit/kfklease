# Cross-compiles on the build platform: a multi-arch build does not need
# emulation for the Go toolchain, only for the (empty) distroless base.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH
# PKG is the package to build; the default is the scaler, the stand also
# builds examples/heartbeat.
ARG PKG=./cmd/kfklease-scaler
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /app $PKG

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /app /app
USER nonroot
EXPOSE 9090 9091
ENTRYPOINT ["/app"]

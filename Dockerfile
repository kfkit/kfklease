# Cross-compiles on the build platform: a multi-arch build does not need
# emulation for the Go toolchain, only for the (empty) distroless base.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /kfklease-scaler ./cmd/kfklease-scaler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /kfklease-scaler /kfklease-scaler
USER nonroot
EXPOSE 9090
ENTRYPOINT ["/kfklease-scaler"]

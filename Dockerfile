FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /kfklease-scaler ./cmd/kfklease-scaler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /kfklease-scaler /kfklease-scaler
USER nonroot
EXPOSE 9090
ENTRYPOINT ["/kfklease-scaler"]

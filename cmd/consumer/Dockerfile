FROM golang:1.25.1-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# build consumer binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /out/consumer ./cmd/consumer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/consumer /app/consumer

EXPOSE 2112
USER nobody
ENTRYPOINT ["/app/consumer"]

# Dockerfile for chaos testing — builds a minimal LynxDB binary.
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /lynxdb ./cmd/lynxdb/

FROM alpine:3.21
RUN apk add --no-cache iptables iproute2 bash

COPY --from=builder /lynxdb /usr/local/bin/lynxdb

ENTRYPOINT ["lynxdb"]

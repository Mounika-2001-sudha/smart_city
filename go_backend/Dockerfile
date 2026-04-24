FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /smart_city_backend ./cmd/server

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk --no-cache add ca-certificates wget

COPY --from=builder /smart_city_backend /smart_city_backend

EXPOSE 8080

ENTRYPOINT ["/smart_city_backend"]

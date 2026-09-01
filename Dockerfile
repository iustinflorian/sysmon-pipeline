FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET_APP=api
RUN CGO_ENABLED=0 GOOS=linux go build -o /sysmon-app ./cmd/${TARGET_APP}/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /sysmon-app .

EXPOSE 8080

CMD ["./sysmon-app"]
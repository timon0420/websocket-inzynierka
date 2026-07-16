FROM golang:1.26.0 AS builder

WORKDIR /build

COPY app/go.mod app/go.sum ./

RUN go mod download

COPY app/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main main.go

EXPOSE 8080

FROM alpine:latest

WORKDIR /app

COPY --from=builder /main .

EXPOSE 8080

CMD ["./main"]
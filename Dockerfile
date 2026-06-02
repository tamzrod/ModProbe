FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/modprobe .

FROM alpine:3.22

WORKDIR /app
COPY --from=build /out/modprobe /app/modprobe
COPY web /app/web

ENV BIND_ADDR=0.0.0.0:8080

EXPOSE 8080
ENTRYPOINT ["/app/modprobe"]

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /eventcore ./apps/api && mkdir -p /var/lib/eventcore && chown 10001:10001 /var/lib/eventcore
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /eventcore /eventcore
COPY --from=build --chown=10001:10001 /var/lib/eventcore /var/lib/eventcore
USER 10001:10001
ENV BROKER_DATA_PATH=/var/lib/eventcore HTTP_ADDR=:8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s CMD ["/eventcore", "healthcheck"]
ENTRYPOINT ["/eventcore"]

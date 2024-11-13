FROM golang:1.22.2 AS build-stage

WORKDIR /app

COPY . .

RUN go mod download && \
      make validate && \
      make test && \
      make build 

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s \
  CMD curl -f http://localhost:8080/healthz || exit 1

CMD ["/app/bin/server"]

FROM gcr.io/distroless/base-debian12 AS build-release-stage

WORKDIR /

COPY --from=build-stage /app/bin/server /usr/bin/server

EXPOSE 8080

LABEL org.opencontainers.image.source=https://github.com/0xc0d3d00d/candledb
LABEL org.opencontainers.image.description="CandleDB"

USER nonroot:nonroot

CMD ["/usr/bin/server"]

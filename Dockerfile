# syntax=docker/dockerfile:1
# One image, five binaries: the compose file picks the service via `command`.
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... \
 && mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /data /data
ENV DB_PATH=/data/service.db HTTP_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT []
CMD ["orders"]

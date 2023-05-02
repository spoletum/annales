FROM alpine:latest

LABEL org.opencontainers.image.source="https://github.com/spoletum/annales"
LABEL org.opencontainers.image.description="The super-simplistic event store"
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apk add --no-cache gcompat && \
    mkdir -p /app

WORKDIR /app

COPY bin/annales .

RUN chmod u+x annales

ENTRYPOINT [ "/app/annales" ]
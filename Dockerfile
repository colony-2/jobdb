# syntax=docker/dockerfile:1

FROM gcr.io/distroless/static-debian13:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7

ARG TARGETPLATFORM
COPY --chmod=0555 $TARGETPLATFORM/jobdb /jobdb

ENV JOBDB_LISTEN=0.0.0.0:8080 \
    JOBDB_MAX_INLINE_ARTIFACT_BYTES=4096

EXPOSE 8080
USER 65532:65532

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD ["/jobdb", "healthcheck"]
ENTRYPOINT ["/jobdb", "serve"]

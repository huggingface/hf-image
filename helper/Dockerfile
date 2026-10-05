# The helper image: the Linux hf-image-helper that `hf image` runs next to the Docker daemon.
# The context holds static (musl) builds named hf-image-helper-<arch>: see scripts/helper-image.sh.
FROM gcr.io/distroless/static-debian12
ARG TARGETARCH
COPY hf-image-helper-${TARGETARCH} /hf-image-helper
ENTRYPOINT ["/hf-image-helper"]

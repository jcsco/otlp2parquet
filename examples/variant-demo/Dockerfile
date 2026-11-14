# Sender for the compose `variant` profile: curl plus the sample OTLP/JSON payloads.
# Payloads are copied in (not bind-mounted) so this works when the Docker daemon
# cannot see the workspace path, e.g. from a devcontainer or Docker Desktop.
# Build context: examples/variant-demo
ARG DOCKER_HUB_MIRROR=""
FROM ${DOCKER_HUB_MIRROR}curlimages/curl:latest
COPY traces.json logs.json metrics.json /payload/

# Stage 1: extract garage binary
FROM dxflrs/garage:v2.1.0 AS garage_dist

# Stage 2: shell-capable runtime
FROM busybox:1.36.1

COPY --from=garage_dist /garage /usr/local/bin/garage

ENV GARAGE_BIN=/usr/local/bin/garage
ENV GARAGE_CONFIG_PATH=/etc/garage.toml

# Do NOT copy the script here
ENTRYPOINT ["/bin/sh", "-c", "/opt/garage/garage-layout-bootstrap.sh"]
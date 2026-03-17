# EYWA Core - Production image
#
# Prerequisites: Build the JAR first
#   cd /path/to/eywa/core
#   clj -T:build release
#
# Then build the Docker image:
#   docker build -t neyho/eywa-core:latest .
#
FROM eclipse-temurin:21-jre-alpine

# Install minimal dependencies
RUN apk add --no-cache \
    postgresql-client \
    curl \
    bash

# Create EYWA user and directories
RUN adduser -D -u 1000 -s /bin/bash eywa && \
    mkdir -p /opt/eywa /data/eywa && \
    chown -R eywa:eywa /opt/eywa /data/eywa

WORKDIR /opt/eywa

# Copy pre-built JAR (built with: clj -T:build release)
COPY target/*.jar /opt/eywa/eywa-core.jar

# Copy frontend files (oauth, graphiql, etc.)
COPY frontend /opt/web

# Copy entrypoint script
COPY docker/entrypoint.sh /opt/eywa/entrypoint.sh
RUN chmod +x /opt/eywa/entrypoint.sh && \
    chown eywa:eywa /opt/eywa/entrypoint.sh && \
    chown -R eywa:eywa /opt/web

USER eywa

# Environment variables
ENV EYWA_HOME=/data/eywa \
    EYWA_LOG_LEVEL=INFO \
    EYWA_IAM_ENFORCE_ACCESS=false \
    EYWA_SERVE=/opt/web \
    EYWA_SERVER_HOST=0.0.0.0 \
    EYWA_SERVER_PORT=8080 \
    JAVA_OPTS="-Xmx2g -Xms512m"

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/opt/eywa/entrypoint.sh"]

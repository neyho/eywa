# Multi-stage build for EYWA Core
FROM clojure:temurin-21-tools-deps-jammy AS builder

# Install Node.js for frontend build (if needed)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy dependency files first for better caching
COPY deps.edn build.clj ./

# Download dependencies
RUN clojure -P -X:build

# Copy source code
COPY src ./src
COPY resources ./resources
COPY frontend ./frontend
COPY patcho ./patcho

# Build the uberjar
RUN clojure -T:build release

# Find the built JAR file
RUN mv target/*.jar /eywa-core.jar

# Copy frontend files for serving (frontend expects to be at /eywa/ path)
RUN mkdir -p /eywa-frontend/eywa && \
    cp -r frontend/* /eywa-frontend/eywa/

# Production stage - Alpine for minimal size
FROM eclipse-temurin:21-jre-alpine

# Install minimal dependencies (postgresql-client and curl)
RUN apk add --no-cache \
    postgresql-client \
    curl \
    bash

# Create EYWA user and directories (Alpine uses adduser)
RUN adduser -D -u 1000 -s /bin/bash eywa && \
    mkdir -p /opt/eywa /data/eywa && \
    chown -R eywa:eywa /opt/eywa /data/eywa

# Set working directory
WORKDIR /opt/eywa

# Copy JAR from builder
COPY --from=builder /eywa-core.jar /opt/eywa/eywa-core.jar

# Copy frontend from builder
COPY --from=builder /eywa-frontend /opt/web

# Copy entrypoint script
COPY docker/entrypoint.sh /opt/eywa/entrypoint.sh
RUN chmod +x /opt/eywa/entrypoint.sh && \
    chown eywa:eywa /opt/eywa/entrypoint.sh && \
    chown -R eywa:eywa /opt/web

# Switch to eywa user
USER eywa

# Set environment variables
ENV EYWA_HOME=/data/eywa \
    EYWA_LOG_LEVEL=INFO \
    EYWA_IAM_ENFORCE_ACCESS=true \
    EYWA_SERVE=/opt/web \
    EYWA_SERVER_HOST=0.0.0.0 \
    EYWA_SERVER_PORT=8080 \
    DATABASE_HOST=postgres \
    DATABASE_PORT=5432 \
    DATABASE_NAME=eywa \
    DATABASE_USER=eywa \
    DATABASE_PASSWORD=eywa \
    JAVA_OPTS="-Xmx2g -Xms512m"

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Entrypoint
ENTRYPOINT ["/opt/eywa/entrypoint.sh"]

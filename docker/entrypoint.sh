#!/bin/bash
set -e

# Set database defaults from POSTGRES_* variables
export POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-eywa}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-eywa}"
# Admin database settings (for creating the eywa database)
export POSTGRES_ADMIN_DB="${POSTGRES_ADMIN_DB:-postgres}"
export POSTGRES_ADMIN_USER="${POSTGRES_ADMIN_USER:-postgres}"
export POSTGRES_ADMIN_PASSWORD="${POSTGRES_ADMIN_PASSWORD:-eywa}"

# Set demo defaults if not provided
export ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
export ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"

echo "================================================"
echo "EYWA Core - Starting (Demo Mode)"
echo "================================================"
echo "EYWA_HOME: ${EYWA_HOME}"
echo "EYWA_SERVE: ${EYWA_SERVE}"
echo "POSTGRES_HOST: ${POSTGRES_HOST}"
echo "POSTGRES_DB: ${POSTGRES_DB}"
echo "POSTGRES_USER: ${POSTGRES_USER}"
echo "EYWA_LOG_LEVEL: ${EYWA_LOG_LEVEL}"
echo "EYWA_IAM_OAUTH_PERSISTENCE: true"
echo "Admin User: ${ADMIN_USERNAME}"
echo "================================================"

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
timeout=60
counter=0
until pg_isready -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USER} 2>/dev/null; do
    counter=$((counter + 1))
    if [ $counter -gt $timeout ]; then
        echo "ERROR: PostgreSQL not ready after ${timeout} seconds"
        exit 1
    fi
    echo "PostgreSQL not ready yet... waiting (${counter}/${timeout})"
    sleep 1
done
echo "PostgreSQL is ready!"

# Set admin user credentials for init
export EYWA_USER="${ADMIN_USERNAME}"
export EYWA_PASSWORD="${ADMIN_PASSWORD}"

# Enable OAuth persistence to database (requires encryption)
export EYWA_OAUTH_PERSISTENCE=true

# Initialize EYWA if first run
if [ ! -f "${EYWA_HOME}/.initialized" ]; then
    echo "First run detected - initializing EYWA..."

    # Run EYWA initialization (creates database, schemas, and admin user)
    echo "Initializing EYWA (database, IAM, Datacraft)..."
    java ${JAVA_OPTS} \
        -cp /opt/eywa/eywa-core.jar \
        clojure.main -m neyho.eywa.core init

    if [ $? -eq 0 ]; then
        echo "✓ EYWA initialization complete!"

        # Initialize encryption (auto-generate master key)
        echo ""
        echo "Initializing encryption..."
        MASTER_KEY=$(java ${JAVA_OPTS} \
            -cp /opt/eywa/eywa-core.jar \
            clojure.main -m neyho.eywa.core encryption init 2>/dev/null | tail -1)

        if [ $? -eq 0 ] && [ -n "$MASTER_KEY" ]; then
            echo "✓ Encryption initialized!"

            # Store master key for persistence
            mkdir -p "${EYWA_HOME}/config"
            echo "$MASTER_KEY" > "${EYWA_HOME}/config/.master_key"
            chmod 600 "${EYWA_HOME}/config/.master_key"
            export EYWA_ENCRYPTION_MASTER_KEY="$MASTER_KEY"
        else
            echo "ERROR: Encryption initialization failed"
            exit 1
        fi

        # Mark as initialized
        touch "${EYWA_HOME}/.initialized"

        echo ""
        echo "=========================================="
        echo "✓ EYWA is ready!"
        echo "=========================================="
        echo "Access EYWA at: http://localhost:8080/eywa/"
        echo "Login: ${ADMIN_USERNAME} / ${ADMIN_PASSWORD}"
        echo "=========================================="
        echo ""
    else
        echo "ERROR: EYWA initialization failed"
        exit 1
    fi
else
    echo "EYWA already initialized - loading configuration..."

    # Load master key from persistent storage
    if [ -f "${EYWA_HOME}/config/.master_key" ]; then
        export EYWA_ENCRYPTION_MASTER_KEY=$(cat "${EYWA_HOME}/config/.master_key")
        echo "✓ Encryption master key loaded"
    fi
fi

# Start EYWA server
echo "Starting EYWA server..."
echo "Debug - EYWA_ENCRYPTION_MASTER_KEY is set: ${EYWA_ENCRYPTION_MASTER_KEY:+YES}"
echo "Debug - EYWA_OAUTH_PERSISTENCE: ${EYWA_OAUTH_PERSISTENCE}"

# Also set as Java system properties for environ library
JAVA_PROPS=""
if [ -n "$EYWA_ENCRYPTION_MASTER_KEY" ]; then
    JAVA_PROPS="${JAVA_PROPS} -Deywa.encryption.master.key=${EYWA_ENCRYPTION_MASTER_KEY}"
fi

exec java ${JAVA_OPTS} ${JAVA_PROPS} \
    -cp /opt/eywa/eywa-core.jar \
    clojure.main -m neyho.eywa.core start

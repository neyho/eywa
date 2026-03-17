# EYWA Core - Docker Setup

This directory contains Docker configurations for running EYWA Core (IAM + Datacraft) as a complete showcase environment.

## What's Included

- **PostgreSQL 16**: Database for EYWA
- **EYWA Core**: Identity Access Management (OAuth2.1 + OIDC) + Data Modeling (Datacraft)
- **Frontend**: Web UI for data modeling and user management
- **Auto-initialization**: First-run setup with admin user creation

## Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 4GB RAM available for containers

### 1. Start EYWA

```bash
cd /path/to/eywa/core
docker-compose up -d
```

This will:
1. Pull PostgreSQL image
2. Build EYWA Core image
3. Start PostgreSQL and wait for it to be ready
4. Initialize EYWA (IAM + Datacraft schemas)
5. Create admin user
6. Start EYWA server

### 2. Access EYWA

Open your browser and navigate to:
- **EYWA UI**: http://localhost:8080/eywa/
- **GraphQL Playground**: http://localhost:8080/graphiql/
- **OAuth**: http://localhost:8080/oauth/

### 3. Login

Default demo credentials:
- **Username**: `admin`
- **Password**: `admin`

⚠️ **For production**: Change admin password immediately after first login!

## Configuration

### Environment Variables

Edit `docker-compose.yml` to customize:

```yaml
environment:
  # Database
  DATABASE_HOST: postgres
  DATABASE_PORT: 5432
  DATABASE_NAME: eywa
  DATABASE_USER: eywa
  DATABASE_PASSWORD: eywa

  # EYWA Settings
  EYWA_HOME: /data/eywa
  EYWA_LOG_LEVEL: INFO  # DEBUG, INFO, WARN, ERROR
  EYWA_IAM_ENFORCE_ACCESS: "true"
  EYWA_SERVE: /opt/eywa/frontend  # Frontend files location
  EYWA_SERVER_HOST: 0.0.0.0
  EYWA_SERVER_PORT: 8080

  # OAuth Configuration
  OAUTH_PERSISTENCE: db  # Store OAuth tokens in database
  EYWA_IAM_ALLOWED_REDIRECTIONS: "http://localhost:8080/eywa/callback,..."
  EYWA_IAM_ALLOWED_LOGOUTS: "http://localhost:8080/eywa"

  # Admin User (Demo Defaults)
  ADMIN_USERNAME: admin
  ADMIN_PASSWORD: admin

  # Encryption (Optional - auto-generated if not set)
  # ENCRYPTION_KEY: your-secure-key-here

  # Java Memory
  JAVA_OPTS: "-Xmx2g -Xms512m"
```

### Port Configuration

Default ports in `docker-compose.yml`:
- EYWA: `8080:8080`
- PostgreSQL: `5432:5432`

Change if you have conflicts:
```yaml
ports:
  - "9090:8080"  # Access EYWA on port 9090
```

## Usage

### Start Services

```bash
docker-compose up -d
```

### View Logs

```bash
# All services
docker-compose logs -f

# EYWA only
docker-compose logs -f eywa

# PostgreSQL only
docker-compose logs -f postgres
```

### Stop Services

```bash
docker-compose down
```

### Stop and Remove Data

⚠️ **This deletes all data!**

```bash
docker-compose down -v
```

### Restart Services

```bash
docker-compose restart
```

### Rebuild EYWA Image

If you make changes to the code:

```bash
docker-compose build eywa
docker-compose up -d eywa
```

## Data Persistence

Data is persisted in Docker volumes:
- `postgres_data`: PostgreSQL database files
- `eywa_data`: EYWA configuration and logs

### Backup Data

```bash
# Backup PostgreSQL
docker exec eywa-postgres pg_dump -U eywa eywa > backup.sql

# Backup EYWA data
docker cp eywa-core:/data/eywa ./eywa-backup
```

### Restore Data

```bash
# Restore PostgreSQL
cat backup.sql | docker exec -i eywa-postgres psql -U eywa eywa

# Restore EYWA data
docker cp ./eywa-backup eywa-core:/data/eywa
```

## Building for Different Architectures

### Build for Linux AMD64

```bash
docker build --platform linux/amd64 -t eywa-core:latest .
```

### Build for ARM64 (Apple Silicon)

```bash
docker build --platform linux/arm64 -t eywa-core:latest .
```

### Multi-platform Build

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t eywa-core:latest .
```

## Production Deployment

### Security Checklist

- [ ] Change default admin password
- [ ] Use strong database password
- [ ] Enable HTTPS/TLS
- [ ] Configure firewall rules
- [ ] Set up regular backups
- [ ] Use Docker secrets for passwords
- [ ] Review and restrict network access
- [ ] Enable audit logging

### Using Docker Secrets

Create a `docker-compose.prod.yml`:

```yaml
version: '3.8'

services:
  postgres:
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
    secrets:
      - db_password

  eywa:
    environment:
      DATABASE_PASSWORD_FILE: /run/secrets/db_password
      ADMIN_PASSWORD_FILE: /run/secrets/admin_password
    secrets:
      - db_password
      - admin_password

secrets:
  db_password:
    file: ./secrets/db_password.txt
  admin_password:
    file: ./secrets/admin_password.txt
```

### HTTPS/TLS Setup

Use a reverse proxy like nginx or Traefik:

```yaml
services:
  nginx:
    image: nginx:alpine
    ports:
      - "443:443"
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./certs:/etc/nginx/certs
    depends_on:
      - eywa
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs eywa

# Check container status
docker-compose ps

# Restart services
docker-compose restart
```

### Database Connection Failed

```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Check PostgreSQL logs
docker-compose logs postgres

# Test database connection
docker exec eywa-postgres psql -U eywa -d eywa -c "SELECT 1"
```

### EYWA Initialization Failed

```bash
# Remove initialization flag to retry
docker exec eywa-core rm /data/eywa/.initialized

# Restart EYWA
docker-compose restart eywa
```

### Out of Memory Errors

Increase Java heap size in `docker-compose.yml`:

```yaml
environment:
  JAVA_OPTS: "-Xmx4g -Xms1g"  # Increased from 2g
```

### Port Already in Use

```bash
# Find what's using port 8080
lsof -i :8080

# Change port in docker-compose.yml
ports:
  - "9090:8080"
```

## Advanced Usage

### Running with Custom Configuration

Mount a custom configuration file:

```yaml
services:
  eywa:
    volumes:
      - ./my-config.edn:/opt/eywa/config.edn
      - eywa_data:/data/eywa
```

### Connecting External PostgreSQL

```yaml
services:
  eywa:
    environment:
      DATABASE_HOST: external-postgres.example.com
      DATABASE_PORT: 5432
      # ... other settings
```

Remove the `postgres` service from docker-compose.yml.

### Running in Development Mode

```yaml
services:
  eywa:
    environment:
      EYWA_LOG_LEVEL: DEBUG
    volumes:
      - ./src:/opt/eywa/src  # Mount source code
    command: ["repl"]  # Start REPL instead of server
```

## Health Checks

### Check EYWA Health

```bash
curl http://localhost:8080/health
```

### Check PostgreSQL Health

```bash
docker exec eywa-postgres pg_isready -U eywa
```

### View Service Status

```bash
docker-compose ps
```

## Performance Tuning

### PostgreSQL Settings

Edit `docker/init-db.sql` or mount custom `postgresql.conf`:

```sql
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
```

### EYWA JVM Settings

```yaml
environment:
  JAVA_OPTS: "-Xmx4g -Xms1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

## Monitoring

### Resource Usage

```bash
# Container stats
docker stats eywa-core eywa-postgres

# Disk usage
docker system df
```

### Application Logs

```bash
# Real-time logs
docker-compose logs -f eywa

# Last 100 lines
docker-compose logs --tail=100 eywa
```

## Support

- **Documentation**: https://github.com/neyho/eywa-core
- **Issues**: https://github.com/neyho/eywa-core/issues
- **Examples**: https://github.com/neyho/eywa-examples

## License

See LICENSE file in the repository root.

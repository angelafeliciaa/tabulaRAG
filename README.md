# TabulaRAG

## Quick start (infra)

1. Build and start services:
   `./scripts/dev-up.sh`
2. Verify backend:
   `http://localhost:8000/health`
3. Stop services:
   `./scripts/dev-down.sh`
4. Stream logs:
   `./scripts/dev-logs.sh` or `./scripts/dev-logs.sh backend`

Services:
- Backend: `localhost:8000`
- Postgres: `localhost:5432`
- Qdrant: `localhost:6333`

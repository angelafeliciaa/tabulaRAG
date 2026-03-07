# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

**TabulaRAG** — a fast-ingesting tabular data RAG tool with cell citations, exposed via REST API and MCP (Streamable HTTP).

| Component | Stack |
|-----------|-------|
| **Backend** | Python 3.14, FastAPI, SQLAlchemy, PostgreSQL, Qdrant, FastEmbed |
| **Frontend** | React 19, Vite, TypeScript, React Router |
| **Infra** | Docker Compose (Postgres 16, Qdrant 1.13) |

## Project Structure

```
backend/
  app/
    main.py              # FastAPI app, ingestion endpoint, MCP mount
    db.py                # SQLAlchemy engine and session
    models/              # ORM models (Dataset, DatasetColumn, DatasetRow)
    embeddings.py        # Embedding model loader
    indexing.py          # Vector indexing pipeline
    index_worker.py      # Background index worker
    index_jobs.py        # Index job state management
    retrieval.py         # RAG retrieval logic
    qdrant_client.py     # Qdrant client wrapper
    routes_query.py      # Query endpoints
    routes_tables.py     # Table management endpoints
    mcp_server.py        # MCP server config
    typed_values.py      # Value normalization
    name_guard.py        # Dataset name validation
frontend/
  src/
    App.tsx              # Root component with routing
    api.ts               # Backend API client
    components/          # Shared components (DataTable)
    pages/               # Upload, TableView, AggregateTable, HighlightView
scripts/
  dev-up.sh              # Build and start all services
  dev-down.sh            # Stop all services
  dev-logs.sh            # Stream service logs
```

## Commands

### Infrastructure

```bash
./scripts/dev-up.sh          # Build and start all Docker services
./scripts/dev-down.sh        # Stop all services
./scripts/dev-logs.sh        # Stream all logs
./scripts/dev-logs.sh backend  # Stream backend logs only
```

### Frontend

```bash
cd frontend
npm install                  # Install dependencies
npm run dev                  # Start Vite dev server
npm run build                # Type-check and build
npm run lint                 # Run ESLint
```

### Backend

```bash
cd backend
pip install -r requirements.txt   # Install dependencies
uvicorn app.main:app --reload     # Run dev server
```

### Testing

```bash
pytest tests/                # Run tests from project root
```

## Services

| Service | URL |
|---------|-----|
| Frontend | `localhost:5174` |
| Backend API | `localhost:8000` |
| Health check | `localhost:8000/health` |
| Dependency health | `localhost:8000/health/deps` |
| OpenAPI spec | `localhost:8000/openapi.json` |
| MCP endpoint | `localhost:8000/mcp/mcp` |
| PostgreSQL | `localhost:5432` |
| Qdrant | `localhost:6333` |

## Key Rules

- **No `Co-Authored-By` trailers** in git commits
- **Use `npm`** in the frontend — no yarn, no pnpm
- **Use `pip`** in the backend — no poetry, no conda
- **Never commit `.env`** — use `.env.example` as reference
- Always read the file before editing it
- Follow conventions in `.claude/rules/git.md` for branch names, commits, and PR titles

## Conventions

- Backend uses **snake_case** everywhere (files, functions, variables)
- Frontend uses **camelCase** for variables/functions, **PascalCase** for components
- API routes return JSON; ingestion endpoint is `POST /ingest`
- Vector indexing runs asynchronously via background workers after ingestion
- PostgreSQL ingestion uses `COPY` for performance, with batched fallback

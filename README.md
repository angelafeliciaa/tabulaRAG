<div align="center">
  <img src="frontend/src/images/logo.png" alt="TabulaRAG logo" width="64" height="64" />
  <h1>TabulaRAG</h1>
  A fast-ingesting tabular data MCP RAG tool backed with cell citations.
</div>

## Quick start (infra)

1. Build and start services:
   `./scripts/dev-up.sh`
2. Verify backend:
   `http://localhost:8000/health`
   `curl http://localhost:8000/health/deps`
3. Stop services:
   `./scripts/dev-down.sh`
4. Stream logs:
   `./scripts/dev-logs.sh` or `./scripts/dev-logs.sh backend`

Services:
- Frontend: `localhost:5173`
- Backend: `localhost:8000`
- Postgres: `localhost:5432`
- Qdrant: `localhost:6333`

## Ingestion (CSV/TSV)

Upload a table for ingestion:

```bash
curl -F "file=@/path/to/data.csv" \
  -F "dataset_name=my_table" \
  -F "has_header=true" \
  -F "delimiter=," \
  http://localhost:8000/ingest
```

Notes:
- UTF-8 CSV/TSV only.
- `delimiter` supports `,` or tab. If omitted, it is auto-detected.

## Connecting via External Tools

TabulaRAG exposes two endpoints for integration with AI assistants and tool runners:

| Type | URL |
|---|---|
| OpenAPI | `http://localhost:8000/openapi.json` |
| MCP (Streamable HTTP) | `http://localhost:8000/mcp/mcp` |

> **Note:** If your client is running outside the browser (e.g. inside Docker or a desktop app), replace `localhost` with your machine's local IP address. Run `ipconfig` (Windows) or `ifconfig` (Mac/Linux) to find it.

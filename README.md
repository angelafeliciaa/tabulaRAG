<h1 align="center">
  <img src="frontend/src/images/logo.png" alt="TabulaRAG logo" width="64" height="64" /></br>
  TabulaRAG
</h1>

<p align="center">
  <strong>A fast-ingesting tabular data MCP RAG tool backed with cell citations.</strong><br/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/node-%3E%3D18-brightgreen" alt="Node >= 18" />
  <img src="https://img.shields.io/badge/typescript-strict-blue" alt="TypeScript Strict" />
  <img src="https://img.shields.io/badge/license-MIT-lightgrey" alt="License" />
</p> <br> <br>

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
- Postgres: `localhost:5433`
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
| MCP (Streamable HTTP) | `http://localhost:8000/mcp` |

> **Note:** If your client is running outside the browser (e.g. inside Docker or a desktop app), replace `localhost` with your machine's local IP address. Run `ipconfig` (Windows) or `ifconfig` (Mac/Linux) to find it.

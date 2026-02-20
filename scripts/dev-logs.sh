#!/usr/bin/env bash
set -euo pipefail

docker compose logs -f "${1:-}"

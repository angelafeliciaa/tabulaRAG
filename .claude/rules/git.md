# Git Conventions

## Branch Naming

Use descriptive kebab-case branches:

```
{type}/{kebab-description}
```

Examples:
```
feat/add-aggregate-tool
fix/filter-ui-bugfix
refactor/ingestion-pipeline
```

## Commit Format

Conventional Commits: `type(scope): subject`

```
feat(retrieval): add cell-level citation support
```

- **Subject line** — one line, present tense, imperative mood ("add" not "added")
- **Lowercase** after the colon — no capital letter
- **No period** at the end
- **50 chars max** for the subject (the part after `type(scope): `)
- **Body is optional** — one additional line max. Two lines total, never more.
- **No `Co-Authored-By` trailers** — no AI attribution lines in commits

### Commit Types

| Type | When to use |
|---|---|
| `feat` | New functionality, new behavior |
| `fix` | Bug fix |
| `refactor` | Code change that doesn't fix a bug or add a feature |
| `docs` | Documentation only |
| `test` | Adding or updating tests |
| `chore` | Maintenance, deps, config — no production code change |
| `style` | Formatting, whitespace, linting — no logic change |
| `ci` | CI/CD changes |
| `perf` | Performance improvement |
| `build` | Build system or external dependency changes |

### Scopes

Scope is **optional**. Omit it for changes that span multiple areas.

| Scope | Area |
|---|---|
| `ingestion` | CSV/TSV parsing, COPY pipeline, row insertion |
| `retrieval` | RAG retrieval, query logic, citations |
| `indexing` | Embedding, Qdrant upsert, index workers |
| `tables` | Table management routes, dataset CRUD |
| `filter` | Filtering and aggregation features |
| `ui` | Frontend components and pages |
| `api` | Backend route definitions, MCP endpoints |
| `db` | Database models, migrations, schema |
| `infra` | Docker, scripts, environment config |

## PR Titles

```
{Short informative title}
```

- **Sentence case**
- **Under 70 characters**
- **Describe the change**

Examples:
```
Add advanced filtering support
Fix aggregate table link UI
Support multi-file upload and parallel ingestion
```

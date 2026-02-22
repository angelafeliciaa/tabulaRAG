from mcp.server.fastmcp import FastMCP
from app.routes_tables import list_tables, get_table_slice
from app.retrieval import get_highlight, hybrid_search

mcp = FastMCP("TabulaRAG")

@mcp.tool()
def ping() -> dict:
    """Check connectivity."""
    return {"status": "ok"}

@mcp.tool()
def mcp_list_tables() -> list:
    """List all ingested tables."""
    return list_tables()

@mcp.tool()
def mcp_get_table_slice(dataset_id: int, offset: int = 0, limit: int = 30) -> dict:
    """Get a slice of rows from a table by dataset_id."""
    return get_table_slice(dataset_id, offset, limit)

@mcp.tool()
def mcp_query(
    dataset_id: int,
    question: str,
    top_k: int = 10,
) -> dict:
    """Query a dataset using hybrid search and return matching rows with highlights."""
    results = hybrid_search(
        dataset_id=dataset_id,
        question=question,
        top_k=top_k,
    )
    return {"dataset_id": dataset_id, "question": question, "results": results}

@mcp.tool()
def mcp_get_highlight(highlight_id: str) -> dict:
    """Get a specific highlighted cell by its highlight ID."""
    result = get_highlight(highlight_id)
    if not result:
        raise ValueError("Highlight not found")
    return result
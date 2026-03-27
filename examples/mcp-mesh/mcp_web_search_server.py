#!/usr/bin/env python3
"""MCP server exposing a mock web_search tool."""

import os

from fastmcp import FastMCP

mcp = FastMCP("Web Search Server")

_MAX_QUERY_LEN = 1000
_MAX_TOP_K = 100


@mcp.tool()
def web_search(query: str, top_k: int = 5) -> dict:
    """Return mock search results for a query."""
    if len(query) > _MAX_QUERY_LEN:
        return {"error": f"query exceeds max length ({_MAX_QUERY_LEN})"}
    top_k = max(1, min(top_k, _MAX_TOP_K))
    return {
        "query": query,
        "results": [
            {
                "title": f"Result {index + 1} for: {query}",
                "url": f"https://example.com/search/{index}",
                "snippet": f"Mock result {index + 1} for query {query}.",
            }
            for index in range(top_k)
        ],
    }


if __name__ == "__main__":
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    port = int(os.environ.get("MCP_PORT", "8001"))
    print(f"Starting MCP Web Search Server on {host}:{port}...", flush=True)
    mcp.run(transport="streamable-http", host=host, port=port)

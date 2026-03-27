#!/usr/bin/env python3
"""MCP server exposing a mock code_review tool."""

import os

from fastmcp import FastMCP

mcp = FastMCP("Code Review Server")

_MAX_CODE_BYTES = 100_000
_SUPPORTED_LANGUAGES = {"python", "javascript", "go", "rust", "java", "typescript", "c", "cpp"}


@mcp.tool()
def code_review(code: str, language: str = "python") -> dict:
    """Return a lightweight mock review for the submitted code."""
    if len(code) > _MAX_CODE_BYTES:
        return {"error": f"code exceeds max size ({_MAX_CODE_BYTES} bytes)"}
    if language not in _SUPPORTED_LANGUAGES:
        return {"error": f"unsupported language: {language!r}"}

    lines = code.strip().split('\n') if code.strip() else []
    issues = []

    if lines and not lines[0].startswith('"""') and not lines[0].startswith('#'):
        issues.append({
            "line": 1,
            "severity": "warning",
            "message": "Consider adding a docstring or comment at the top",
        })

    if language == "python" and any('def ' in line and '->' not in line for line in lines):
        issues.append({
            "line": 1,
            "severity": "warning",
            "message": "Consider adding type hints to function definitions",
        })

    score = max(0.0, min(10.0, 10.0 - (len(issues) * 0.5)))
    return {
        "language": language,
        "lines_analyzed": len(lines),
        "issues": issues,
        "suggestions": ["Add tests", "Cover edge cases"],
        "score": round(score, 1),
    }


if __name__ == "__main__":
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    port = int(os.environ.get("MCP_PORT", "8002"))
    print(f"Starting MCP Code Review Server on {host}:{port}...", flush=True)
    mcp.run(transport="streamable-http", host=host, port=port)

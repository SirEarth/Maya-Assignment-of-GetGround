"""
API (Application Programming Interface) skeleton for Maya Assignment of GetGround.

Built with FastAPI; auto-generates OpenAPI (Open API specification) at /docs.

Run from Apple SDE/:
    uvicorn api.main:app --reload --port 8000

Then open:
    http://localhost:8000/docs        — Swagger UI (interactive)
    http://localhost:8000/redoc       — ReDoc UI (read-only)
    http://localhost:8000/openapi.json — Raw OpenAPI spec
"""

from .main import app

__all__ = ["app"]

"""Exception classes for the Agent Space SDK."""

from typing import Any, Dict, Optional


class AgentSpaceError(Exception):
    """Base exception for all Agent Space SDK errors."""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}


class NotFoundError(AgentSpaceError):
    """Raised when a requested resource is not found (404)."""
    pass


class BadRequestError(AgentSpaceError):
    """Raised when the request is invalid (400)."""
    pass


class TimeoutError(AgentSpaceError):
    """Raised when an operation times out (408)."""
    pass


class InternalError(AgentSpaceError):
    """Raised when the server encounters an internal error (500)."""
    pass


class AuthenticationError(AgentSpaceError):
    """Raised when authentication fails (401)."""
    pass


class RateLimitError(AgentSpaceError):
    """Raised when rate limit is exceeded (429)."""
    pass


class ServiceUnavailableError(AgentSpaceError):
    """Raised when the service is temporarily unavailable (503). Retryable."""
    pass


class GatewayTimeoutError(AgentSpaceError):
    """Raised when a gateway timeout occurs (504). Retryable."""
    pass
"""Authentication utilities for the Agent Space SDK."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class AuthConfig:
    """Configuration for API authentication."""
    api_key: Optional[str] = None
    
    def get_headers(self) -> Dict[str, str]:
        """Get authentication headers."""
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
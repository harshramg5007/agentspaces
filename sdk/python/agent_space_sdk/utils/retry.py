"""Retry logic with exponential backoff for the Agent Space SDK."""

import random
import time
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Optional, Tuple, Type, Union

import requests

from ..exceptions.errors import (
    GatewayTimeoutError,
    InternalError,
    RateLimitError,
    ServiceUnavailableError,
    AgentSpaceError,
)

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: Tuple[Type[Exception], ...] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        AgentSpaceError,
    )
    retry_status_codes: Tuple[int, ...] = (500, 502, 503, 504)


def should_retry(exception: Exception, config: RetryConfig) -> bool:
    """Determine if an exception should trigger a retry."""
    if isinstance(exception, config.retry_on):
        if isinstance(exception, requests.exceptions.HTTPError):
            # Check status code for HTTP errors
            response = getattr(exception, 'response', None)
            if response is not None:
                return response.status_code in config.retry_status_codes
            return True
        if isinstance(exception, AgentSpaceError):
            return isinstance(exception, (ServiceUnavailableError, GatewayTimeoutError, InternalError, RateLimitError))
        return True
    return False


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate the delay before the next retry attempt."""
    delay = min(
        config.base_delay * (config.exponential_base ** attempt),
        config.max_delay
    )
    
    if config.jitter:
        # Add random jitter to prevent thundering herd
        delay *= (0.5 + random.random())
    
    return delay


def retry_with_backoff(config: Optional[RetryConfig] = None) -> Callable:
    """
    Decorator that implements retry logic with exponential backoff.
    
    Args:
        config: Retry configuration. If None, uses default configuration.
    
    Returns:
        Decorated function with retry logic.
    
    Example:
        @retry_with_backoff()
        def make_request():
            response = requests.get('https://api.example.com')
            response.raise_for_status()
            return response
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == config.max_retries or not should_retry(e, config):
                        raise
                    
                    delay = calculate_delay(attempt, config)
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            
        return wrapper
    return decorator

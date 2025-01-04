import time
import logging
from typing import List, Dict, Any
from functools import wraps
import schwabdev
from requests import Response  # Changed to use requests.Response

logger = logging.getLogger(__name__)

def rate_limit(delay: float):
    """Decorator to implement rate limiting."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            time.sleep(delay)
            return result
        return wrapper
    return decorator

class SchwabClient:
    def __init__(self, config: Dict, api_key: str, api_secret: str, callback_url: str):
        """Initialize Schwab API client."""
        self.client = schwabdev.Client(api_key, api_secret, callback_url)
        self.config = config
        self.rate_limit_delay = config['api']['rate_limit_delay']

    @rate_limit(0.5)
    def get_quotes(self, symbols: List[str]) -> Response:
        """Get quotes for a batch of symbols."""
        return self.client.quotes(symbols)

    @rate_limit(0.5)
    def get_option_chains(self, symbol: str, **kwargs) -> Response:
        """Get option chain data for a symbol."""
        return self.client.option_chains(
            symbol,
            **kwargs
        )
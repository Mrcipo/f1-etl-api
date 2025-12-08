"""
HTTP client for Ergast Developer API.
"""
import logging
from typing import Any, Dict, Optional

from etl.config import ERGAST_BASE_URL
from etl.extract.utils import perform_request_with_retries

logger = logging.getLogger(__name__)


class ErgastClient:
    """
    Client for interacting with the Ergast F1 API.
    
    Handles URL construction and delegates HTTP requests to utility functions.
    """
    
    def __init__(self, base_url: str = ERGAST_BASE_URL) -> None:
        """
        Initialize Ergast API client.
        
        Args:
            base_url: Base URL for Ergast API (default from config)
        """
        self.base_url = base_url.rstrip('/')
        logger.info(f"Initialized ErgastClient with base URL: {self.base_url}")
    
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Perform GET request to Ergast API endpoint.
        
        Args:
            path: API endpoint path (e.g., '/2023.json')
            params: Optional query parameters
            
        Returns:
            Parsed JSON response as dictionary
            
        Raises:
            ErgastAPIError: If request fails after retries
        """
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        
        # Construct full URL
        url = f"{self.base_url}{path}"
        
        # Add .json extension if not present
        if not url.endswith('.json'):
            url += '.json'
        
        logger.debug(f"Requesting endpoint: {path}")
        
        # Delegate to utility function with retry logic
        return perform_request_with_retries(url, params=params)
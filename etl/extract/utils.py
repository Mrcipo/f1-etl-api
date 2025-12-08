"""
Utility functions for HTTP requests with retry logic and rate limiting.
"""
import logging
import time
from typing import Any, Dict, Optional

import requests

from etl.config import (
    BACKOFF_FACTOR,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)


class ErgastAPIError(Exception):
    """Custom exception for Ergast API errors."""
    pass


def perform_request_with_retries(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Perform HTTP GET request with exponential backoff retry logic.
    
    Args:
        url: Complete URL to request
        params: Optional query parameters
        timeout: Request timeout in seconds
        
    Returns:
        Parsed JSON response as dictionary
        
    Raises:
        ErgastAPIError: If all retries are exhausted or unrecoverable error occurs
    """
    delay = REQUEST_DELAY_SECONDS
    last_exception = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"Requesting URL: {url} (attempt {attempt}/{MAX_RETRIES})"
            )
            
            # Add basic delay for rate limiting
            if attempt > 1:
                logger.info(f"Waiting {delay:.2f}s before retry...")
                time.sleep(delay)
            
            response = requests.get(url, params=params, timeout=timeout)
            
            # Log response status
            logger.info(f"Response status: {response.status_code}")
            
            # Check if request was successful
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    raise ErgastAPIError(f"Invalid JSON response: {e}")
            
            # Handle client/server errors
            if 400 <= response.status_code < 600:
                logger.warning(
                    f"HTTP {response.status_code} error: {response.text[:200]}"
                )
                response.raise_for_status()
                
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Request timeout on attempt {attempt}: {e}")
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Connection error on attempt {attempt}: {e}")
            
        except requests.exceptions.HTTPError as e:
            last_exception = e
            resp = e.response
            logger.error(f"HTTP error on attempt {attempt}: {e}")

            if resp is not None:
                if resp.status_code == 429:
                    logger.warning("Rate limit hit, will retry with backoff")
                elif 400 <= resp.status_code < 500:
                    raise ErgastAPIError(
                        f"Client error {resp.status_code}: {resp.text[:200]}"
                    )
            else:
                # Si por algún motivo no hay response asociado, tratamos como error genérico
                raise ErgastAPIError(f"HTTP error without response: {e}")
                
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f"Request error on attempt {attempt}: {e}")
        
        # Calculate next delay with exponential backoff
        if attempt < MAX_RETRIES:
            delay *= BACKOFF_FACTOR
    
    # All retries exhausted
    error_msg = (
        f"Failed to fetch {url} after {MAX_RETRIES} attempts. "
        f"Last error: {last_exception}"
    )
    logger.error(error_msg)
    raise ErgastAPIError(error_msg)


def save_json_to_file(data: Dict[str, Any], filepath: str) -> None:
    """
    Save dictionary data to JSON file.
    
    Args:
        data: Dictionary to save
        filepath: Full path to output file
    """
    import json
    import os
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved raw JSON to: {filepath}")
    except Exception as e:
        logger.error(f"Failed to save JSON to {filepath}: {e}")
        raise
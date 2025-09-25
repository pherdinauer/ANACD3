"""HTTP client with retry logic and rate limiting."""

import asyncio
import time
from typing import Any, Dict, Optional, Tuple

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console

from .config import Config
from .utils import jittered_delay, get_timestamp

console = Console()


class HTTPClient:
    """HTTP client with rate limiting and retry logic."""
    
    def __init__(self, config: Config):
        self.config = config
        self.last_request_time = 0.0
        self.rate_limit = 1.0 / config.downloader.rate_limit_rps
        
        # Create httpx client
        self.client = httpx.Client(
            timeout=httpx.Timeout(
                connect=config.http.timeout_connect_s,
                read=config.http.timeout_read_s,
                write=config.http.timeout_read_s,  # Use read timeout for write
                pool=config.http.timeout_connect_s  # Use connect timeout for pool
            ),
            http2=config.http.http2,
            headers=config.http.headers,
            follow_redirects=True
        )
    
    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit:
            sleep_time = self.rate_limit - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    def head(self, url: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Make HEAD request and return headers and error if any."""
        self._wait_for_rate_limit()
        
        try:
            response = self.client.head(url)
            response.raise_for_status()
            
            headers = dict(response.headers)
            return headers, None
            
        except httpx.RequestError as e:
            return {}, str(e)
        except httpx.HTTPStatusError as e:
            return {}, f"HTTP {e.response.status_code}: {e.response.text}"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, Dict[str, Any], Optional[str]]:
        """Make GET request and return content, headers, and error if any."""
        self._wait_for_rate_limit()
        
        try:
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            
            # Debug logging
            content_length = len(response.content)
            if content_length < 1000:  # Suspiciously short response
                content_preview = response.content.decode('utf-8', errors='ignore')[:200]
                console.print(f"[yellow]WARNING: Short response ({content_length} bytes) for {url}[/yellow]")
                console.print(f"[yellow]Content preview: {content_preview}...[/yellow]")
            
            headers = dict(response.headers)
            return response.content, headers, None
            
        except httpx.RequestError as e:
            console.print(f"[red]DEBUG: GET request failed: {e}[/red]")
            return b"", {}, str(e)
        except httpx.HTTPStatusError as e:
            console.print(f"[red]DEBUG: HTTP error {e.response.status_code}: {e.response.text}[/red]")
            return b"", {}, f"HTTP {e.response.status_code}: {e.response.text}"
    
    def get_streaming(self, url: str, headers: Optional[Dict[str, str]] = None):
        """Get streaming response."""
        self._wait_for_rate_limit()
        
        try:
            response = self.client.get(url, headers=headers, stream=True)
            response.raise_for_status()
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            raise e
    
    def get_range(self, url: str, start: int, end: Optional[int] = None, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, Dict[str, Any], Optional[str]]:
        """Make GET request with Range header."""
        self._wait_for_rate_limit()
        
        range_headers = headers or {}
        if end is not None:
            range_headers['Range'] = f'bytes={start}-{end}'
        else:
            range_headers['Range'] = f'bytes={start}-'
        
        try:
            response = self.client.get(url, headers=range_headers)
            
            # Handle partial content
            if response.status_code == 206:
                headers = dict(response.headers)
                return response.content, headers, None
            elif response.status_code == 200:
                # Server doesn't support ranges, return full content
                headers = dict(response.headers)
                return response.content, headers, None
            else:
                response.raise_for_status()
                
        except httpx.RequestError as e:
            return b"", {}, str(e)
        except httpx.HTTPStatusError as e:
            return b"", {}, f"HTTP {e.response.status_code}: {e.response.text}"
    
    def check_resource_info(self, url: str) -> Dict[str, Any]:
        """Check resource information (size, etag, etc.) using HEAD or GET."""
        headers, error = self.head(url)
        
        if error:
            # Fallback to GET with limited content
            content, headers, error = self.get(url)
            if error:
                return {'error': error}
        
        # Extract relevant headers
        info = {
            'url': url,
            'content_length': None,
            'etag': None,
            'last_modified': None,
            'accept_ranges': None,
            'content_type': None
        }
        
        if 'content-length' in headers:
            try:
                info['content_length'] = int(headers['content-length'])
            except ValueError:
                pass
        
        if 'etag' in headers:
            info['etag'] = headers['etag']
        
        if 'last-modified' in headers:
            info['last_modified'] = headers['last-modified']
        
        if 'accept-ranges' in headers:
            info['accept_ranges'] = headers['accept-ranges'].lower() == 'bytes'
        
        if 'content-type' in headers:
            info['content_type'] = headers['content-type']
        
        return info
    
    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AsyncHTTPClient:
    """Async HTTP client with rate limiting and retry logic."""
    
    def __init__(self, config: Config):
        self.config = config
        self.last_request_time = 0.0
        self.rate_limit = 1.0 / config.downloader.rate_limit_rps
        self.semaphore = asyncio.Semaphore(config.crawler.max_concurrency)
        
        # Create httpx async client
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=config.http.timeout_connect_s,
                read=config.http.timeout_read_s
            ),
            http2=config.http.http2,
            headers=config.http.headers,
            follow_redirects=True
        )
    
    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit:
            sleep_time = self.rate_limit - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    async def head(self, url: str) -> Tuple[Dict[str, Any], Optional[str]]:
        """Make HEAD request and return headers and error if any."""
        async with self.semaphore:
            await self._wait_for_rate_limit()
            
            try:
                response = await self.client.head(url)
                response.raise_for_status()
                
                headers = dict(response.headers)
                return headers, None
                
            except httpx.RequestError as e:
                return {}, str(e)
            except httpx.HTTPStatusError as e:
                return {}, f"HTTP {e.response.status_code}: {e.response.text}"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    async def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, Dict[str, Any], Optional[str]]:
        """Make GET request and return content, headers, and error if any."""
        async with self.semaphore:
            await self._wait_for_rate_limit()
            
            try:
                response = await self.client.get(url, headers=headers)
                response.raise_for_status()
                
                headers = dict(response.headers)
                return response.content, headers, None
                
            except httpx.RequestError as e:
                return b"", {}, str(e)
            except httpx.HTTPStatusError as e:
                return b"", {}, f"HTTP {e.response.status_code}: {e.response.text}"
    
    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


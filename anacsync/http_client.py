"""HTTP client with retry logic and rate limiting."""

import asyncio
import random
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
        self.session_cookies = {}
        self.request_count = 0
        
        # User-Agent rotation for better stealth
        self.user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:119.0) Gecko/20100101 Firefox/119.0"
        ]
        
        # Create httpx client
        self.client = httpx.Client(
            timeout=httpx.Timeout(
                connect=config.http.timeout_connect_s,
                read=config.http.timeout_read_s,
                write=config.http.timeout_read_s,  # Use read timeout for write
                pool=config.http.timeout_connect_s  # Use connect timeout for pool
            ),
            http2=config.http.http2,
            headers=self._get_dynamic_headers(),
            follow_redirects=True
        )
    
    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Temporarily disable rate limiting for debugging
        # if time_since_last < self.rate_limit:
        #     sleep_time = self.rate_limit - time_since_last
        #     time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _get_dynamic_headers(self) -> Dict[str, str]:
        """Generate dynamic headers that change between requests."""
        user_agent = random.choice(self.user_agents)
        
        # Base headers that change dynamically
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        # Add random variations to make requests look more natural
        if random.random() < 0.3:  # 30% chance to add DNT header
            headers["DNT"] = "1"
        
        if random.random() < 0.2:  # 20% chance to add Connection header
            headers["Connection"] = "keep-alive"
        
        return headers
    
    def _simulate_human_behavior(self):
        """Simulate human-like behavior with random delays."""
        # Random delay between 0.5 and 2.0 seconds
        delay = random.uniform(0.5, 2.0)
        time.sleep(delay)
        
        # Occasionally add longer pauses (simulating reading)
        if random.random() < 0.1:  # 10% chance
            time.sleep(random.uniform(2.0, 5.0))
    
    def _update_session_cookies(self, response_headers: Dict[str, Any]):
        """Update session cookies from response headers."""
        set_cookie = response_headers.get('set-cookie')
        if set_cookie:
            # Parse cookies and store them
            for cookie in set_cookie.split(','):
                if '=' in cookie:
                    name, value = cookie.split('=', 1)
                    self.session_cookies[name.strip()] = value.strip().split(';')[0]
    
    def initialize_session(self):
        """Initialize session by visiting the homepage first."""
        console.print("[blue]Initializing browser session...[/blue]")
        
        # Visit homepage first to establish session
        homepage_url = "https://dati.anticorruzione.it/"
        try:
            response = self.client.get(homepage_url, headers=self._get_dynamic_headers())
            response.raise_for_status()
            self._update_session_cookies(dict(response.headers))
            console.print("[green]Session initialized successfully[/green]")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not initialize session: {e}[/yellow]")
        
        # Small delay to simulate human behavior
        time.sleep(random.uniform(1.0, 3.0))
    
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
        self._simulate_human_behavior()
        
        # Update request count and rotate headers
        self.request_count += 1
        
        # Use dynamic headers for each request
        dynamic_headers = self._get_dynamic_headers()
        if headers:
            dynamic_headers.update(headers)
        
        # Add referer for subsequent requests (simulate navigation)
        if self.request_count > 1:
            dynamic_headers["Referer"] = "https://dati.anticorruzione.it/opendata/dataset"
        
        # Add session cookies if available
        if self.session_cookies:
            cookie_string = "; ".join([f"{name}={value}" for name, value in self.session_cookies.items()])
            dynamic_headers["Cookie"] = cookie_string
        
        try:
            response = self.client.get(url, headers=dynamic_headers)
            response.raise_for_status()
            
            # Update session cookies from response
            self._update_session_cookies(dict(response.headers))
            
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


"""Download strategies implementation."""

import json
import os
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console

from ..config import Config
from ..http_client import HTTPClient
from ..utils import (
    atomic_write, calculate_sha256, get_timestamp, 
    jittered_delay, sleep_with_jitter, format_bytes
)

console = Console()


@dataclass
class DownloadResult:
    """Download result."""
    ok: bool
    bytes_written: int
    strategy: str
    etag: Optional[str] = None
    error: Optional[str] = None
    duration: float = 0.0


class StrategyBase(ABC):
    """Base class for download strategies."""
    
    def __init__(self, config: Config):
        self.config = config
        self.name = self.__class__.__name__
    
    @abstractmethod
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download file using this strategy."""
        pass
    
    def _get_chunk_size(self, file_size: Optional[int]) -> int:
        """Get appropriate chunk size based on file size."""
        if not file_size:
            return self.config.downloader.dynamic_chunks_mb[0] * 1024 * 1024
        
        size_mb = file_size / (1024 * 1024)
        
        if size_mb < 50:
            return self.config.downloader.dynamic_chunks_mb[0] * 1024 * 1024
        elif size_mb < 300:
            return self.config.downloader.dynamic_chunks_mb[1] * 1024 * 1024
        else:
            return self.config.downloader.dynamic_chunks_mb[2] * 1024 * 1024
    
    def _save_sidecar_meta(self, dest_path: Path, meta: Dict[str, Any]) -> None:
        """Save sidecar metadata."""
        meta_path = dest_path.with_suffix(dest_path.suffix + '.meta.json')
        atomic_write(meta_path, json.dumps(meta, indent=2, ensure_ascii=False))
    
    def _load_sidecar_meta(self, dest_path: Path) -> Dict[str, Any]:
        """Load sidecar metadata."""
        meta_path = dest_path.with_suffix(dest_path.suffix + '.meta.json')
        if meta_path.exists():
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}


class S1DynamicStrategy(StrategyBase):
    """S1 - Dynamic Range Streaming with adaptive chunk sizes."""
    
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download using dynamic range streaming."""
        start_time = time.time()
        
        try:
            with HTTPClient(cfg) as http_client:
                # Get file info
                resource_info = http_client.check_resource_info(url)
                if 'error' in resource_info:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"Failed to get resource info: {resource_info['error']}"
                    )
                
                file_size = resource_info.get('content_length')
                etag = resource_info.get('etag')
                
                # Check if file already exists and is complete
                if dest_path.exists() and file_size:
                    if dest_path.stat().st_size == file_size:
                        # Verify integrity
                        current_hash = calculate_sha256(dest_path)
                        if current_hash == meta.get('sha256'):
                            return DownloadResult(
                                ok=True, bytes_written=file_size, strategy=self.name,
                                etag=etag, duration=time.time() - start_time
                            )
                
                # Determine chunk size
                chunk_size = self._get_chunk_size(file_size)
                overlap = cfg.downloader.overlap_bytes
                
                # Download in chunks
                temp_path = dest_path.with_suffix(dest_path.suffix + '.part')
                bytes_written = 0
                offset = 0
                
                # Check for existing partial download
                if temp_path.exists():
                    offset = temp_path.stat().st_size
                    bytes_written = offset
                    # Adjust offset to account for overlap
                    if offset > overlap:
                        offset -= overlap
                
                with open(temp_path, 'ab' if offset > 0 else 'wb') as f:
                    while True:
                        if file_size and offset >= file_size:
                            break
                        
                        # Calculate range
                        end = offset + chunk_size - 1
                        if file_size:
                            end = min(end, file_size - 1)
                        
                        # Download chunk
                        content, headers, error = http_client.get_range(url, offset, end)
                        if error:
                            return DownloadResult(
                                ok=False, bytes_written=bytes_written, strategy=self.name,
                                error=f"Range request failed: {error}"
                            )
                        
                        if not content:
                            break
                        
                        # Write chunk
                        f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                        
                        bytes_written += len(content)
                        offset += len(content)
                        
                        # Rate limiting
                        sleep_with_jitter(100, 200)
                
                # Atomic move
                os.replace(temp_path, dest_path)
                
                # Calculate final hash
                final_hash = calculate_sha256(dest_path)
                
                # Update metadata
                meta.update({
                    'sha256': final_hash,
                    'downloaded_at': get_timestamp(),
                    'strategy': self.name,
                    'etag': etag,
                    'content_length': file_size
                })
                self._save_sidecar_meta(dest_path, meta)
                
                return DownloadResult(
                    ok=True, bytes_written=bytes_written, strategy=self.name,
                    etag=etag, duration=time.time() - start_time
                )
        
        except Exception as e:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=self.name,
                error=str(e), duration=time.time() - start_time
            )


class S2SparseStrategy(StrategyBase):
    """S2 - Sparse Segments with Bitmap."""
    
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download using sparse segments with bitmap."""
        start_time = time.time()
        
        try:
            with HTTPClient(cfg) as http_client:
                # Get file info
                resource_info = http_client.check_resource_info(url)
                if 'error' in resource_info:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"Failed to get resource info: {resource_info['error']}"
                    )
                
                file_size = resource_info.get('content_length')
                etag = resource_info.get('etag')
                
                if not file_size:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error="File size unknown, cannot use sparse strategy"
                    )
                
                # Segment configuration
                segment_size = cfg.downloader.sparse_segment_mb * 1024 * 1024
                num_segments = (file_size + segment_size - 1) // segment_size
                
                # Load existing bitmap
                existing_meta = self._load_sidecar_meta(dest_path)
                bitmap = existing_meta.get('segments', {}).get('bitmap', '0' * num_segments)
                
                # Ensure bitmap is correct length
                if len(bitmap) != num_segments:
                    bitmap = '0' * num_segments
                
                # Create temp file
                temp_path = dest_path.with_suffix(dest_path.suffix + '.part')
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Initialize file with correct size
                with open(temp_path, 'wb') as f:
                    f.seek(file_size - 1)
                    f.write(b'\0')
                
                # Download segments in non-linear order
                segment_order = self._get_segment_order(num_segments)
                bytes_written = 0
                
                for segment_idx in segment_order:
                    if bitmap[segment_idx] == '1':
                        continue  # Already downloaded
                    
                    start = segment_idx * segment_size
                    end = min(start + segment_size - 1, file_size - 1)
                    
                    # Download segment
                    content, headers, error = http_client.get_range(url, start, end)
                    if error:
                        continue  # Skip failed segment
                    
                    # Write segment
                    with open(temp_path, 'r+b') as f:
                        f.seek(start)
                        f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                    
                    bytes_written += len(content)
                    
                    # Update bitmap
                    bitmap = bitmap[:segment_idx] + '1' + bitmap[segment_idx + 1:]
                    
                    # Rate limiting
                    sleep_with_jitter(100, 200)
                
                # Check if all segments downloaded
                if '0' in bitmap:
                    return DownloadResult(
                        ok=False, bytes_written=bytes_written, strategy=self.name,
                        error="Not all segments downloaded successfully"
                    )
                
                # Atomic move
                os.replace(temp_path, dest_path)
                
                # Calculate final hash
                final_hash = calculate_sha256(dest_path)
                
                # Update metadata
                meta.update({
                    'sha256': final_hash,
                    'downloaded_at': get_timestamp(),
                    'strategy': self.name,
                    'etag': etag,
                    'content_length': file_size,
                    'segments': {
                        'size': segment_size,
                        'bitmap': bitmap
                    }
                })
                self._save_sidecar_meta(dest_path, meta)
                
                return DownloadResult(
                    ok=True, bytes_written=bytes_written, strategy=self.name,
                    etag=etag, duration=time.time() - start_time
                )
        
        except Exception as e:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=self.name,
                error=str(e), duration=time.time() - start_time
            )
    
    def _get_segment_order(self, num_segments: int) -> List[int]:
        """Get non-linear segment download order."""
        if num_segments <= 1:
            return [0]
        
        # Start with beginning, end, then middle, then fill gaps
        order = []
        order.append(0)  # Start
        if num_segments > 1:
            order.append(num_segments - 1)  # End
        
        # Add middle segments
        if num_segments > 2:
            middle = num_segments // 2
            order.append(middle)
        
        # Fill remaining segments
        for i in range(1, num_segments - 1):
            if i not in order:
                order.append(i)
        
        return order


class S3CurlStrategy(StrategyBase):
    """S3 - External curl tool."""
    
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download using external curl tool."""
        start_time = time.time()
        
        try:
            if not cfg.downloader.enable_curl:
                return DownloadResult(
                    ok=False, bytes_written=0, strategy=self.name,
                    error="Curl strategy disabled in configuration"
                )
            
            # Check if curl is available
            try:
                subprocess.run([cfg.downloader.curl_path, '--version'], 
                             capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                return DownloadResult(
                    ok=False, bytes_written=0, strategy=self.name,
                    error=f"Curl not found at {cfg.downloader.curl_path}"
                )
            
            # Prepare curl command
            temp_path = dest_path.with_suffix(dest_path.suffix + '.part')
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            
            cmd = [
                cfg.downloader.curl_path,
                '--location',
                '--retry', '10',
                '--retry-delay', '5',
                '--limit-rate', '200k',
                '--continue-at', '-',  # Resume if possible
                '--output', str(temp_path),
                '--write-out', '%{http_code}:%{size_download}:%{time_total}',
                url
            ]
            
            # Execute curl
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                return DownloadResult(
                    ok=False, bytes_written=0, strategy=self.name,
                    error=f"Curl failed: {result.stderr}"
                )
            
            # Parse output
            output_parts = result.stdout.strip().split(':')
            if len(output_parts) >= 3:
                http_code = int(output_parts[0])
                bytes_downloaded = int(output_parts[1])
                time_total = float(output_parts[2])
                
                if http_code not in [200, 206]:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"HTTP {http_code}"
                    )
            else:
                bytes_downloaded = temp_path.stat().st_size if temp_path.exists() else 0
            
            # Atomic move
            if temp_path.exists():
                os.replace(temp_path, dest_path)
                
                # Calculate hash
                final_hash = calculate_sha256(dest_path)
                
                # Update metadata
                meta.update({
                    'sha256': final_hash,
                    'downloaded_at': get_timestamp(),
                    'strategy': self.name,
                    'content_length': bytes_downloaded
                })
                self._save_sidecar_meta(dest_path, meta)
                
                return DownloadResult(
                    ok=True, bytes_written=bytes_downloaded, strategy=self.name,
                    duration=time.time() - start_time
                )
            else:
                return DownloadResult(
                    ok=False, bytes_written=0, strategy=self.name,
                    error="No file created by curl"
                )
        
        except Exception as e:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=self.name,
                error=str(e), duration=time.time() - start_time
            )


class S4ShortConnStrategy(StrategyBase):
    """S4 - Short Connections with small chunks."""
    
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download using short connections with small chunks."""
        start_time = time.time()
        
        try:
            with HTTPClient(cfg) as http_client:
                # Get file info
                resource_info = http_client.check_resource_info(url)
                if 'error' in resource_info:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"Failed to get resource info: {resource_info['error']}"
                    )
                
                file_size = resource_info.get('content_length')
                etag = resource_info.get('etag')
                
                # Small chunk size
                chunk_size = cfg.downloader.snail_chunks_kb * 1024
                
                temp_path = dest_path.with_suffix(dest_path.suffix + '.part')
                bytes_written = 0
                offset = 0
                
                # Check for existing partial download
                if temp_path.exists():
                    offset = temp_path.stat().st_size
                    bytes_written = offset
                
                with open(temp_path, 'ab' if offset > 0 else 'wb') as f:
                    while True:
                        if file_size and offset >= file_size:
                            break
                        
                        # Calculate range
                        end = offset + chunk_size - 1
                        if file_size:
                            end = min(end, file_size - 1)
                        
                        # Download chunk with Connection: close
                        headers = {'Connection': 'close'}
                        content, resp_headers, error = http_client.get_range(url, offset, end, headers)
                        
                        if error:
                            return DownloadResult(
                                ok=False, bytes_written=bytes_written, strategy=self.name,
                                error=f"Chunk download failed: {error}"
                            )
                        
                        if not content:
                            break
                        
                        # Write chunk
                        f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                        
                        bytes_written += len(content)
                        offset += len(content)
                        
                        # Small delay between chunks
                        sleep_with_jitter(50, 100)
                
                # Atomic move
                os.replace(temp_path, dest_path)
                
                # Calculate final hash
                final_hash = calculate_sha256(dest_path)
                
                # Update metadata
                meta.update({
                    'sha256': final_hash,
                    'downloaded_at': get_timestamp(),
                    'strategy': self.name,
                    'etag': etag,
                    'content_length': file_size
                })
                self._save_sidecar_meta(dest_path, meta)
                
                return DownloadResult(
                    ok=True, bytes_written=bytes_written, strategy=self.name,
                    etag=etag, duration=time.time() - start_time
                )
        
        except Exception as e:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=self.name,
                error=str(e), duration=time.time() - start_time
            )


class S5TailFirstStrategy(StrategyBase):
    """S5 - Tail-First download strategy."""
    
    def fetch(self, url: str, dest_path: Path, meta: Dict[str, Any], cfg: Config) -> DownloadResult:
        """Download using tail-first strategy."""
        start_time = time.time()
        
        try:
            with HTTPClient(cfg) as http_client:
                # Get file info
                resource_info = http_client.check_resource_info(url)
                if 'error' in resource_info:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"Failed to get resource info: {resource_info['error']}"
                    )
                
                file_size = resource_info.get('content_length')
                etag = resource_info.get('etag')
                
                if not file_size:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error="File size unknown, cannot use tail-first strategy"
                    )
                
                # Download last 1MB first to validate
                tail_size = min(1024 * 1024, file_size)
                tail_start = file_size - tail_size
                
                # Download tail
                tail_content, headers, error = http_client.get_range(url, tail_start, file_size - 1)
                if error:
                    return DownloadResult(
                        ok=False, bytes_written=0, strategy=self.name,
                        error=f"Tail download failed: {error}"
                    )
                
                # Create temp file
                temp_path = dest_path.with_suffix(dest_path.suffix + '.part')
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Initialize file with correct size
                with open(temp_path, 'wb') as f:
                    f.seek(file_size - 1)
                    f.write(b'\0')
                
                # Write tail
                with open(temp_path, 'r+b') as f:
                    f.seek(tail_start)
                    f.write(tail_content)
                    f.flush()
                    os.fsync(f.fileno())
                
                # Download remaining content
                chunk_size = self._get_chunk_size(file_size)
                bytes_written = len(tail_content)
                offset = 0
                
                with open(temp_path, 'r+b') as f:
                    while offset < tail_start:
                        end = min(offset + chunk_size - 1, tail_start - 1)
                        
                        # Download chunk
                        content, headers, error = http_client.get_range(url, offset, end)
                        if error:
                            return DownloadResult(
                                ok=False, bytes_written=bytes_written, strategy=self.name,
                                error=f"Chunk download failed: {error}"
                            )
                        
                        if not content:
                            break
                        
                        # Write chunk
                        f.seek(offset)
                        f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                        
                        bytes_written += len(content)
                        offset += len(content)
                        
                        # Rate limiting
                        sleep_with_jitter(100, 200)
                
                # Atomic move
                os.replace(temp_path, dest_path)
                
                # Calculate final hash
                final_hash = calculate_sha256(dest_path)
                
                # Update metadata
                meta.update({
                    'sha256': final_hash,
                    'downloaded_at': get_timestamp(),
                    'strategy': self.name,
                    'etag': etag,
                    'content_length': file_size
                })
                self._save_sidecar_meta(dest_path, meta)
                
                return DownloadResult(
                    ok=True, bytes_written=bytes_written, strategy=self.name,
                    etag=etag, duration=time.time() - start_time
                )
        
        except Exception as e:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=self.name,
                error=str(e), duration=time.time() - start_time
            )


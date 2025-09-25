"""Utility functions for ANAC Sync."""

import hashlib
import json
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn


console = Console()


def atomic_write(file_path: Path, content: Union[str, bytes], mode: str = 'w') -> None:
    """Atomically write content to a file."""
    temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
    
    try:
        if mode == 'w':
            with open(temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
                # Ensure data is written to disk (only on Unix-like systems)
                if hasattr(f, 'fileno') and hasattr(os, 'fsync'):
                    try:
                        os.fsync(f.fileno())
                    except (OSError, AttributeError):
                        # fsync not available or not supported (e.g., Windows)
                        pass
        elif mode == 'wb':
            with open(temp_path, 'wb') as f:
                f.write(content)
                # Ensure data is written to disk (only on Unix-like systems)
                if hasattr(f, 'fileno') and hasattr(os, 'fsync'):
                    try:
                        os.fsync(f.fileno())
                    except (OSError, AttributeError):
                        # fsync not available or not supported (e.g., Windows)
                        pass
        else:
            raise ValueError(f"Unsupported mode: {mode}")
        
        # Atomic rename
        os.replace(temp_path, file_path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA256 hash of a file in streaming mode."""
    sha256_hash = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def calculate_sha256_streaming(file_path: Path, progress_callback: Optional[callable] = None) -> str:
    """Calculate SHA256 hash with progress callback."""
    sha256_hash = hashlib.sha256()
    file_size = file_path.stat().st_size
    bytes_read = 0
    
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            
            sha256_hash.update(chunk)
            bytes_read += len(chunk)
            
            if progress_callback:
                progress_callback(bytes_read, file_size)
    
    return sha256_hash.hexdigest()


def append_jsonl(file_path: Path, record: Dict[str, Any]) -> None:
    """Append a record to a JSONL file atomically."""
    line = json.dumps(record, ensure_ascii=False) + '\n'
    
    # Create directory if it doesn't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Append to file
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    """Read records from a JSONL file."""
    if not file_path.exists():
        return
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def load_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """Load all records from a JSONL file."""
    return list(read_jsonl(file_path))


def save_jsonl(file_path: Path, records: List[Dict[str, Any]]) -> None:
    """Save records to a JSONL file atomically."""
    content = '\n'.join(json.dumps(record, ensure_ascii=False) for record in records)
    atomic_write(file_path, content)


def merge_jsonl_records(
    existing_records: List[Dict[str, Any]], 
    new_records: List[Dict[str, Any]], 
    key_fields: List[str]
) -> List[Dict[str, Any]]:
    """Merge new records with existing ones based on key fields."""
    # Create lookup for existing records
    existing_lookup = {}
    for record in existing_records:
        key = tuple(record.get(field) for field in key_fields)
        existing_lookup[key] = record
    
    # Merge with new records
    for record in new_records:
        key = tuple(record.get(field) for field in key_fields)
        if key in existing_lookup:
            # Update existing record
            existing_lookup[key].update(record)
        else:
            # Add new record
            existing_lookup[key] = record
    
    return list(existing_lookup.values())


def format_bytes(bytes_count: int) -> str:
    """Format bytes count in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in human readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def jittered_delay(base_delay_ms: int, max_jitter_ms: int = 100) -> float:
    """Return a jittered delay in seconds."""
    jitter = random.uniform(0, max_jitter_ms)
    total_delay_ms = base_delay_ms + jitter
    return total_delay_ms / 1000.0


def sleep_with_jitter(base_delay_ms: int, max_jitter_ms: int = 100) -> None:
    """Sleep for a jittered amount of time."""
    delay = jittered_delay(base_delay_ms, max_jitter_ms)
    time.sleep(delay)


def get_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.utcnow().isoformat() + 'Z'


def parse_http_date(date_str: str) -> Optional[datetime]:
    """Parse HTTP date string to datetime."""
    if not date_str:
        return None
    
    # Common HTTP date formats
    formats = [
        '%a, %d %b %Y %H:%M:%S %Z',
        '%a, %d %b %Y %H:%M:%S GMT',
        '%A, %d-%b-%y %H:%M:%S %Z',
        '%A, %d-%b-%y %H:%M:%S GMT',
        '%a %b %d %H:%M:%S %Y',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None


def extract_filename_from_url(url: str, content_disposition: Optional[str] = None) -> str:
    """Extract filename from URL or Content-Disposition header."""
    if content_disposition:
        # Parse Content-Disposition header
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"\'')
            if filename:
                return filename
    
    # Extract from URL
    path = url.split('?')[0].split('#')[0]
    filename = Path(path).name
    
    if not filename or filename == '/':
        return 'download'
    
    return filename


def create_progress_bar(description: str) -> Progress:
    """Create a progress bar with standard configuration."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    )


def safe_filename(filename: str) -> str:
    """Make filename safe for filesystem."""
    # Remove or replace unsafe characters
    unsafe_chars = '<>:"/\\|?*'
    for char in unsafe_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing dots and spaces
    filename = filename.strip('. ')
    
    # Ensure it's not empty
    if not filename:
        filename = 'unnamed'
    
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    
    return filename


def ensure_directory(path: Path) -> None:
    """Ensure directory exists, create if necessary."""
    path.mkdir(parents=True, exist_ok=True)


def get_file_info(file_path: Path) -> Dict[str, Any]:
    """Get file information including size, mtime, and sha256."""
    if not file_path.exists():
        return {}
    
    stat = file_path.stat()
    return {
        'size': stat.st_size,
        'mtime': datetime.fromtimestamp(stat.st_mtime).isoformat() + 'Z',
        'sha256': calculate_sha256(file_path)
    }


def retry_with_backoff(
    func: callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Any:
    """Retry function with exponential backoff."""
    delay = base_delay
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            if attempt == max_retries:
                raise e
            
            time.sleep(delay)
            delay = min(delay * backoff_factor, max_delay)
    
    raise RuntimeError("Should not reach here")


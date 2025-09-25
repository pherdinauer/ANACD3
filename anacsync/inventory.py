"""Local file inventory scanner."""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .config import Config
from .utils import (
    load_jsonl, save_jsonl, get_file_info, calculate_sha256,
    get_timestamp, safe_filename, ensure_directory
)

console = Console()


class LocalFileRecord:
    """Local file record structure."""
    
    def __init__(
        self,
        path: str,
        sha256: str,
        size: int,
        mtime: str,
        dataset_slug: Optional[str] = None,
        url: Optional[str] = None
    ):
        self.path = path
        self.sha256 = sha256
        self.size = size
        self.mtime = mtime
        self.dataset_slug = dataset_slug
        self.url = url
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'path': self.path,
            'sha256': self.sha256,
            'size': self.size,
            'mtime': self.mtime,
            'dataset_slug': self.dataset_slug,
            'url': self.url
        }


class InventoryScanner:
    """Local file inventory scanner."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.inventory_file = self.state_dir / 'local' / 'inventory.jsonl'
        self.root_dir = Path(config.root_dir)
        
        # Load existing inventory
        self.existing_files = {r['path']: r for r in load_jsonl(self.inventory_file)}
        
        # Supported file extensions - only JSON files
        self.supported_extensions = {'.json', '.ndjson'}
    
    def is_supported_file(self, file_path: Path) -> bool:
        """Check if file has supported extension."""
        return file_path.suffix.lower() in self.supported_extensions
    
    def load_sidecar_meta(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load metadata from sidecar file."""
        meta_path = file_path.with_suffix(file_path.suffix + '.meta.json')
        
        if not meta_path.exists():
            return None
        
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    
    def save_sidecar_meta(self, file_path: Path, meta: Dict[str, Any]) -> None:
        """Save metadata to sidecar file."""
        meta_path = file_path.with_suffix(file_path.suffix + '.meta.json')
        ensure_directory(meta_path.parent)
        
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
    
    def extract_dataset_slug_from_path(self, file_path: Path) -> Optional[str]:
        """Extract dataset slug from file path using heuristics."""
        path_str = str(file_path)
        
        # Pattern 1: Look for common ANAC dataset patterns in path
        patterns = [
            r'ocds-appalti-ordinari-(\d{4})',
            r'ocds-appalti-(\d{4})',
            r'appalti-ordinari-(\d{4})',
            r'stazioni-appaltanti',
            r'subappalti',
            r'aggiudicazioni',
            r'contratti'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, path_str, re.IGNORECASE)
            if match:
                return match.group(0)
        
        # Pattern 2: Look for UUID-like patterns
        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        match = re.search(uuid_pattern, path_str, re.IGNORECASE)
        if match:
            return match.group(0)
        
        # Pattern 3: Use filename without extension
        filename = file_path.stem
        if len(filename) > 3:
            return safe_filename(filename)
        
        return None
    
    def reconcile_with_catalog(self, file_path: Path, file_info: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Try to reconcile local file with catalog resources."""
        # First, try sidecar metadata
        meta = self.load_sidecar_meta(file_path)
        if meta:
            return meta.get('dataset_slug'), meta.get('url')
        
        # Try to match by filename patterns
        filename = file_path.name
        dataset_slug = self.extract_dataset_slug_from_path(file_path)
        
        if dataset_slug:
            # Try to find matching resource in catalog
            resources_file = self.state_dir / 'catalog' / 'resources.jsonl'
            if resources_file.exists():
                for resource in load_jsonl(resources_file):
                    if resource.get('dataset_slug') == dataset_slug:
                        # Check if filename matches
                        resource_name = resource.get('name', '')
                        if filename in resource_name or resource_name in filename:
                            return dataset_slug, resource.get('url')
        
        return dataset_slug, None
    
    def scan_file(self, file_path: Path) -> Optional[LocalFileRecord]:
        """Scan a single file and return record."""
        if not file_path.exists() or not file_path.is_file():
            return None
        
        if not self.is_supported_file(file_path):
            return None
        
        try:
            # Get file info
            file_info = get_file_info(file_path)
            if not file_info:
                return None
            
            # Try to reconcile with catalog
            dataset_slug, url = self.reconcile_with_catalog(file_path, file_info)
            
            return LocalFileRecord(
                path=str(file_path),
                sha256=file_info['sha256'],
                size=file_info['size'],
                mtime=file_info['mtime'],
                dataset_slug=dataset_slug,
                url=url
            )
        
        except Exception as e:
            console.print(f"[yellow]Warning: Could not scan {file_path}: {e}[/yellow]")
            return None
    
    def scan_directory(self, directory: Path) -> List[LocalFileRecord]:
        """Scan a directory recursively for supported files."""
        records = []
        
        if not directory.exists() or not directory.is_dir():
            return records
        
        try:
            # Walk through directory recursively
            for file_path in directory.rglob('*'):
                if file_path.is_file():
                    record = self.scan_file(file_path)
                    if record:
                        records.append(record)
        
        except PermissionError:
            console.print(f"[yellow]Warning: Permission denied accessing {directory}[/yellow]")
        except Exception as e:
            console.print(f"[yellow]Warning: Error scanning {directory}: {e}[/yellow]")
        
        return records
    
    def scan_local(self) -> Dict[str, Any]:
        """Scan local files and update inventory."""
        console.print("[bold blue]Scanning local files...[/bold blue]")
        
        stats = {
            'files_scanned': 0,
            'files_found': 0,
            'files_new': 0,
            'files_updated': 0,
            'files_removed': 0,
            'directories_scanned': 0
        }
        
        # Ensure root directory exists
        if not self.root_dir.exists():
            console.print(f"[yellow]Root directory {self.root_dir} does not exist, creating...[/yellow]")
            ensure_directory(self.root_dir)
            return stats
        
        # Scan root directory
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Scanning files...", total=None)
            
            # Get all files first to show progress
            all_files = []
            for file_path in self.root_dir.rglob('*'):
                if file_path.is_file() and self.is_supported_file(file_path):
                    all_files.append(file_path)
            
            progress.update(task, total=len(all_files))
            
            for i, file_path in enumerate(all_files):
                progress.update(task, description=f"Scanning {file_path.name}...")
                
                record = self.scan_file(file_path)
                if record:
                    stats['files_found'] += 1
                    
                    # Check if file is new or updated
                    existing = self.existing_files.get(record.path)
                    if existing:
                        # Check if file has changed
                        if (existing['sha256'] != record.sha256 or 
                            existing['size'] != record.size or 
                            existing['mtime'] != record.mtime):
                            self.existing_files[record.path] = record.to_dict()
                            stats['files_updated'] += 1
                        # else: file unchanged, keep existing record
                    else:
                        # New file
                        self.existing_files[record.path] = record.to_dict()
                        stats['files_new'] += 1
                
                stats['files_scanned'] += 1
                progress.advance(task)
        
        # Check for removed files
        current_files = set()
        for file_path in self.root_dir.rglob('*'):
            if file_path.is_file() and self.is_supported_file(file_path):
                current_files.add(str(file_path))
        
        removed_files = set(self.existing_files.keys()) - current_files
        for removed_path in removed_files:
            del self.existing_files[removed_path]
            stats['files_removed'] += 1
        
        # Save updated inventory
        save_jsonl(self.inventory_file, list(self.existing_files.values()))
        
        console.print(f"[green]Scan completed![/green]")
        console.print(f"Files scanned: {stats['files_scanned']}")
        console.print(f"Files found: {stats['files_found']} (new: {stats['files_new']}, updated: {stats['files_updated']}, removed: {stats['files_removed']})")
        
        return stats
    
    def get_file_by_path(self, path: str) -> Optional[Dict[str, Any]]:
        """Get file record by path."""
        return self.existing_files.get(path)
    
    def get_files_by_dataset(self, dataset_slug: str) -> List[Dict[str, Any]]:
        """Get all files for a specific dataset."""
        return [
            record for record in self.existing_files.values()
            if record.get('dataset_slug') == dataset_slug
        ]
    
    def get_orphaned_files(self) -> List[Dict[str, Any]]:
        """Get files that don't match any dataset."""
        return [
            record for record in self.existing_files.values()
            if not record.get('dataset_slug')
        ]
    
    def verify_file_integrity(self, file_path: Path) -> bool:
        """Verify file integrity by recalculating hash."""
        if not file_path.exists():
            return False
        
        try:
            current_hash = calculate_sha256(file_path)
            record = self.get_file_by_path(str(file_path))
            
            if record:
                return record['sha256'] == current_hash
            else:
                # File not in inventory, calculate and store
                file_info = get_file_info(file_path)
                if file_info:
                    new_record = LocalFileRecord(
                        path=str(file_path),
                        sha256=file_info['sha256'],
                        size=file_info['size'],
                        mtime=file_info['mtime']
                    )
                    self.existing_files[str(file_path)] = new_record.to_dict()
                    save_jsonl(self.inventory_file, list(self.existing_files.values()))
                    return True
            
            return False
        
        except Exception:
            return False


def scan_local(config: Config) -> Dict[str, Any]:
    """Main function to scan local files and update inventory."""
    scanner = InventoryScanner(config)
    return scanner.scan_local()


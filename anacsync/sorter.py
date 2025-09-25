"""File sorting based on configurable rules."""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.table import Table

from .config import Config, SortingRule
from .utils import (
    load_jsonl, save_jsonl, atomic_write, get_timestamp,
    ensure_directory, safe_filename
)

console = Console()


class FileSorter:
    """File sorter that applies configurable rules."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.root_dir = Path(config.root_dir)
        self.inventory_file = self.state_dir / 'local' / 'inventory.jsonl'
        
        # Load existing inventory
        self.inventory = {r['path']: r for r in load_jsonl(self.inventory_file)}
    
    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a sorting condition against file context."""
        try:
            # Parse condition
            if ' matches ' in condition:
                field, pattern = condition.split(' matches ', 1)
                field = field.strip()
                pattern = pattern.strip().strip('"\'')
                
                value = context.get(field, '')
                if isinstance(value, str):
                    return bool(re.search(pattern, value, re.IGNORECASE))
            
            elif ' contains ' in condition:
                field, substring = condition.split(' contains ', 1)
                field = field.strip()
                substring = substring.strip().strip('"\'')
                
                value = context.get(field, '')
                if isinstance(value, str):
                    return substring.lower() in value.lower()
            
            elif ' == ' in condition:
                field, expected = condition.split(' == ', 1)
                field = field.strip()
                expected = expected.strip().strip('"\'')
                
                value = context.get(field, '')
                return str(value) == expected
            
            elif ' != ' in condition:
                field, expected = condition.split(' != ', 1)
                field = field.strip()
                expected = expected.strip().strip('"\'')
                
                value = context.get(field, '')
                return str(value) != expected
            
            elif condition.strip() == 'true':
                return True
            
            elif condition.strip() == 'false':
                return False
            
            else:
                # Try to evaluate as Python expression (with limited context)
                safe_context = {
                    'slug': context.get('slug', ''),
                    'filename': context.get('filename', ''),
                    'url': context.get('url', ''),
                    'format': context.get('format', ''),
                    'size': context.get('size', 0),
                    'path': context.get('path', ''),
                    'dataset_slug': context.get('dataset_slug', '')
                }
                
                # Simple evaluation for basic conditions
                return eval(condition, {"__builtins__": {}}, safe_context)
        
        except Exception as e:
            console.print(f"[yellow]Warning: Could not evaluate condition '{condition}': {e}[/yellow]")
            return False
    
    def _get_file_context(self, file_path: Path, inventory_record: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get context information for a file."""
        context = {
            'path': str(file_path),
            'filename': file_path.name,
            'stem': file_path.stem,
            'suffix': file_path.suffix,
            'parent': str(file_path.parent),
            'size': file_path.stat().st_size if file_path.exists() else 0
        }
        
        # Add inventory information
        if inventory_record:
            context.update({
                'dataset_slug': inventory_record.get('dataset_slug', ''),
                'url': inventory_record.get('url', ''),
                'sha256': inventory_record.get('sha256', ''),
                'mtime': inventory_record.get('mtime', '')
            })
            
            # Extract slug from dataset_slug or path
            slug = inventory_record.get('dataset_slug', '')
            if not slug:
                # Try to extract from path
                path_parts = file_path.parts
                for part in path_parts:
                    if 'ocds' in part.lower() or 'appalti' in part.lower():
                        slug = part
                        break
            
            context['slug'] = slug
        
        # Try to determine format from extension
        ext = file_path.suffix.lower()
        if ext == '.json':
            context['format'] = 'JSON'
        elif ext == '.csv':
            context['format'] = 'CSV'
        elif ext == '.xlsx':
            context['format'] = 'XLSX'
        elif ext == '.xml':
            context['format'] = 'XML'
        elif ext == '.zip':
            context['format'] = 'ZIP'
        else:
            context['format'] = 'UNKNOWN'
        
        return context
    
    def _apply_rule(self, file_path: Path, rule: SortingRule, context: Dict[str, Any]) -> Optional[Path]:
        """Apply a single sorting rule to a file."""
        # Evaluate condition
        if not self._evaluate_condition(rule.if_, context):
            return None
        
        # Determine destination
        if rule.move_to:
            dest_path = Path(rule.move_to)
        elif rule.default:
            dest_path = Path(rule.default)
        else:
            return None
        
        # Make path absolute if relative
        if not dest_path.is_absolute():
            dest_path = self.root_dir / dest_path
        
        # Ensure destination is a directory
        if not dest_path.suffix:
            # It's a directory
            ensure_directory(dest_path)
            return dest_path / file_path.name
        else:
            # It's a file path
            ensure_directory(dest_path.parent)
            return dest_path
    
    def _move_file(self, src_path: Path, dest_path: Path) -> bool:
        """Move file atomically and update inventory."""
        try:
            if not src_path.exists():
                return False
            
            # Ensure destination directory exists
            ensure_directory(dest_path.parent)
            
            # Move file atomically
            src_path.replace(dest_path)
            
            # Update inventory
            old_path = str(src_path)
            new_path = str(dest_path)
            
            if old_path in self.inventory:
                record = self.inventory[old_path]
                record['path'] = new_path
                del self.inventory[old_path]
                self.inventory[new_path] = record
            
            return True
        
        except Exception as e:
            console.print(f"[red]Error moving {src_path} to {dest_path}: {e}[/red]")
            return False
    
    def sort_file(self, file_path: Path) -> Tuple[bool, Optional[Path], str]:
        """Sort a single file according to rules."""
        if not file_path.exists() or not file_path.is_file():
            return False, None, "File does not exist"
        
        # Get file context
        inventory_record = self.inventory.get(str(file_path))
        context = self._get_file_context(file_path, inventory_record)
        
        # Try each rule in order
        for rule in self.config.sorting.rules:
            dest_path = self._apply_rule(file_path, rule, context)
            if dest_path:
                # Check if destination is different from source
                if dest_path.resolve() != file_path.resolve():
                    success = self._move_file(file_path, dest_path)
                    if success:
                        return True, dest_path, f"Matched rule: {rule.if_}"
                    else:
                        return False, None, f"Failed to move file"
                else:
                    return True, file_path, "Already in correct location"
        
        # No rule matched
        return False, None, "No matching rule found"
    
    def sort_all(self) -> Dict[str, Any]:
        """Sort all files in the root directory."""
        console.print("[bold blue]Sorting files according to rules...[/bold blue]")
        
        stats = {
            'files_processed': 0,
            'files_moved': 0,
            'files_already_sorted': 0,
            'files_failed': 0,
            'files_unsorted': 0,
            'by_rule': {},
            'errors': []
        }
        
        # Find all files to sort
        files_to_sort = []
        for file_path in self.root_dir.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in {'.json', '.ndjson', '.csv', '.xlsx', '.xml', '.zip'}:
                files_to_sort.append(file_path)
        
        console.print(f"Found {len(files_to_sort)} files to sort")
        
        for file_path in files_to_sort:
            stats['files_processed'] += 1
            
            success, dest_path, message = self.sort_file(file_path)
            
            if success:
                if dest_path == file_path:
                    stats['files_already_sorted'] += 1
                else:
                    stats['files_moved'] += 1
                    console.print(f"[green]✓ Moved {file_path.name} to {dest_path.parent.name}/[/green]")
            else:
                if "No matching rule" in message:
                    stats['files_unsorted'] += 1
                    console.print(f"[yellow]⚠ No rule for {file_path.name}[/yellow]")
                else:
                    stats['files_failed'] += 1
                    stats['errors'].append({
                        'file': str(file_path),
                        'error': message
                    })
                    console.print(f"[red]✗ Failed to sort {file_path.name}: {message}[/red]")
        
        # Save updated inventory
        save_jsonl(self.inventory_file, list(self.inventory.values()))
        
        # Display results
        self._display_sort_stats(stats)
        
        return stats
    
    def _display_sort_stats(self, stats: Dict[str, Any]) -> None:
        """Display sorting statistics."""
        console.print(f"\n[bold green]Sorting completed![/bold green]")
        
        # Summary table
        table = Table(title="Sorting Summary")
        table.add_column("Category", style="cyan")
        table.add_column("Count", style="magenta")
        table.add_column("Percentage", style="green")
        
        total = stats['files_processed']
        
        categories = [
            ('Files Moved', stats['files_moved']),
            ('Already Sorted', stats['files_already_sorted']),
            ('Unsorted', stats['files_unsorted']),
            ('Failed', stats['files_failed'])
        ]
        
        for category, count in categories:
            percentage = (count / total * 100) if total > 0 else 0
            table.add_row(category, str(count), f"{percentage:.1f}%")
        
        console.print(table)
        
        # Errors
        if stats['errors']:
            console.print(f"\n[bold red]Errors ({len(stats['errors'])}):[/bold red]")
            for error in stats['errors'][:10]:  # Show first 10 errors
                console.print(f"  • {error['file']}: {error['error']}")
            
            if len(stats['errors']) > 10:
                console.print(f"  ... and {len(stats['errors']) - 10} more errors")
        
        # Unsorted files
        if stats['files_unsorted'] > 0:
            console.print(f"\n[bold yellow]Note: {stats['files_unsorted']} files were not sorted.[/bold yellow]")
            console.print("Consider adding rules for these files or they will be moved to the default location.")
    
    def get_unsorted_files(self) -> List[Path]:
        """Get list of files that don't match any sorting rule."""
        unsorted = []
        
        for file_path in self.root_dir.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in {'.json', '.ndjson', '.csv', '.xlsx', '.xml', '.zip'}:
                inventory_record = self.inventory.get(str(file_path))
                context = self._get_file_context(file_path, inventory_record)
                
                # Check if any rule matches
                matched = False
                for rule in self.config.sorting.rules:
                    if self._evaluate_condition(rule.if_, context):
                        matched = True
                        break
                
                if not matched:
                    unsorted.append(file_path)
        
        return unsorted
    
    def preview_sort(self, file_path: Path) -> Optional[Path]:
        """Preview where a file would be moved without actually moving it."""
        if not file_path.exists() or not file_path.is_file():
            return None
        
        # Get file context
        inventory_record = self.inventory.get(str(file_path))
        context = self._get_file_context(file_path, inventory_record)
        
        # Try each rule in order
        for rule in self.config.sorting.rules:
            dest_path = self._apply_rule(file_path, rule, context)
            if dest_path:
                return dest_path
        
        return None
    
    def add_sorting_rule(self, condition: str, destination: str) -> None:
        """Add a new sorting rule to the configuration."""
        new_rule = SortingRule(if_=condition, move_to=destination)
        self.config.sorting.rules.append(new_rule)
        
        # Save updated configuration
        from .config import save_config
        save_config(self.config)
        
        console.print(f"[green]Added sorting rule: {condition} → {destination}[/green]")


def sort_all(config: Config) -> Dict[str, Any]:
    """Main function to sort all files according to rules."""
    sorter = FileSorter(config)
    return sorter.sort_all()


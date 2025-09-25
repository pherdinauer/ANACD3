"""Download planning based on catalog vs inventory diff."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.table import Table

from .config import Config
from .utils import (
    load_jsonl, save_jsonl, get_timestamp, safe_filename,
    extract_filename_from_url, ensure_directory
)

console = Console()


class PlanItem:
    """Download plan item."""
    
    def __init__(
        self,
        dataset_slug: str,
        resource_url: str,
        dest_path: str,
        reason: str,
        size: Optional[int] = None,
        etag: Optional[str] = None,
        resource_name: Optional[str] = None
    ):
        self.dataset_slug = dataset_slug
        self.resource_url = resource_url
        self.dest_path = dest_path
        self.reason = reason
        self.size = size
        self.etag = etag
        self.resource_name = resource_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_slug': self.dataset_slug,
            'resource_url': self.resource_url,
            'dest_path': self.dest_path,
            'reason': self.reason,
            'size': self.size,
            'etag': self.etag,
            'resource_name': self.resource_name
        }


class DownloadPlanner:
    """Download planner that compares catalog with local inventory."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.root_dir = Path(config.root_dir)
        
        # File paths
        self.datasets_file = self.state_dir / 'catalog' / 'datasets.jsonl'
        self.resources_file = self.state_dir / 'catalog' / 'resources.jsonl'
        self.inventory_file = self.state_dir / 'local' / 'inventory.jsonl'
        self.plans_dir = self.state_dir / 'plans'
        
        # Ensure plans directory exists
        ensure_directory(self.plans_dir)
    
    def load_catalog(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Load catalog data (datasets and resources)."""
        datasets = {r['slug']: r for r in load_jsonl(self.datasets_file)}
        resources = {(r['dataset_slug'], r['url']): r for r in load_jsonl(self.resources_file)}
        return datasets, resources
    
    def load_inventory(self) -> Dict[str, Dict[str, Any]]:
        """Load local inventory."""
        return {r['path']: r for r in load_jsonl(self.inventory_file)}
    
    def generate_dest_path(self, resource: Dict[str, Any], dataset: Dict[str, Any]) -> str:
        """Generate destination path for a resource."""
        # Extract filename from URL or use resource name
        filename = resource.get('name', '')
        if not filename:
            filename = extract_filename_from_url(resource['url'])
        
        # Make filename safe
        filename = safe_filename(filename)
        
        # Create dataset directory
        dataset_dir = self.root_dir / safe_filename(resource['dataset_slug'])
        ensure_directory(dataset_dir)
        
        return str(dataset_dir / filename)
    
    def find_matching_local_file(self, resource: Dict[str, Any], inventory: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find matching local file for a resource."""
        # Strategy 1: Match by URL (from sidecar metadata)
        for local_file in inventory.values():
            if local_file.get('url') == resource['url']:
                return local_file
        
        # Strategy 2: Match by dataset slug and filename
        resource_name = resource.get('name', '')
        if resource_name:
            for local_file in inventory.values():
                if (local_file.get('dataset_slug') == resource['dataset_slug'] and
                    resource_name in local_file['path']):
                    return local_file
        
        # Strategy 3: Match by filename pattern
        filename = extract_filename_from_url(resource['url'])
        if filename:
            for local_file in inventory.values():
                if filename in local_file['path']:
                    return local_file
        
        return None
    
    def should_download(self, resource: Dict[str, Any], local_file: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
        """Determine if resource should be downloaded and why."""
        if not local_file:
            return True, "missing"
        
        # Check if file size changed
        if resource.get('content_length') and local_file.get('size'):
            if resource['content_length'] != local_file['size']:
                return True, "size_changed"
        
        # Check if ETag changed
        if resource.get('etag') and local_file.get('sha256'):
            # For now, we can't directly compare ETag with SHA256
            # This would require storing ETag in sidecar metadata
            pass
        
        # Check if file is corrupted (size 0 or very small for non-empty resource)
        if local_file.get('size', 0) == 0 and resource.get('content_length', 0) > 0:
            return True, "corrupted"
        
        # File exists and seems up to date
        return False, "up_to_date"
    
    def make_plan(
        self, 
        only_missing: bool = True, 
        filter_slug: Optional[str] = None
    ) -> List[PlanItem]:
        """Generate download plan."""
        console.print("[bold blue]Generating download plan...[/bold blue]")
        
        # Load data
        datasets, resources = self.load_catalog()
        inventory = self.load_inventory()
        
        if not resources:
            console.print("[yellow]No resources found in catalog. Run 'crawl' first.[/yellow]")
            return []
        
        if not datasets:
            console.print("[yellow]No datasets found in catalog. Run 'crawl' first.[/yellow]")
            return []
        
        plan_items = []
        stats = {
            'total_resources': len(resources),
            'missing': 0,
            'size_changed': 0,
            'etag_changed': 0,
            'corrupted': 0,
            'up_to_date': 0,
            'filtered_out': 0
        }
        
        for (dataset_slug, resource_url), resource in resources.items():
            # Apply filter
            if filter_slug and filter_slug not in dataset_slug:
                stats['filtered_out'] += 1
                continue
            
            # Get dataset info
            dataset = datasets.get(dataset_slug)
            if not dataset:
                continue
            
            # Find matching local file
            local_file = self.find_matching_local_file(resource, inventory)
            
            # Determine if download is needed
            should_download, reason = self.should_download(resource, local_file)
            
            if not should_download:
                stats['up_to_date'] += 1
                continue
            
            # Skip if only_missing and reason is not missing
            if only_missing and reason != "missing":
                continue
            
            # Generate destination path
            dest_path = self.generate_dest_path(resource, dataset)
            
            # Create plan item
            plan_item = PlanItem(
                dataset_slug=dataset_slug,
                resource_url=resource_url,
                dest_path=dest_path,
                reason=reason,
                size=resource.get('content_length'),
                etag=resource.get('etag'),
                resource_name=resource.get('name')
            )
            
            plan_items.append(plan_item)
            stats[reason] += 1
        
        # Save plan to file
        if plan_items:
            plan_filename = f"plan-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"
            plan_file = self.plans_dir / plan_filename
            save_jsonl(plan_file, [item.to_dict() for item in plan_items])
            console.print(f"[green]Plan saved to {plan_file}[/green]")
        
        # Display statistics
        self._display_plan_stats(stats, plan_items)
        
        return plan_items
    
    def _display_plan_stats(self, stats: Dict[str, Any], plan_items: List[PlanItem]) -> None:
        """Display planning statistics."""
        table = Table(title="Download Plan Statistics")
        table.add_column("Category", style="cyan")
        table.add_column("Count", style="magenta")
        table.add_column("Percentage", style="green")
        
        total = stats['total_resources']
        
        for category, count in stats.items():
            if category == 'total_resources':
                continue
            
            percentage = (count / total * 100) if total > 0 else 0
            table.add_row(
                category.replace('_', ' ').title(),
                str(count),
                f"{percentage:.1f}%"
            )
        
        console.print(table)
        
        if plan_items:
            console.print(f"\n[bold green]Plan contains {len(plan_items)} items to download[/bold green]")
            
            # Show breakdown by reason
            reason_counts = {}
            for item in plan_items:
                reason_counts[item.reason] = reason_counts.get(item.reason, 0) + 1
            
            console.print("\nBreakdown by reason:")
            for reason, count in reason_counts.items():
                console.print(f"  {reason.replace('_', ' ').title()}: {count}")
        else:
            console.print("\n[bold yellow]No items to download[/bold yellow]")
    
    def load_latest_plan(self) -> List[PlanItem]:
        """Load the most recent plan file."""
        if not self.plans_dir.exists():
            return []
        
        plan_files = list(self.plans_dir.glob("plan-*.jsonl"))
        if not plan_files:
            return []
        
        # Sort by modification time, get latest
        latest_plan = max(plan_files, key=lambda p: p.stat().st_mtime)
        
        plan_data = load_jsonl(latest_plan)
        return [PlanItem(**item) for item in plan_data]
    
    def get_plan_summary(self, plan_items: List[PlanItem]) -> Dict[str, Any]:
        """Get summary of plan items."""
        if not plan_items:
            return {
                'total_items': 0,
                'total_size': 0,
                'by_reason': {},
                'by_dataset': {}
            }
        
        total_size = sum(item.size or 0 for item in plan_items)
        
        by_reason = {}
        by_dataset = {}
        
        for item in plan_items:
            # Count by reason
            by_reason[item.reason] = by_reason.get(item.reason, 0) + 1
            
            # Count by dataset
            by_dataset[item.dataset_slug] = by_dataset.get(item.dataset_slug, 0) + 1
        
        return {
            'total_items': len(plan_items),
            'total_size': total_size,
            'by_reason': by_reason,
            'by_dataset': by_dataset
        }
    
    def filter_plan_by_dataset(self, plan_items: List[PlanItem], dataset_slug: str) -> List[PlanItem]:
        """Filter plan items by dataset slug."""
        return [item for item in plan_items if item.dataset_slug == dataset_slug]
    
    def filter_plan_by_reason(self, plan_items: List[PlanItem], reason: str) -> List[PlanItem]:
        """Filter plan items by reason."""
        return [item for item in plan_items if item.reason == reason]


def make_plan(
    config: Config, 
    only_missing: bool = True, 
    filter_slug: Optional[str] = None
) -> List[PlanItem]:
    """Main function to generate download plan."""
    planner = DownloadPlanner(config)
    return planner.make_plan(only_missing, filter_slug)


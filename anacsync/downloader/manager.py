"""Download manager with strategy orchestration."""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table

from ..config import Config
from ..planner import PlanItem
from ..utils import (
    append_jsonl, get_timestamp, format_bytes, format_duration,
    ensure_directory
)
from .strategies import (
    S1DynamicStrategy, S2SparseStrategy, S3CurlStrategy,
    S4ShortConnStrategy, S5TailFirstStrategy, DownloadResult
)

console = Console()


class DownloadManager:
    """Download manager that orchestrates multiple strategies."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.history_file = self.state_dir / 'downloads' / 'history.jsonl'
        
        # Initialize strategies
        self.strategies = {
            's1_dynamic': S1DynamicStrategy(config),
            's2_sparse': S2SparseStrategy(config),
            's3_curl': S3CurlStrategy(config),
            's4_shortconn': S4ShortConnStrategy(config),
            's5_tailfirst': S5TailFirstStrategy(config)
        }
        
        # Ensure downloads directory exists
        ensure_directory(self.history_file.parent)
    
    def _log_download_attempt(self, result: DownloadResult, url: str, dest_path: str) -> None:
        """Log download attempt to history."""
        attempt = {
            'resource_url': url,
            'strategy': result.strategy,
            'start': get_timestamp(),
            'end': get_timestamp(),
            'bytes': result.bytes_written,
            'ok': result.ok,
            'error': result.error,
            'dest_path': dest_path,
            'duration': result.duration
        }
        append_jsonl(self.history_file, attempt)
    
    def _download_with_strategy(
        self, 
        strategy_name: str, 
        plan_item: PlanItem, 
        meta: Dict[str, Any]
    ) -> DownloadResult:
        """Download using specific strategy."""
        strategy = self.strategies.get(strategy_name)
        if not strategy:
            return DownloadResult(
                ok=False, bytes_written=0, strategy=strategy_name,
                error=f"Unknown strategy: {strategy_name}"
            )
        
        dest_path = Path(plan_item.dest_path)
        ensure_directory(dest_path.parent)
        
        # Prepare metadata
        meta.update({
            'url': plan_item.resource_url,
            'dataset_slug': plan_item.dataset_slug,
            'resource_name': plan_item.resource_name,
            'downloaded_at': get_timestamp(),
            'retries': meta.get('retries', 0) + 1
        })
        
        # Attempt download
        result = strategy.fetch(plan_item.resource_url, dest_path, meta, self.config)
        
        # Log attempt
        self._log_download_attempt(result, plan_item.resource_url, plan_item.dest_path)
        
        return result
    
    def _should_switch_strategy(
        self, 
        current_strategy: str, 
        attempts: List[DownloadResult],
        start_time: float
    ) -> bool:
        """Determine if we should switch to next strategy."""
        if not attempts:
            return False
        
        # Check if we've exceeded retry limit for current strategy
        current_attempts = [a for a in attempts if a.strategy == current_strategy]
        if len(current_attempts) >= self.config.downloader.retries_per_strategy:
            return True
        
        # Check if we've been stuck without progress
        if time.time() - start_time > self.config.downloader.switch_after_seconds_without_progress:
            return True
        
        # Check if last attempt was successful
        if attempts and attempts[-1].ok:
            return False
        
        return False
    
    def download_single(self, plan_item: PlanItem) -> DownloadResult:
        """Download a single plan item using strategy cascade."""
        console.print(f"[blue]Downloading {plan_item.resource_name or 'file'}...[/blue]")
        
        # Prepare metadata
        meta = {
            'url': plan_item.resource_url,
            'dataset_slug': plan_item.dataset_slug,
            'resource_name': plan_item.resource_name,
            'etag': plan_item.etag,
            'content_length': plan_item.size
        }
        
        # Try strategies in order
        strategy_order = self.config.downloader.strategies
        attempts = []
        start_time = time.time()
        
        for strategy_name in strategy_order:
            if strategy_name not in self.strategies:
                console.print(f"[yellow]Warning: Strategy {strategy_name} not available[/yellow]")
                continue
            
            console.print(f"[cyan]Trying strategy: {strategy_name}[/cyan]")
            
            # Attempt download
            result = self._download_with_strategy(strategy_name, plan_item, meta.copy())
            attempts.append(result)
            
            if result.ok:
                console.print(f"[green]✓ Downloaded successfully using {strategy_name}[/green]")
                console.print(f"  Size: {format_bytes(result.bytes_written)}")
                console.print(f"  Duration: {format_duration(result.duration)}")
                return result
            
            console.print(f"[red]✗ Strategy {strategy_name} failed: {result.error}[/red]")
            
            # Check if we should switch strategies
            if self._should_switch_strategy(strategy_name, attempts, start_time):
                console.print(f"[yellow]Switching to next strategy...[/yellow]")
                continue
        
        # All strategies failed
        console.print(f"[red]✗ All strategies failed for {plan_item.resource_name}[/red]")
        
        # Return the last attempt
        return attempts[-1] if attempts else DownloadResult(
            ok=False, bytes_written=0, strategy="none",
            error="No strategies attempted"
        )
    
    def run_plan(self, plan_items: List[PlanItem]) -> Dict[str, Any]:
        """Execute download plan."""
        if not plan_items:
            console.print("[yellow]No items to download[/yellow]")
            return {
                'total_items': 0,
                'successful': 0,
                'failed': 0,
                'total_bytes': 0,
                'total_duration': 0.0
            }
        
        console.print(f"[bold blue]Starting download of {len(plan_items)} items...[/bold blue]")
        
        stats = {
            'total_items': len(plan_items),
            'successful': 0,
            'failed': 0,
            'total_bytes': 0,
            'total_duration': 0.0,
            'by_strategy': {},
            'errors': []
        }
        
        start_time = time.time()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Downloading files...", total=len(plan_items))
            
            for i, plan_item in enumerate(plan_items):
                progress.update(task, description=f"Downloading {plan_item.resource_name or 'file'}...")
                
                result = self.download_single(plan_item)
                
                # Update stats
                if result.ok:
                    stats['successful'] += 1
                    stats['total_bytes'] += result.bytes_written
                else:
                    stats['failed'] += 1
                    stats['errors'].append({
                        'resource': plan_item.resource_name,
                        'error': result.error
                    })
                
                # Track strategy usage
                strategy = result.strategy
                if strategy not in stats['by_strategy']:
                    stats['by_strategy'][strategy] = {'successful': 0, 'failed': 0}
                
                if result.ok:
                    stats['by_strategy'][strategy]['successful'] += 1
                else:
                    stats['by_strategy'][strategy]['failed'] += 1
                
                progress.advance(task)
        
        stats['total_duration'] = time.time() - start_time
        
        # Display results
        self._display_download_stats(stats)
        
        return stats
    
    def _display_download_stats(self, stats: Dict[str, Any]) -> None:
        """Display download statistics."""
        console.print(f"\n[bold green]Download completed![/bold green]")
        
        # Summary table
        table = Table(title="Download Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Total Items", str(stats['total_items']))
        table.add_row("Successful", str(stats['successful']))
        table.add_row("Failed", str(stats['failed']))
        table.add_row("Total Size", format_bytes(stats['total_bytes']))
        table.add_row("Duration", format_duration(stats['total_duration']))
        
        if stats['total_duration'] > 0:
            avg_speed = stats['total_bytes'] / stats['total_duration']
            table.add_row("Average Speed", f"{format_bytes(avg_speed)}/s")
        
        console.print(table)
        
        # Strategy breakdown
        if stats['by_strategy']:
            console.print("\n[bold]Strategy Usage:[/bold]")
            strategy_table = Table()
            strategy_table.add_column("Strategy", style="cyan")
            strategy_table.add_column("Successful", style="green")
            strategy_table.add_column("Failed", style="red")
            strategy_table.add_column("Success Rate", style="yellow")
            
            for strategy, counts in stats['by_strategy'].items():
                total = counts['successful'] + counts['failed']
                success_rate = (counts['successful'] / total * 100) if total > 0 else 0
                
                strategy_table.add_row(
                    strategy,
                    str(counts['successful']),
                    str(counts['failed']),
                    f"{success_rate:.1f}%"
                )
            
            console.print(strategy_table)
        
        # Errors
        if stats['errors']:
            console.print(f"\n[bold red]Errors ({len(stats['errors'])}):[/bold red]")
            for error in stats['errors'][:10]:  # Show first 10 errors
                console.print(f"  • {error['resource']}: {error['error']}")
            
            if len(stats['errors']) > 10:
                console.print(f"  ... and {len(stats['errors']) - 10} more errors")
    
    def get_download_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent download history."""
        if not self.history_file.exists():
            return []
        
        # Read last N lines
        with open(self.history_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        history = []
        for line in lines[-limit:]:
            try:
                import json
                history.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                continue
        
        return history
    
    def retry_failed_downloads(self, plan_items: List[PlanItem]) -> Dict[str, Any]:
        """Retry failed downloads from plan."""
        # Filter to only failed items
        failed_items = []
        for item in plan_items:
            dest_path = Path(item.dest_path)
            if not dest_path.exists() or dest_path.stat().st_size == 0:
                failed_items.append(item)
        
        if not failed_items:
            console.print("[green]No failed downloads to retry[/green]")
            return {'total_items': 0, 'successful': 0, 'failed': 0}
        
        console.print(f"[yellow]Retrying {len(failed_items)} failed downloads...[/yellow]")
        return self.run_plan(failed_items)


def run_plan(config: Config, plan_items: List[PlanItem]) -> Dict[str, Any]:
    """Main function to execute download plan."""
    manager = DownloadManager(config)
    return manager.run_plan(plan_items)


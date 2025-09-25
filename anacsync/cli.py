"""Interactive CLI for ANAC Sync."""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

from .config import Config, load_config, save_config, get_default_config
from .crawler import crawl_all
from .inventory import scan_local
from .planner import make_plan, DownloadPlanner
from .downloader import run_plan
from .sorter import sort_all, FileSorter
from .utils import format_bytes, get_timestamp

console = Console()
app = typer.Typer(help="ANAC Sync - Professional ANAC dataset crawler and downloader")


def show_banner():
    """Show application banner."""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                        ANAC SYNC                             ║
    ║              Professional Dataset Crawler                    ║
    ║                                                              ║
    ║  🕷️  Smart Crawling    📥  Multi-Strategy Downloads         ║
    ║  📊  Local Inventory   🗂️  Intelligent Sorting              ║
    ║  🔄  Auto Sync         📈  Rich Reporting                   ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    console.print(Panel(banner, style="bold blue"))


def show_main_menu():
    """Show main interactive menu."""
    console.print("\n[bold cyan]Main Menu[/bold cyan]")
    
    menu_items = [
        ("1", "🕷️  Crawl ANAC datasets", "Discover and catalog all available datasets"),
        ("2", "📊  Scan local files", "Update local file inventory"),
        ("3", "📋  Generate download plan", "Create plan for missing/updated files"),
        ("4", "📥  Download files", "Execute download plan with multiple strategies"),
        ("5", "🗂️  Sort files", "Organize files according to rules"),
        ("6", "📈  Show report", "Display sync status and statistics"),
        ("7", "🔧  Configuration", "Manage settings and rules"),
        ("8", "❓  Help", "Show help and documentation"),
        ("0", "🚪  Exit", "Exit the application")
    ]
    
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Option", style="bold yellow", width=4)
    table.add_column("Action", style="cyan", width=30)
    table.add_column("Description", style="dim", width=50)
    
    for option, action, description in menu_items:
        table.add_row(option, action, description)
    
    console.print(table)


def get_user_choice() -> str:
    """Get user choice from main menu."""
    while True:
        choice = Prompt.ask("\n[bold]Select an option", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"])
        return choice


def handle_crawl(config: Config):
    """Handle crawl operation."""
    console.print("\n[bold blue]🕷️  Crawling ANAC Datasets[/bold blue]")
    
    if not Confirm.ask("This will crawl all ANAC datasets. Continue?"):
        return
    
    try:
        stats = crawl_all(config)
        console.print(f"\n[green]✓ Crawl completed successfully![/green]")
        console.print(f"Found {stats['datasets_found']} datasets and {stats['resources_found']} resources")
    except Exception as e:
        console.print(f"\n[red]✗ Crawl failed: {e}[/red]")


def handle_scan(config: Config):
    """Handle scan operation."""
    console.print("\n[bold blue]📊  Scanning Local Files[/bold blue]")
    
    if not Confirm.ask("This will scan local files and update inventory. Continue?"):
        return
    
    try:
        stats = scan_local(config)
        console.print(f"\n[green]✓ Scan completed successfully![/green]")
        console.print(f"Processed {stats['files_scanned']} files")
    except Exception as e:
        console.print(f"\n[red]✗ Scan failed: {e}[/red]")


def handle_plan(config: Config):
    """Handle plan generation."""
    console.print("\n[bold blue]📋  Generate Download Plan[/bold blue]")
    
    # Ask for options
    only_missing = Confirm.ask("Only plan missing files?", default=True)
    
    filter_slug = None
    if Confirm.ask("Filter by dataset slug?"):
        filter_slug = Prompt.ask("Enter dataset slug pattern")
    
    try:
        plan_items = make_plan(config, only_missing, filter_slug)
        
        if plan_items:
            console.print(f"\n[green]✓ Plan generated with {len(plan_items)} items[/green]")
            
            # Show plan summary
            total_size = sum(item.size or 0 for item in plan_items)
            console.print(f"Total size to download: {format_bytes(total_size)}")
            
            # Show breakdown by reason
            by_reason = {}
            for item in plan_items:
                by_reason[item.reason] = by_reason.get(item.reason, 0) + 1
            
            console.print("\nBreakdown by reason:")
            for reason, count in by_reason.items():
                console.print(f"  {reason.replace('_', ' ').title()}: {count}")
        else:
            console.print("\n[yellow]No items to download[/yellow]")
    
    except Exception as e:
        console.print(f"\n[red]✗ Plan generation failed: {e}[/red]")


def handle_download(config: Config):
    """Handle download operation."""
    console.print("\n[bold blue]📥  Download Files[/bold blue]")
    
    # Load latest plan
    planner = DownloadPlanner(config)
    plan_items = planner.load_latest_plan()
    
    if not plan_items:
        console.print("[yellow]No download plan found. Generate a plan first.[/yellow]")
        return
    
    console.print(f"Found plan with {len(plan_items)} items")
    
    # Show plan summary
    total_size = sum(item.size or 0 for item in plan_items)
    console.print(f"Total size: {format_bytes(total_size)}")
    
    if not Confirm.ask("Continue with download?"):
        return
    
    try:
        stats = run_plan(config, plan_items)
        console.print(f"\n[green]✓ Download completed![/green]")
        console.print(f"Successfully downloaded {stats['successful']} files")
        if stats['failed'] > 0:
            console.print(f"Failed: {stats['failed']} files")
    except Exception as e:
        console.print(f"\n[red]✗ Download failed: {e}[/red]")


def handle_sort(config: Config):
    """Handle sort operation."""
    console.print("\n[bold blue]🗂️  Sort Files[/bold blue]")
    
    if not Confirm.ask("This will organize files according to sorting rules. Continue?"):
        return
    
    try:
        stats = sort_all(config)
        console.print(f"\n[green]✓ Sorting completed![/green]")
        console.print(f"Moved {stats['files_moved']} files")
        if stats['files_unsorted'] > 0:
            console.print(f"Unsorted: {stats['files_unsorted']} files")
    except Exception as e:
        console.print(f"\n[red]✗ Sorting failed: {e}[/red]")


def handle_report(config: Config):
    """Handle report display."""
    console.print("\n[bold blue]📈  Sync Report[/bold blue]")
    
    try:
        # Load data
        state_dir = Path(config.state_dir)
        datasets_file = state_dir / 'catalog' / 'datasets.jsonl'
        resources_file = state_dir / 'catalog' / 'resources.jsonl'
        inventory_file = state_dir / 'local' / 'inventory.jsonl'
        
        # Count datasets
        datasets_count = 0
        if datasets_file.exists():
            with open(datasets_file, 'r') as f:
                datasets_count = sum(1 for _ in f)
        
        # Count resources
        resources_count = 0
        if resources_file.exists():
            with open(resources_file, 'r') as f:
                resources_count = sum(1 for _ in f)
        
        # Count local files
        local_files_count = 0
        if inventory_file.exists():
            with open(inventory_file, 'r') as f:
                local_files_count = sum(1 for _ in f)
        
        # Show report
        table = Table(title="Sync Status Report")
        table.add_column("Category", style="cyan")
        table.add_column("Count", style="magenta")
        table.add_column("Last Updated", style="green")
        
        table.add_row("Datasets in Catalog", str(datasets_count), "N/A")
        table.add_row("Resources in Catalog", str(resources_count), "N/A")
        table.add_row("Local Files", str(local_files_count), "N/A")
        
        console.print(table)
        
        # Show configuration
        console.print(f"\n[bold]Configuration:[/bold]")
        console.print(f"  Root Directory: {config.root_dir}")
        console.print(f"  State Directory: {config.state_dir}")
        console.print(f"  Rate Limit: {config.downloader.rate_limit_rps} req/s")
        console.print(f"  Download Strategies: {', '.join(config.downloader.strategies)}")
        
    except Exception as e:
        console.print(f"\n[red]✗ Report generation failed: {e}[/red]")


def handle_config(config: Config):
    """Handle configuration management."""
    console.print("\n[bold blue]🔧  Configuration[/bold blue]")
    
    config_menu = [
        ("1", "View current configuration"),
        ("2", "Edit download settings"),
        ("3", "Edit sorting rules"),
        ("4", "Reset to defaults"),
        ("0", "Back to main menu")
    ]
    
    while True:
        console.print("\n[bold cyan]Configuration Menu[/bold cyan]")
        for option, description in config_menu:
            console.print(f"  {option}. {description}")
        
        choice = Prompt.ask("Select option", choices=["0", "1", "2", "3", "4"])
        
        if choice == "0":
            break
        elif choice == "1":
            show_config(config)
        elif choice == "2":
            edit_download_config(config)
        elif choice == "3":
            edit_sorting_rules(config)
        elif choice == "4":
            if Confirm.ask("Reset configuration to defaults?"):
                config = get_default_config()
                save_config(config)
                console.print("[green]✓ Configuration reset to defaults[/green]")


def show_config(config: Config):
    """Show current configuration."""
    console.print("\n[bold]Current Configuration:[/bold]")
    
    # Basic settings
    console.print(f"  Root Directory: {config.root_dir}")
    console.print(f"  State Directory: {config.state_dir}")
    console.print(f"  Base URL: {config.base_url}")
    
    # Crawler settings
    console.print(f"\n  Crawler:")
    console.print(f"    Delay: {config.crawler.delay_ms_min}-{config.crawler.delay_ms_max}ms")
    console.print(f"    Max Concurrency: {config.crawler.max_concurrency}")
    
    # Downloader settings
    console.print(f"\n  Downloader:")
    console.print(f"    Rate Limit: {config.downloader.rate_limit_rps} req/s")
    console.print(f"    Strategies: {', '.join(config.downloader.strategies)}")
    console.print(f"    Retries per Strategy: {config.downloader.retries_per_strategy}")
    
    # Sorting rules
    console.print(f"\n  Sorting Rules ({len(config.sorting.rules)}):")
    for i, rule in enumerate(config.sorting.rules, 1):
        console.print(f"    {i}. {rule.if_} → {rule.move_to}")


def edit_download_config(config: Config):
    """Edit download configuration."""
    console.print("\n[bold]Download Configuration:[/bold]")
    
    # Rate limit
    new_rate = IntPrompt.ask("Rate limit (requests per second)", default=int(config.downloader.rate_limit_rps))
    config.downloader.rate_limit_rps = float(new_rate)
    
    # Retries
    new_retries = IntPrompt.ask("Retries per strategy", default=config.downloader.retries_per_strategy)
    config.downloader.retries_per_strategy = new_retries
    
    # Enable curl
    config.downloader.enable_curl = Confirm.ask("Enable curl strategy?", default=config.downloader.enable_curl)
    
    save_config(config)
    console.print("[green]✓ Download configuration updated[/green]")


def edit_sorting_rules(config: Config):
    """Edit sorting rules."""
    console.print("\n[bold]Sorting Rules:[/bold]")
    
    while True:
        console.print("\nCurrent rules:")
        for i, rule in enumerate(config.sorting.rules, 1):
            console.print(f"  {i}. {rule.if_} → {rule.move_to}")
        
        console.print("\nOptions:")
        console.print("  1. Add new rule")
        console.print("  2. Remove rule")
        console.print("  0. Back")
        
        choice = Prompt.ask("Select option", choices=["0", "1", "2"])
        
        if choice == "0":
            break
        elif choice == "1":
            condition = Prompt.ask("Enter condition (e.g., 'slug contains \"appalti\"')")
            destination = Prompt.ask("Enter destination path")
            config.sorting.rules.append(config.sorting.rules[0].__class__(if_=condition, move_to=destination))
            save_config(config)
            console.print("[green]✓ Rule added[/green]")
        elif choice == "2":
            if config.sorting.rules:
                rule_num = IntPrompt.ask("Enter rule number to remove", default=1)
                if 1 <= rule_num <= len(config.sorting.rules):
                    del config.sorting.rules[rule_num - 1]
                    save_config(config)
                    console.print("[green]✓ Rule removed[/green]")


def handle_help():
    """Show help and documentation."""
    help_text = """
    [bold]ANAC Sync - Help[/bold]
    
    [bold cyan]Overview:[/bold cyan]
    ANAC Sync is a professional tool for downloading and organizing ANAC datasets.
    
    [bold cyan]Main Operations:[/bold cyan]
    
    [bold]1. Crawl[/bold] - Discovers all available datasets and resources from ANAC
    [bold]2. Scan[/bold] - Updates local file inventory and checks integrity
    [bold]3. Plan[/bold] - Generates download plan for missing or updated files
    [bold]4. Download[/bold] - Downloads files using multiple strategies
    [bold]5. Sort[/bold] - Organizes files according to configurable rules
    [bold]6. Report[/bold] - Shows sync status and statistics
    
    [bold cyan]Download Strategies:[/bold cyan]
    • S1 Dynamic: Adaptive chunk sizes with resume support
    • S2 Sparse: Non-linear segment downloading
    • S3 Curl: External tool for maximum compatibility
    • S4 Short: Small chunks with connection close
    • S5 Tail-First: Downloads end first for validation
    
    [bold cyan]Configuration:[/bold cyan]
    All settings are stored in ~/.anacsync/anacsync.yaml
    You can edit settings through the Configuration menu.
    
    [bold cyan]File Organization:[/bold cyan]
    Files are automatically sorted based on configurable rules.
    Rules can match on dataset slug, filename, format, etc.
    """
    
    console.print(Panel(help_text, title="Help", border_style="blue"))


def interactive_mode():
    """Run interactive mode."""
    show_banner()
    
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        console.print(f"[red]Failed to load configuration: {e}[/red]")
        console.print("Using default configuration...")
        config = get_default_config()
        save_config(config)
    
    while True:
        try:
            show_main_menu()
            choice = get_user_choice()
            
            if choice == "0":
                console.print("\n[bold green]Goodbye! 👋[/bold green]")
                break
            elif choice == "1":
                handle_crawl(config)
            elif choice == "2":
                handle_scan(config)
            elif choice == "3":
                handle_plan(config)
            elif choice == "4":
                handle_download(config)
            elif choice == "5":
                handle_sort(config)
            elif choice == "6":
                handle_report(config)
            elif choice == "7":
                handle_config(config)
            elif choice == "8":
                handle_help()
            
            # Pause before returning to menu
            if choice != "0":
                Prompt.ask("\nPress Enter to continue...")
        
        except KeyboardInterrupt:
            console.print("\n\n[bold yellow]Interrupted by user[/bold yellow]")
            break
        except Exception as e:
            console.print(f"\n[red]Unexpected error: {e}[/red]")
            if Confirm.ask("Continue?"):
                continue
            else:
                break


# Command line interface
@app.command()
def main():
    """ANAC Sync - Interactive mode."""
    interactive_mode()


@app.command()
def crawl(
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Crawl ANAC datasets."""
    config = load_config(config_path)
    crawl_all(config)


@app.command()
def scan(
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Scan local files."""
    config = load_config(config_path)
    scan_local(config)


@app.command()
def plan(
    only_missing: bool = typer.Option(True, "--only-missing/--all", help="Only plan missing files"),
    filter_slug: Optional[str] = typer.Option(None, "--slug", help="Filter by dataset slug"),
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Generate download plan."""
    config = load_config(config_path)
    make_plan(config, only_missing, filter_slug)


@app.command()
def download(
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Execute download plan."""
    config = load_config(config_path)
    planner = DownloadPlanner(config)
    plan_items = planner.load_latest_plan()
    if plan_items:
        run_plan(config, plan_items)
    else:
        console.print("[yellow]No download plan found[/yellow]")


@app.command()
def sort(
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Sort files according to rules."""
    config = load_config(config_path)
    sort_all(config)


@app.command()
def report(
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Configuration file path")
):
    """Show sync report."""
    config = load_config(config_path)
    handle_report(config)


if __name__ == "__main__":
    app()


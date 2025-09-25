#!/usr/bin/env python3
"""
Example usage of ANAC Sync programmatically.

This script demonstrates how to use ANAC Sync from Python code
instead of the command line interface.
"""

import tempfile
from pathlib import Path

from anacsync.config import Config, get_default_config
from anacsync.crawler import crawl_all
from anacsync.inventory import scan_local
from anacsync.planner import make_plan
from anacsync.downloader import run_plan
from anacsync.sorter import sort_all


def main():
    """Example usage of ANAC Sync."""
    print("ANAC Sync - Programmatic Usage Example")
    print("=" * 50)
    
    # Create a temporary configuration for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        # Set up configuration
        config = get_default_config()
        config.root_dir = str(Path(tmpdir) / "database" / "JSON")
        config.state_dir = str(Path(tmpdir) / ".anacsync")
        config.downloader.rate_limit_rps = 0.5  # Slower for testing
        
        print(f"Root directory: {config.root_dir}")
        print(f"State directory: {config.state_dir}")
        
        # Create directories
        Path(config.root_dir).mkdir(parents=True, exist_ok=True)
        Path(config.state_dir).mkdir(parents=True, exist_ok=True)
        
        try:
            # Step 1: Crawl datasets (this will take a while)
            print("\n1. Crawling ANAC datasets...")
            crawl_stats = crawl_all(config)
            print(f"   Found {crawl_stats['datasets_found']} datasets")
            print(f"   Found {crawl_stats['resources_found']} resources")
            
            # Step 2: Scan local files
            print("\n2. Scanning local files...")
            scan_stats = scan_local(config)
            print(f"   Scanned {scan_stats['files_scanned']} files")
            
            # Step 3: Generate download plan
            print("\n3. Generating download plan...")
            plan_items = make_plan(config, only_missing=True)
            print(f"   Plan contains {len(plan_items)} items")
            
            if plan_items:
                # Show first few items
                print("   First few items:")
                for i, item in enumerate(plan_items[:3]):
                    print(f"     {i+1}. {item.resource_name} ({item.reason})")
                
                # Step 4: Download files (only first few for demo)
                print(f"\n4. Downloading first 3 files...")
                demo_plan = plan_items[:3]
                download_stats = run_plan(config, demo_plan)
                print(f"   Downloaded {download_stats['successful']} files successfully")
                
                # Step 5: Sort files
                print("\n5. Sorting files...")
                sort_stats = sort_all(config)
                print(f"   Moved {sort_stats['files_moved']} files")
            else:
                print("   No files to download")
            
            print("\n✓ Example completed successfully!")
            
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()


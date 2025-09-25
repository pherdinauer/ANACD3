"""ANAC dataset crawler."""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from selectolax.parser import HTMLParser

from .config import Config
from .http_client import HTTPClient
from .utils import (
    append_jsonl, load_jsonl, save_jsonl, merge_jsonl_records,
    get_timestamp, jittered_delay, sleep_with_jitter, extract_filename_from_url
)

console = Console()


class DatasetRecord:
    """Dataset record structure."""
    
    def __init__(self, slug: str, title: str, url: str, last_seen_at: str):
        self.slug = slug
        self.title = title
        self.url = url
        self.last_seen_at = last_seen_at
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'slug': self.slug,
            'title': self.title,
            'url': self.url,
            'last_seen_at': self.last_seen_at
        }


class ResourceRecord:
    """Resource record structure."""
    
    def __init__(
        self,
        dataset_slug: str,
        name: str,
        format: str,
        url: str,
        content_length: Optional[int] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        accept_ranges: Optional[bool] = None,
        first_seen_at: Optional[str] = None,
        last_seen_at: Optional[str] = None
    ):
        self.dataset_slug = dataset_slug
        self.name = name
        self.format = format
        self.url = url
        self.content_length = content_length
        self.etag = etag
        self.last_modified = last_modified
        self.accept_ranges = accept_ranges
        self.first_seen_at = first_seen_at or get_timestamp()
        self.last_seen_at = last_seen_at or get_timestamp()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_slug': self.dataset_slug,
            'name': self.name,
            'format': self.format,
            'url': self.url,
            'content_length': self.content_length,
            'etag': self.etag,
            'last_modified': self.last_modified,
            'accept_ranges': self.accept_ranges,
            'first_seen_at': self.first_seen_at,
            'last_seen_at': self.last_seen_at
        }


class ANACCrawler:
    """ANAC dataset crawler."""
    
    def __init__(self, config: Config):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.datasets_file = self.state_dir / 'catalog' / 'datasets.jsonl'
        self.resources_file = self.state_dir / 'catalog' / 'resources.jsonl'
        
        # Load existing data
        self.existing_datasets = {r['slug']: r for r in load_jsonl(self.datasets_file)}
        self.existing_resources = {(r['dataset_slug'], r['url']): r for r in load_jsonl(self.resources_file)}
    
    def extract_dataset_slug(self, url: str) -> Optional[str]:
        """Extract dataset slug from URL."""
        # Pattern: /opendata/dataset/<slug>
        match = re.search(r'/opendata/dataset/([^/]+)', url)
        return match.group(1) if match else None
    
    def parse_dataset_page(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        """Parse dataset page and extract dataset information."""
        parser = HTMLParser(html)
        datasets = []
        seen_slugs = set()
        
        # First, try to find dataset items with specific classes
        dataset_items = parser.css('.dataset-item')
        for item in dataset_items:
            # Look for links within dataset items
            links = item.css('a')
            for link in links:
                href = link.attributes.get('href')
                if not href or '/opendata/dataset/' not in href:
                    continue
                
                # Make absolute URL
                url = urljoin(base_url, href)
                slug = self.extract_dataset_slug(url)
                
                if not slug or slug in seen_slugs:
                    continue
                
                # Get title - try multiple approaches
                title = link.text(strip=True)
                if not title:
                    # Try to get title from dataset heading
                    heading = item.css('.dataset-heading')
                    if heading:
                        title = heading[0].text(strip=True)
                    else:
                        # Try parent element
                        parent = link.parent
                        if parent:
                            title = parent.text(strip=True)
                
                if title and title not in ['JSON', 'CSV', 'XML']:  # Skip format links
                    seen_slugs.add(slug)
                    datasets.append({
                        'slug': slug,
                        'title': title,
                        'url': url
                    })
        
        # If no dataset items found, fall back to finding all links with dataset URLs
        if not datasets:
            all_links = parser.css('a')
            for link in all_links:
                href = link.attributes.get('href')
                if not href or '/opendata/dataset/' not in href:
                    continue
                
                # Make absolute URL
                url = urljoin(base_url, href)
                slug = self.extract_dataset_slug(url)
                
                if not slug or slug in seen_slugs:
                    continue
                
                # Get title
                title = link.text(strip=True)
                if not title or title in ['JSON', 'CSV', 'XML']:  # Skip format links
                    continue
                
                seen_slugs.add(slug)
                datasets.append({
                    'slug': slug,
                    'title': title,
                    'url': url
                })
        
        return datasets
    
    def parse_resource_page(self, html: str, dataset_slug: str, base_url: str) -> List[Dict[str, Any]]:
        """Parse dataset resource page and extract resource information."""
        parser = HTMLParser(html)
        resources = []
        seen_urls = set()
        
        # Get all links and filter for resource/download links
        all_links = parser.css('a')
        
        for link in all_links:
            href = link.attributes.get('href')
            if not href:
                continue
            
            # Make absolute URL
            url = urljoin(base_url, href)
            
            # Skip if we've already seen this URL
            if url in seen_urls:
                continue
            
            # Look for direct download links (preferred) or resource links
            is_download_link = '/download/' in url and any(url.endswith(ext) for ext in ['.json', '.csv', '.xlsx', '.xml', '.zip'])
            is_resource_link = '/resource/' in url
            
            if not (is_download_link or is_resource_link):
                continue
            
            # Get resource name
            name = link.text(strip=True)
            if not name or name in ['Altre informazioni', 'Vai alla risorsa']:
                # Try to extract from URL
                name = extract_filename_from_url(url)
                if not name:
                    # Generate name from URL path
                    path_parts = url.split('/')
                    if path_parts:
                        name = path_parts[-1]
            
            # Determine format from extension
            format_type = 'UNKNOWN'
            if url.endswith('.json'):
                format_type = 'JSON'
            elif url.endswith('.csv'):
                format_type = 'CSV'
            elif url.endswith('.xlsx'):
                format_type = 'XLSX'
            elif url.endswith('.xml'):
                format_type = 'XML'
            elif url.endswith('.zip'):
                format_type = 'ZIP'
            
            # Only add if we have a valid name and format
            if name and format_type != 'UNKNOWN':
                seen_urls.add(url)
                resources.append({
                    'dataset_slug': dataset_slug,
                    'name': name,
                    'format': format_type,
                    'url': url
                })
        
        return resources
    
    def crawl_page(self, page_num: int, http_client: HTTPClient) -> Tuple[List[Dict[str, Any]], bool]:
        """Crawl a single page of datasets."""
        url = f"{self.config.base_url}/dataset?page={page_num}"
        
        content, headers, error = http_client.get(url)
        if error:
            console.print(f"[red]Error crawling page {page_num}: {error}[/red]")
            return [], False
        
        html = content.decode('utf-8', errors='ignore')
        datasets = self.parse_dataset_page(html, self.config.base_url)
        
        return datasets, len(datasets) > 0
    
    def crawl_dataset_resources(self, dataset: Dict[str, Any], http_client: HTTPClient) -> List[Dict[str, Any]]:
        """Crawl resources for a specific dataset."""
        url = dataset['url']
        
        content, headers, error = http_client.get(url)
        if error:
            console.print(f"[red]Error crawling dataset {dataset['slug']}: {error}[/red]")
            return []
        
        html = content.decode('utf-8', errors='ignore')
        resources = self.parse_resource_page(html, dataset['slug'], self.config.base_url)
        
        # Get additional resource information
        for resource in resources:
            resource_info = http_client.check_resource_info(resource['url'])
            if 'error' not in resource_info:
                resource.update({
                    'content_length': resource_info.get('content_length'),
                    'etag': resource_info.get('etag'),
                    'last_modified': resource_info.get('last_modified'),
                    'accept_ranges': resource_info.get('accept_ranges')
                })
        
        return resources
    
    def crawl_all(self) -> Dict[str, Any]:
        """Crawl all datasets and resources."""
        console.print("[bold blue]Starting ANAC dataset crawl...[/bold blue]")
        
        stats = {
            'pages_crawled': 0,
            'datasets_found': 0,
            'datasets_new': 0,
            'datasets_updated': 0,
            'resources_found': 0,
            'resources_new': 0,
            'resources_updated': 0
        }
        
        with HTTPClient(self.config) as http_client:
            # Crawl dataset pages
            page_num = self.config.crawler.page_start
            empty_pages = 0
            all_datasets = []
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Crawling dataset pages...", total=None)
                
                while empty_pages < self.config.crawler.empty_page_stop_after:
                    datasets, has_content = self.crawl_page(page_num, http_client)
                    
                    if has_content:
                        all_datasets.extend(datasets)
                        empty_pages = 0
                        progress.update(task, description=f"Crawling page {page_num}... Found {len(datasets)} datasets")
                    else:
                        empty_pages += 1
                        progress.update(task, description=f"Page {page_num} empty ({empty_pages}/{self.config.crawler.empty_page_stop_after})")
                    
                    page_num += 1
                    stats['pages_crawled'] += 1
                    
                    # Rate limiting
                    sleep_with_jitter(
                        self.config.crawler.delay_ms_min,
                        self.config.crawler.delay_ms_max - self.config.crawler.delay_ms_min
                    )
            
            stats['datasets_found'] = len(all_datasets)
            
            # Process datasets
            new_datasets = []
            for dataset_data in all_datasets:
                slug = dataset_data['slug']
                dataset_data['last_seen_at'] = get_timestamp()
                
                if slug in self.existing_datasets:
                    # Update existing dataset
                    self.existing_datasets[slug].update(dataset_data)
                    stats['datasets_updated'] += 1
                else:
                    # New dataset
                    self.existing_datasets[slug] = dataset_data
                    stats['datasets_new'] += 1
                
                new_datasets.append(self.existing_datasets[slug])
            
            # Save datasets
            save_jsonl(self.datasets_file, list(self.existing_datasets.values()))
            
            # Crawl resources for each dataset
            all_resources = []
            resource_task = progress.add_task("Crawling dataset resources...", total=len(new_datasets))
            
            console.print(f"[blue]Starting to crawl resources for {len(new_datasets)} datasets...[/blue]")
            
            for i, dataset in enumerate(new_datasets):
                console.print(f"[yellow]Processing dataset {i+1}/{len(new_datasets)}: {dataset['slug']}[/yellow]")
                progress.update(resource_task, description=f"Crawling resources for {dataset['slug']}...")
                
                try:
                    resources = self.crawl_dataset_resources(dataset, http_client)
                    all_resources.extend(resources)
                    stats['resources_found'] += len(resources)
                    console.print(f"[green]Found {len(resources)} resources for {dataset['slug']}[/green]")
                except Exception as e:
                    console.print(f"[red]Error crawling resources for {dataset['slug']}: {e}[/red]")
                    continue
                
                # Rate limiting
                sleep_with_jitter(
                    self.config.crawler.delay_ms_min,
                    self.config.crawler.delay_ms_max - self.config.crawler.delay_ms_min
                )
                
                progress.advance(resource_task)
            
            # Process resources
            console.print(f"[blue]Processing {len(all_resources)} resources...[/blue]")
            for resource_data in all_resources:
                key = (resource_data['dataset_slug'], resource_data['url'])
                resource_data['last_seen_at'] = get_timestamp()
                
                if key in self.existing_resources:
                    # Update existing resource
                    self.existing_resources[key].update(resource_data)
                    stats['resources_updated'] += 1
                else:
                    # New resource
                    resource_data['first_seen_at'] = get_timestamp()
                    self.existing_resources[key] = resource_data
                    stats['resources_new'] += 1
            
            # Save resources
            console.print(f"[blue]Saving {len(self.existing_resources)} resources to catalog...[/blue]")
            save_jsonl(self.resources_file, list(self.existing_resources.values()))
            console.print(f"[green]Resources saved successfully![/green]")
        
        console.print(f"[green]Crawl completed![/green]")
        console.print(f"Pages crawled: {stats['pages_crawled']}")
        console.print(f"Datasets found: {stats['datasets_found']} (new: {stats['datasets_new']}, updated: {stats['datasets_updated']})")
        console.print(f"Resources found: {stats['resources_found']} (new: {stats['resources_new']}, updated: {stats['resources_updated']})")
        
        return stats


def crawl_all(config: Config) -> Dict[str, Any]:
    """Main function to crawl all ANAC datasets and resources."""
    crawler = ANACCrawler(config)
    return crawler.crawl_all()


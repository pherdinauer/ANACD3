"""Configuration management for ANAC Sync."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator


class CrawlerConfig(BaseModel):
    """Crawler configuration."""
    
    page_start: int = 1
    empty_page_stop_after: int = 2
    delay_ms_min: int = 300
    delay_ms_max: int = 700
    max_concurrency: int = 1
    respect_robots: bool = False


class HttpConfig(BaseModel):
    """HTTP client configuration."""
    
    timeout_connect_s: int = 10
    timeout_read_s: int = 60
    http2: bool = False  # Disable HTTP/2 by default to avoid h2 dependency
    headers: Dict[str, str] = Field(default_factory=dict)
    
    @validator('headers', pre=True)
    def set_default_headers(cls, v):
        if not v:
            return {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        return v


class DownloaderConfig(BaseModel):
    """Downloader configuration."""
    
    retries_per_strategy: int = 3
    switch_after_seconds_without_progress: int = 300
    strategies: List[str] = Field(default_factory=lambda: [
        "s1_dynamic", "s2_sparse", "s3_curl", "s4_shortconn", "s5_tailfirst"
    ])
    dynamic_chunks_mb: List[int] = Field(default_factory=lambda: [2, 6, 12])
    sparse_segment_mb: int = 4
    snail_chunks_kb: int = 1024
    overlap_bytes: int = 32768
    enable_curl: bool = True
    curl_path: str = "curl"
    rate_limit_rps: float = 1.0


class SortingRule(BaseModel):
    """Single sorting rule."""
    
    if_: str = Field(alias="if")
    move_to: str
    default: Optional[str] = None
    
    model_config = {"populate_by_name": True}


class SortingConfig(BaseModel):
    """Sorting configuration."""
    
    rules: List[SortingRule] = Field(default_factory=list)
    
    @validator('rules', pre=True)
    def parse_rules(cls, v):
        if isinstance(v, list):
            return [SortingRule(**rule) if isinstance(rule, dict) else rule for rule in v]
        return v


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    level: str = "INFO"
    file: Optional[str] = None


class Config(BaseModel):
    """Main configuration."""
    
    root_dir: str = "/database/JSON"
    base_url: str = "https://dati.anticorruzione.it/opendata"
    state_dir: Optional[str] = None
    
    crawler: CrawlerConfig = Field(default_factory=CrawlerConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)
    sorting: SortingConfig = Field(default_factory=SortingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    @validator('state_dir', pre=True)
    def set_default_state_dir(cls, v):
        if v is None:
            return str(Path.home() / ".anacsync")
        return str(v)
    
    @validator('sorting', pre=True)
    def parse_sorting_rules(cls, v):
        if isinstance(v, dict) and 'rules' not in v:
            # Handle old format where rules were directly in sorting
            rules = []
            for rule_data in v.get('rules', []):
                if isinstance(rule_data, dict):
                    rules.append(SortingRule(**rule_data))
            return SortingConfig(rules=rules)
        return v


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from file or create default."""
    if config_path is None:
        config_path = str(Path.home() / ".anacsync" / "anacsync.yaml")
    
    config_path = Path(config_path)
    
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    
    # Ensure state_dir is set if not provided
    if 'state_dir' not in data or data['state_dir'] is None:
        data['state_dir'] = str(Path.home() / ".anacsync")
    
    # Create default configuration
    config = Config(**data)
    
    # Ensure state directory exists
    state_dir = Path(config.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    for subdir in ['catalog', 'local', 'plans', 'downloads']:
        (state_dir / subdir).mkdir(exist_ok=True)
    
    return config


def save_config(config: Config, config_path: Optional[str] = None) -> None:
    """Save configuration to file."""
    if config_path is None:
        config_path = Path.home() / ".anacsync" / "anacsync.yaml"
    
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to dict and handle aliases
    data = config.dict(by_alias=True, exclude_none=True)
    
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_default_config() -> Config:
    """Get default configuration with example sorting rules."""
    # Create config with explicit state_dir to ensure it's not None
    config = Config(state_dir=str(Path.home() / ".anacsync"))
    
    # Add example sorting rules
    config.sorting.rules = [
        SortingRule(
            if_="slug matches '^ocds-appalti-ordinari'",
            move_to="/database/JSON/aggiudicazioni_json"
        ),
        SortingRule(
            if_="filename matches 'subappalti_.*\\.json'",
            move_to="/database/JSON/subappalti_json"
        ),
        SortingRule(
            if_="slug contains 'stazioni-appaltanti'",
            move_to="/database/JSON/stazioni-appaltanti_json"
        ),
        SortingRule(
            if_="true",  # default rule
            move_to="/database/JSON/_unsorted"
        )
    ]
    
    return config


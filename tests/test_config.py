"""Tests for configuration management."""

import tempfile
from pathlib import Path

import pytest
import yaml

from anacsync.config import Config, load_config, save_config, get_default_config


class TestConfig:
    """Test configuration functionality."""
    
    def test_default_config(self):
        """Test default configuration creation."""
        config = get_default_config()
        
        assert config.root_dir == "/database/JSON"
        assert config.base_url == "https://dati.anticorruzione.it/opendata"
        assert config.crawler.page_start == 1
        assert config.downloader.rate_limit_rps == 1.0
        assert len(config.downloader.strategies) == 5
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Valid config
        config_data = {
            "root_dir": "/test",
            "crawler": {
                "page_start": 1,
                "delay_ms_min": 100
            }
        }
        config = Config(**config_data)
        assert config.root_dir == "/test"
        assert config.crawler.page_start == 1
    
    def test_config_default_headers(self):
        """Test default HTTP headers."""
        config = Config()
        assert "User-Agent" in config.http.headers
        assert "anacsync" in config.http.headers["User-Agent"]
    
    def test_config_state_dir_default(self):
        """Test default state directory."""
        config = Config()
        assert config.state_dir is not None
        assert "anacsync" in config.state_dir


class TestConfigIO:
    """Test configuration file I/O."""
    
    def test_save_load_config(self):
        """Test saving and loading configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "test_config.yaml"
            
            # Create config
            config = get_default_config()
            config.root_dir = "/test/root"
            config.downloader.rate_limit_rps = 2.0
            
            # Save config
            save_config(config, str(config_path))
            
            # Load config
            loaded_config = load_config(str(config_path))
            
            assert loaded_config.root_dir == "/test/root"
            assert loaded_config.downloader.rate_limit_rps == 2.0
    
    def test_load_nonexistent_config(self):
        """Test loading nonexistent configuration file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "nonexistent.yaml"
            
            # Should create default config
            config = load_config(str(config_path))
            
            assert config.root_dir == "/database/JSON"
            assert config.base_url == "https://dati.anticorruzione.it/opendata"
    
    def test_load_empty_config(self):
        """Test loading empty configuration file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "empty.yaml"
            config_path.write_text("")
            
            # Should create default config
            config = load_config(str(config_path))
            
            assert config.root_dir == "/database/JSON"
    
    def test_config_yaml_format(self):
        """Test that saved config is valid YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "test.yaml"
            
            config = get_default_config()
            save_config(config, str(config_path))
            
            # Should be valid YAML
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f)
            
            assert data is not None
            assert "root_dir" in data
            assert "crawler" in data
            assert "downloader" in data


class TestSortingRules:
    """Test sorting rules configuration."""
    
    def test_sorting_rule_creation(self):
        """Test sorting rule creation."""
        from anacsync.config import SortingRule
        
        rule = SortingRule(if_="slug contains 'test'", move_to="/test/dir")
        
        assert rule.if_ == "slug contains 'test'"
        assert rule.move_to == "/test/dir"
        assert rule.default is None
    
    def test_sorting_rule_with_default(self):
        """Test sorting rule with default."""
        from anacsync.config import SortingRule
        
        rule = SortingRule(if_="true", move_to="/default", default="/fallback")
        
        assert rule.if_ == "true"
        assert rule.move_to == "/default"
        assert rule.default == "/fallback"
    
    def test_sorting_config_parsing(self):
        """Test sorting configuration parsing."""
        from anacsync.config import SortingConfig
        
        rules_data = [
            {"if": "slug contains 'test'", "move_to": "/test"},
            {"if": "true", "move_to": "/default"}
        ]
        
        sorting_config = SortingConfig(rules=rules_data)
        
        assert len(sorting_config.rules) == 2
        assert sorting_config.rules[0].if_ == "slug contains 'test'"
        assert sorting_config.rules[1].if_ == "true"


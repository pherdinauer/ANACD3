"""Tests for downloader strategies."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from anacsync.config import Config
from anacsync.downloader.strategies import (
    S1DynamicStrategy, S2SparseStrategy, S3CurlStrategy,
    S4ShortConnStrategy, S5TailFirstStrategy, DownloadResult
)


class TestDownloadResult:
    """Test DownloadResult dataclass."""
    
    def test_download_result_creation(self):
        """Test DownloadResult creation."""
        result = DownloadResult(
            ok=True,
            bytes_written=1024,
            strategy="test",
            etag="abc123",
            duration=1.5
        )
        
        assert result.ok is True
        assert result.bytes_written == 1024
        assert result.strategy == "test"
        assert result.etag == "abc123"
        assert result.duration == 1.5
        assert result.error is None
    
    def test_download_result_with_error(self):
        """Test DownloadResult with error."""
        result = DownloadResult(
            ok=False,
            bytes_written=0,
            strategy="test",
            error="Network error"
        )
        
        assert result.ok is False
        assert result.bytes_written == 0
        assert result.error == "Network error"


class TestStrategyBase:
    """Test base strategy functionality."""
    
    def test_chunk_size_calculation(self):
        """Test chunk size calculation based on file size."""
        config = Config()
        strategy = S1DynamicStrategy(config)
        
        # Small file
        chunk_size = strategy._get_chunk_size(10 * 1024 * 1024)  # 10MB
        assert chunk_size == config.downloader.dynamic_chunks_mb[0] * 1024 * 1024
        
        # Medium file
        chunk_size = strategy._get_chunk_size(100 * 1024 * 1024)  # 100MB
        assert chunk_size == config.downloader.dynamic_chunks_mb[1] * 1024 * 1024
        
        # Large file
        chunk_size = strategy._get_chunk_size(500 * 1024 * 1024)  # 500MB
        assert chunk_size == config.downloader.dynamic_chunks_mb[2] * 1024 * 1024
        
        # No size info
        chunk_size = strategy._get_chunk_size(None)
        assert chunk_size == config.downloader.dynamic_chunks_mb[0] * 1024 * 1024


class TestS1DynamicStrategy:
    """Test S1 Dynamic strategy."""
    
    def test_strategy_name(self):
        """Test strategy name."""
        config = Config()
        strategy = S1DynamicStrategy(config)
        assert strategy.name == "S1DynamicStrategy"
    
    @patch('anacsync.downloader.strategies.HTTPClient')
    def test_fetch_success(self, mock_http_client):
        """Test successful fetch."""
        config = Config()
        strategy = S1DynamicStrategy(config)
        
        # Mock HTTP client
        mock_client = Mock()
        mock_http_client.return_value.__enter__.return_value = mock_client
        
        # Mock resource info
        mock_client.check_resource_info.return_value = {
            'content_length': 1024,
            'etag': 'abc123'
        }
        
        # Mock range request
        mock_client.get_range.return_value = (b"test content", {}, None)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "test.txt"
            meta = {}
            
            result = strategy.fetch("http://example.com/test", dest_path, meta, config)
            
            assert result.ok is True
            assert result.bytes_written > 0
            assert result.strategy == "S1DynamicStrategy"
            assert dest_path.exists()
    
    @patch('anacsync.downloader.strategies.HTTPClient')
    def test_fetch_resource_info_error(self, mock_http_client):
        """Test fetch with resource info error."""
        config = Config()
        strategy = S1DynamicStrategy(config)
        
        # Mock HTTP client
        mock_client = Mock()
        mock_http_client.return_value.__enter__.return_value = mock_client
        
        # Mock resource info error
        mock_client.check_resource_info.return_value = {'error': 'Network error'}
        
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "test.txt"
            meta = {}
            
            result = strategy.fetch("http://example.com/test", dest_path, meta, config)
            
            assert result.ok is False
            assert "Failed to get resource info" in result.error


class TestS2SparseStrategy:
    """Test S2 Sparse strategy."""
    
    def test_strategy_name(self):
        """Test strategy name."""
        config = Config()
        strategy = S2SparseStrategy(config)
        assert strategy.name == "S2SparseStrategy"
    
    def test_segment_order(self):
        """Test segment order generation."""
        config = Config()
        strategy = S2SparseStrategy(config)
        
        # Test with different numbers of segments
        order1 = strategy._get_segment_order(1)
        assert order1 == [0]
        
        order2 = strategy._get_segment_order(2)
        assert order2 == [0, 1]
        
        order3 = strategy._get_segment_order(3)
        assert 0 in order3  # Should start with first segment
        assert 2 in order3  # Should include last segment
        assert len(order3) == 3  # Should include all segments


class TestS3CurlStrategy:
    """Test S3 Curl strategy."""
    
    def test_strategy_name(self):
        """Test strategy name."""
        config = Config()
        strategy = S3CurlStrategy(config)
        assert strategy.name == "S3CurlStrategy"
    
    @patch('subprocess.run')
    def test_fetch_curl_disabled(self, mock_run):
        """Test fetch with curl disabled."""
        config = Config()
        config.downloader.enable_curl = False
        strategy = S3CurlStrategy(config)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "test.txt"
            meta = {}
            
            result = strategy.fetch("http://example.com/test", dest_path, meta, config)
            
            assert result.ok is False
            assert "Curl strategy disabled" in result.error
    
    @patch('subprocess.run')
    def test_fetch_curl_not_found(self, mock_run):
        """Test fetch with curl not found."""
        config = Config()
        config.downloader.enable_curl = True
        strategy = S3CurlStrategy(config)
        
        # Mock curl not found
        mock_run.side_effect = FileNotFoundError()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "test.txt"
            meta = {}
            
            result = strategy.fetch("http://example.com/test", dest_path, meta, config)
            
            assert result.ok is False
            assert "Curl not found" in result.error


class TestS4ShortConnStrategy:
    """Test S4 Short Connections strategy."""
    
    def test_strategy_name(self):
        """Test strategy name."""
        config = Config()
        strategy = S4ShortConnStrategy(config)
        assert strategy.name == "S4ShortConnStrategy"


class TestS5TailFirstStrategy:
    """Test S5 Tail-First strategy."""
    
    def test_strategy_name(self):
        """Test strategy name."""
        config = Config()
        strategy = S5TailFirstStrategy(config)
        assert strategy.name == "S5TailFirstStrategy"
    
    @patch('anacsync.downloader.strategies.HTTPClient')
    def test_fetch_no_file_size(self, mock_http_client):
        """Test fetch with unknown file size."""
        config = Config()
        strategy = S5TailFirstStrategy(config)
        
        # Mock HTTP client
        mock_client = Mock()
        mock_http_client.return_value.__enter__.return_value = mock_client
        
        # Mock resource info without file size
        mock_client.check_resource_info.return_value = {
            'content_length': None,
            'etag': 'abc123'
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            dest_path = Path(tmpdir) / "test.txt"
            meta = {}
            
            result = strategy.fetch("http://example.com/test", dest_path, meta, config)
            
            assert result.ok is False
            assert "File size unknown" in result.error


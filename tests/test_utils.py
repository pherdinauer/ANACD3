"""Tests for utility functions."""

import tempfile
from pathlib import Path

import pytest

from anacsync.utils import (
    atomic_write, calculate_sha256, append_jsonl, read_jsonl,
    load_jsonl, save_jsonl, format_bytes, format_duration,
    safe_filename, get_file_info
)


class TestAtomicWrite:
    """Test atomic write functionality."""
    
    def test_atomic_write_text(self):
        """Test atomic write of text content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.txt"
            content = "Hello, World!"
            
            atomic_write(file_path, content)
            
            assert file_path.exists()
            assert file_path.read_text() == content
    
    def test_atomic_write_binary(self):
        """Test atomic write of binary content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.bin"
            content = b"Hello, World!"
            
            atomic_write(file_path, content, mode='wb')
            
            assert file_path.exists()
            assert file_path.read_bytes() == content
    
    def test_atomic_write_cleanup_on_error(self):
        """Test that temp file is cleaned up on error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.txt"
            
            # This should raise an error due to invalid mode
            with pytest.raises(ValueError):
                atomic_write(file_path, "content", mode='invalid')
            
            # Temp file should not exist
            temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
            assert not temp_path.exists()


class TestHashing:
    """Test hashing functionality."""
    
    def test_calculate_sha256(self):
        """Test SHA256 calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.txt"
            content = "Hello, World!"
            file_path.write_text(content)
            
            hash_value = calculate_sha256(file_path)
            
            # SHA256 of "Hello, World!" should be consistent
            expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
            assert hash_value == expected
    
    def test_calculate_sha256_empty_file(self):
        """Test SHA256 calculation for empty file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "empty.txt"
            file_path.touch()
            
            hash_value = calculate_sha256(file_path)
            
            # SHA256 of empty string
            expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            assert hash_value == expected


class TestJSONL:
    """Test JSONL functionality."""
    
    def test_append_jsonl(self):
        """Test appending to JSONL file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.jsonl"
            
            # Append first record
            record1 = {"id": 1, "name": "test1"}
            append_jsonl(file_path, record1)
            
            # Append second record
            record2 = {"id": 2, "name": "test2"}
            append_jsonl(file_path, record2)
            
            # Read back
            records = list(read_jsonl(file_path))
            
            assert len(records) == 2
            assert records[0] == record1
            assert records[1] == record2
    
    def test_save_load_jsonl(self):
        """Test saving and loading JSONL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.jsonl"
            
            records = [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ]
            
            save_jsonl(file_path, records)
            loaded_records = load_jsonl(file_path)
            
            assert loaded_records == records
    
    def test_read_jsonl_empty_file(self):
        """Test reading from empty JSONL file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "empty.jsonl"
            file_path.touch()
            
            records = list(read_jsonl(file_path))
            assert records == []


class TestFormatting:
    """Test formatting functions."""
    
    def test_format_bytes(self):
        """Test byte formatting."""
        assert format_bytes(0) == "0.0 B"
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1024 * 1024) == "1.0 MB"
        assert format_bytes(1024 * 1024 * 1024) == "1.0 GB"
        assert format_bytes(1024 * 1024 * 1024 * 1024) == "1.0 TB"
    
    def test_format_duration(self):
        """Test duration formatting."""
        assert format_duration(30) == "30.0s"
        assert format_duration(90) == "1.5m"
        assert format_duration(3600) == "1.0h"
        assert format_duration(7200) == "2.0h"


class TestSafeFilename:
    """Test safe filename generation."""
    
    def test_safe_filename_basic(self):
        """Test basic safe filename generation."""
        assert safe_filename("test.txt") == "test.txt"
        assert safe_filename("test file.txt") == "test file.txt"
    
    def test_safe_filename_unsafe_chars(self):
        """Test removal of unsafe characters."""
        assert safe_filename("test<file>.txt") == "test_file_.txt"
        assert safe_filename("test:file.txt") == "test_file.txt"
        assert safe_filename("test/file.txt") == "test_file.txt"
    
    def test_safe_filename_empty(self):
        """Test empty filename handling."""
        assert safe_filename("") == "unnamed"
        assert safe_filename("   ") == "unnamed"
        assert safe_filename("...") == "unnamed"
    
    def test_safe_filename_length_limit(self):
        """Test filename length limiting."""
        long_name = "a" * 300 + ".txt"
        safe_name = safe_filename(long_name)
        assert len(safe_name) <= 200
        assert safe_name.endswith(".txt")


class TestFileInfo:
    """Test file info functionality."""
    
    def test_get_file_info(self):
        """Test getting file information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.txt"
            content = "Hello, World!"
            file_path.write_text(content)
            
            info = get_file_info(file_path)
            
            assert info['size'] == len(content)
            assert 'mtime' in info
            assert 'sha256' in info
            assert info['sha256'] == calculate_sha256(file_path)
    
    def test_get_file_info_nonexistent(self):
        """Test getting info for nonexistent file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "nonexistent.txt"
            
            info = get_file_info(file_path)
            
            assert info == {}


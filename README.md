# ANAC Sync

Professional ANAC dataset crawler and downloader with multi-strategy download system.

## Features

- **Smart Crawling**: Automatically discovers and catalogs all ANAC datasets
- **Multi-Strategy Downloads**: 5 different download strategies with automatic fallback
- **Local Inventory**: Tracks downloaded files with integrity verification
- **Intelligent Sorting**: Automatic file organization based on configurable rules
- **User-Friendly CLI**: Interactive menu-driven interface
- **No Database**: Everything stored in JSON/NDJSON files
- **Robust Error Handling**: Graceful recovery from network issues

## Installation

```bash
pip install -e .
```

## Quick Start

Run the interactive CLI:

```bash
anacsync
```

Or use individual commands:

```bash
anacsync crawl        # Update catalog
anacsync scan         # Scan local files
anacsync plan         # Generate download plan
anacsync download     # Execute download plan
anacsync sort         # Apply sorting rules
anacsync report       # Show summary
anacsync verify       # Verify file integrity
```

## Configuration

Configuration is stored in `~/.anacsync/anacsync.yaml`. The default configuration includes:

- Rate limiting and retry policies
- Download strategy preferences
- File sorting rules
- Logging settings

## Download Strategies

1. **S1 - Dynamic Range Streaming**: Adaptive chunk sizes with resume support
2. **S2 - Sparse Segments with Bitmap**: Non-linear segment downloading
3. **S3 - External Tools**: Uses curl/wget for maximum compatibility
4. **S4 - Short Connections**: Small chunks with connection close
5. **S5 - Tail-First**: Downloads end first to validate file stability

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Format code
black anacsync/
ruff check anacsync/ --fix

# Run tests
pytest

# Type checking
mypy anacsync/
```

## License

MIT License


# ANAC Sync - Quick Start Guide

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd anacsync

# Install in development mode
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

## First Run

1. **Start the interactive CLI:**
   ```bash
   anacsync
   ```

2. **Follow the menu:**
   - Select option `1` to crawl ANAC datasets
   - Select option `2` to scan local files
   - Select option `3` to generate a download plan
   - Select option `4` to download files
   - Select option `5` to sort files

## Configuration

The application will create a default configuration at `~/.anacsync/anacsync.yaml`. You can customize:

- **Root directory**: Where to store downloaded files
- **Download strategies**: Which strategies to use and in what order
- **Sorting rules**: How to organize downloaded files
- **Rate limiting**: How fast to download

## Command Line Usage

Instead of the interactive menu, you can use individual commands:

```bash
# Crawl datasets
anacsync crawl

# Scan local files
anacsync scan

# Generate download plan
anacsync plan

# Download files
anacsync download

# Sort files
anacsync sort

# Show report
anacsync report
```

## Download Strategies

ANAC Sync uses 5 different download strategies:

1. **S1 Dynamic**: Adaptive chunk sizes with resume support
2. **S2 Sparse**: Non-linear segment downloading with bitmap
3. **S3 Curl**: External curl tool for maximum compatibility
4. **S4 Short**: Small chunks with connection close
5. **S5 Tail-First**: Downloads end first for validation

The system automatically tries strategies in order and falls back if one fails.

## File Organization

Files are automatically sorted based on configurable rules. Example rules:

- Files with `ocds-appalti-ordinari` in the slug → `aggiudicazioni_json/`
- CSV files → `csv_files/`
- Excel files → `excel_files/`
- Everything else → `_unsorted/`

## Development

```bash
# Format code
make format

# Run linting
make lint

# Run tests
make test

# Run all checks
make check
```

## Troubleshooting

### Common Issues

1. **Permission errors**: Make sure you have write access to the root directory
2. **Network timeouts**: Adjust timeout settings in configuration
3. **Curl not found**: Install curl or disable S3 strategy in config
4. **Memory issues**: Reduce chunk sizes in configuration

### Getting Help

- Use `anacsync --help` for command help
- Check the configuration file at `~/.anacsync/anacsync.yaml`
- Review logs in `~/.anacsync/anacsync.log`

## Example Workflow

```bash
# 1. Start with crawling
anacsync crawl

# 2. Scan your local files
anacsync scan

# 3. Generate a plan for missing files
anacsync plan

# 4. Download the files
anacsync download

# 5. Organize the files
anacsync sort

# 6. Check the results
anacsync report
```

This workflow will discover all available ANAC datasets, compare them with your local files, download what's missing, and organize everything according to your rules.


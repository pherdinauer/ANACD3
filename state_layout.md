# ANAC Sync - State Layout Documentation

## Directory Structure

The application stores all state in JSON/NDJSON files under the state directory (default: `~/.anacsync/`).

```
~/.anacsync/
├── catalog/
│   ├── datasets.jsonl        # Dataset records (one per line)
│   └── resources.jsonl       # Resource records (one per line)
├── local/
│   └── inventory.jsonl       # Local file records (one per line)
├── plans/
│   └── plan-YYYYMMDD.jsonl   # Download plans (one per line)
├── downloads/
│   └── history.jsonl         # Download attempt history (one per line)
└── anacsync.yaml             # Configuration file
```

## File Formats

### Dataset Record (datasets.jsonl)
```json
{
  "slug": "ocds-appalti-ordinari-2022",
  "title": "Appalti Ordinari 2022",
  "url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022",
  "last_seen_at": "2025-01-25T10:00:00Z"
}
```

### Resource Record (resources.jsonl)
```json
{
  "dataset_slug": "ocds-appalti-ordinari-2022",
  "name": "ocds-appalti-ordinari-20240201.json",
  "format": "JSON",
  "url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022/resource/uuid",
  "content_length": 123456789,
  "etag": "W/\"abc123\"",
  "last_modified": "Mon, 01 Jul 2024 10:00:00 GMT",
  "accept_ranges": true,
  "first_seen_at": "2025-01-25T10:00:00Z",
  "last_seen_at": "2025-01-25T10:00:00Z"
}
```

### Local File Record (inventory.jsonl)
```json
{
  "path": "/database/JSON/aggiudicazioni_json/ocds-appalti-ordinari-20240201.json",
  "sha256": "f7ab...9c",
  "size": 123456789,
  "mtime": "2025-01-25T10:00:00Z",
  "dataset_slug": "ocds-appalti-ordinari-2022",
  "url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022/resource/uuid"
}
```

### Plan Item (plans/plan-YYYYMMDD.jsonl)
```json
{
  "dataset_slug": "ocds-appalti-ordinari-2022",
  "resource_url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022/resource/uuid",
  "dest_path": "/database/JSON/aggiudicazioni_json/ocds-appalti-ordinari-20240201.json",
  "reason": "missing",
  "size": 123456789,
  "etag": "W/\"abc123\""
}
```

### Download Attempt (downloads/history.jsonl)
```json
{
  "resource_url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022/resource/uuid",
  "strategy": "s1_dynamic",
  "start": "2025-01-25T10:00:00Z",
  "end": "2025-01-25T10:05:00Z",
  "bytes": 123456789,
  "ok": true,
  "error": null
}
```

## Sidecar Files

Each downloaded file has a corresponding `.meta.json` sidecar file:

```
/database/JSON/aggiudicazioni_json/ocds-appalti-ordinari-20240201.json
/database/JSON/aggiudicazioni_json/ocds-appalti-ordinari-20240201.json.meta.json
```

### Sidecar Content
```json
{
  "url": "https://dati.anticorruzione.it/opendata/dataset/ocds-appalti-ordinari-2022/resource/uuid",
  "dataset_slug": "ocds-appalti-ordinari-2022",
  "resource_name": "ocds-appalti-ordinari-20240201.json",
  "etag": "W/\"abc123\"",
  "last_modified": "Mon, 01 Jul 2024 10:00:00 GMT",
  "content_length": 123456789,
  "accept_ranges": true,
  "sha256": "f7ab...9c",
  "downloaded_at": "2025-01-25T10:00:00Z",
  "strategy": "s1_dynamic",
  "segments": {
    "size": 4194304,
    "bitmap": "111011..."
  },
  "retries": 2,
  "notes": ""
}
```

## Configuration File (anacsync.yaml)

See the default configuration in the main application for the complete schema.

## Data Flow

1. **Crawl**: Updates `catalog/datasets.jsonl` and `catalog/resources.jsonl`
2. **Scan**: Updates `local/inventory.jsonl`
3. **Plan**: Creates `plans/plan-YYYYMMDD.jsonl`
4. **Download**: Updates `downloads/history.jsonl` and creates sidecar files
5. **Sort**: Moves files and updates inventory
6. **Report**: Reads all files to generate summary

## Atomic Operations

- All file writes use atomic operations (write to `.tmp` then rename)
- NDJSON files use append-only operations
- Sidecar files are updated atomically with the main file
- Configuration changes are atomic


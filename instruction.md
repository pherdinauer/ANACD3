Ecco un **prompt pronto-incolla per Cursor** (lungo e completo) che definisce l’app Python “anacsync” solo-file, con crawler, planner, downloader multi-strategia e sorting automatico. Includo requisiti, architettura, contratti delle funzioni, CLI, formati dei JSON/NDJSON, fallback, logging, qualità codice e test. Puoi incollarlo così com’è in Cursor per farti generare lo scheletro del progetto e i moduli principali.

---

# PROMPT PER CURSOR — PROGETTO “anacsync”

Voglio un’applicazione Python **professionale e user-friendly** che gira su Linux e **NON usa alcun database**: tutti gli stati devono essere salvati su **file JSON/NDJSON**. L’app deve:

1. **Crawling** dei dataset ANAC
   Sito: `https://dati.anticorruzione.it/opendata/dataset`

   * Scansiona tutte le pagine con paginazione `?page=N` (N parte da 1).
   * Interrompi in modo “open-ended”: se due pagine **consecutive** oltre l’ultima piena (es. 6 e 7) sono vuote → **stop**.
   * Per ogni dataset raccogli `slug`, `title`, `url_dataset`.
   * Apri ogni dataset e cattura **tutte** le risorse (file) con: `name`, `format` (es. JSON), `url_file` (link diretto alla risorsa).
   * Fai `HEAD` (se possibile) ad ogni `url_file` per `Content-Length`, `ETag`, `Last-Modified`, `Accept-Ranges`. Se `HEAD` non è supportato, gestisci con `GET` a corpo scartato e deduci ciò che puoi.
   * **Rate limit gentile**: default 1 req/s (configurabile) con jitter casuale e `User-Agent` chiaro: `anacsync/<version> (+optional-contact)`; limita la concorrenza a 1–2.

2. **Inventario Locale**

   * Scansiona ricorsivamente `/database/JSON/**` (configurabile) per individuare file `.json`/`.ndjson` ecc.
   * Per ciascun file locale calcola **sha256** in streaming (no file intero in RAM), `size`, `mtime`.
   * Ogni file scaricato dall’app ha un **sidecar** `*.meta.json` adiacente (vedi schema sotto) che contiene l’URL sorgente, etag, ecc. Usa il sidecar per riconciliare file locale ↔ risorsa remota. In assenza di sidecar usa regole di matching (slug/filename/pattern).

3. **Pianificazione (diff)**

   * Confronta il **catalogo remoto** con l’**inventario locale** per creare un **piano** dei file *mancanti* o *da aggiornare* (etag/size variati).
   * Esporta il piano come NDJSON (`plans/plan-YYYYMMDD.jsonl`).

4. **Download robusto e prudente** con **strategie DAVVERO differenti** (non solo sleep). Implementa un **manager** che prova in cascata più strategie, mantenendo i byte già scaricati e registrando la storia:

   * **S1 – Range streaming dinamico (Python/httpx)**
     Range con ripresa da `offset`, chunk adattivi in base a `Content-Length`:
     `<50MB → 2MB` | `50–300MB → 6MB` | `>300MB → 12MB` (configurabile).
     Keep-Alive, HTTP/1.1 o 2; checkpoint (offset, sha256 parziale) nel sidecar; overlap di 32KB alla ripresa.
   * **S2 – Segmenti “sparsi” con bitmap**
     Divide il file in segmenti (es. 4MB) e mantiene una mappa dei blocchi scaricati nel sidecar. Ordine non lineare (inizio → fine → centro) per aggirare time-out tardivi. Alla ripresa scarica solo i blocchi mancanti.
   * **S3 – Tool esterno affidabile (curl o wget)**
     Invoca `curl` con `-C - --retry 10 --retry-delay 5 --limit-rate 200k --location`. Usa `--continue-at -` per ripresa se supportata. Logga il comando. Opzionale via config (se curl non presente, salta).
   * **S4 – Connessioni brevi “a capitoli”**
     Scarica blocchi piccoli (512KB–1MB) con **Connection: close** ad ogni blocco. Utile se il server degrada su connessioni lunghe.
   * **S5 – Tail-first**
     Scarica l’ultimo MB (Range) per validare `Content-Length`/stabilità, poi completa il resto.
   * **Regole di fallback**: S1 → (se 2–3 errori di rete o 5 min senza progresso) S2 → S3 → S4 → S5. Parametri configurabili.
   * **Integrità**: a fine download calcola sha256 e salva nel sidecar. Se mismatch rispetto a un checksum noto (quando disponibile), marca `corrupted` e riprova con strategia più conservativa.
   * **Atomicità**: scarica in `*.part`, fsync, poi `rename` atomico sul file finale; aggiorna `*.meta.json`.

5. **Sorting automatico**

   * Dopo download, smista i file nelle sottocartelle giuste di `/database/JSON/**` applicando **regole dichiarative** in YAML (regex su `slug`, `url`, `filename`).
   * Se una risorsa non matcha alcuna regola, mettila in `_unsorted` e segnala nel report.

6. **Reportistica e UX**

   * CLI semplice con `typer` + `rich` (progress bar, tabelle):

     ```
     anacsync crawl        # aggiorna catalogo
     anacsync scan         # scansiona locale
     anacsync plan         # genera piano (solo mancanti/changed)
     anacsync download     # esegue piano (filtri: --slug --only-missing)
     anacsync sort         # applica regole di smistamento
     anacsync report       # stampa riepilogo differenze/integrità
     anacsync verify       # ricalcola hash e verifica file
     ```
   * Opzioni globali: `--config`, `--state-dir`, `--root /database/JSON`, `--rate-limit`, `--max-concurrency`, `--dry-run`, `--log-file`, `--quiet`.
   * Exit codes chiari (0 ok, 20 nessuna novità, 30 errori parziali, 40 fallimenti download).

---

## Architettura del progetto (solo file, niente DB)

```
anacsync/
  __init__.py
  cli.py
  config.py           # parsing YAML, default values, paths
  http_client.py      # client httpx condiviso + header UA + retry base
  crawler.py          # paginate + parse dataset + parse risorse
  inventory.py        # scansione locale + sidecar handling + hashing stream
  planner.py          # diff catalogo vs inventario → piano (NDJSON)
  sorter.py           # motore regole regex per smistamento
  utils.py            # fs atomici, jsonl helpers, hashing, timers
  downloader/
    __init__.py
    manager.py        # state machine, orchestrazione strategie, logging history
    strategies.py     # S1..S5 come classi separate con interfaccia comune
state_layout.md       # documentazione dei file su disco
pyproject.toml
README.md
```

### Dipendenze

* `httpx` (timeout, HTTP/2 opzionale, streaming)
* `typer[all]` (CLI)
* `rich` (log/console/progress)
* `pydantic` (validazione config/record)
* `tenacity` (backoff dove serve)
* `python-dotenv` (opzionale)
* **Niente** database. Solo filesystem.

---

## File di stato e formati (JSON/NDJSON)

**Directory di stato** predefinita: `~/.anacsync/` (configurabile).
Struttura:

```
~/.anacsync/
  catalog/
    datasets.jsonl        # 1 riga = DatasetRecord
    resources.jsonl       # 1 riga = ResourceRecord
  local/
    inventory.jsonl       # 1 riga = LocalFileRecord
  plans/
    plan-YYYYMMDD.jsonl   # 1 riga = PlanItem
  downloads/
    history.jsonl         # 1 riga = DownloadAttempt
  anacsync.yaml           # config + sorting rules
```

**Sidecar per file scaricato**

```
/database/JSON/.../foo.json
/database/JSON/.../foo.json.meta.json
```

Contenuto `*.meta.json` (esempio):

```json
{
  "url": "https://dati.anticorruzione.it/opendata/dataset/.../resource/uuid",
  "dataset_slug": "ocds-appalti-ordinari-2022",
  "resource_name": "ocds-...-20240201.json",
  "etag": "W/\"abc123\"",
  "last_modified": "Mon, 01 Jul 2024 10:00:00 GMT",
  "content_length": 123456789,
  "accept_ranges": true,
  "sha256": "f7ab...9c",
  "downloaded_at": "2025-09-25T09:00:00Z",
  "strategy": "s2_sparse_bitmap",
  "segments": {
    "size": 4194304,
    "bitmap": "111011... (opzionale)"
  },
  "retries": 2,
  "notes": ""
}
```

**Schema record (indicativo, Pydantic)**

* `DatasetRecord`: `{ "slug": str, "title": str, "url": str, "last_seen_at": iso8601 }`
* `ResourceRecord`: `{ "dataset_slug": str, "name": str, "format": str, "url": str, "content_length": int|null, "etag": str|null, "last_modified": str|null, "accept_ranges": bool|null, "first_seen_at": iso, "last_seen_at": iso }`
* `LocalFileRecord`: `{ "path": str, "sha256": str, "size": int, "mtime": iso, "dataset_slug": str|null, "url": str|null }`
* `PlanItem`: `{ "dataset_slug": str, "resource_url": str, "dest_path": str, "reason": "missing|etag_changed|size_changed", "size": int|null, "etag": str|null }`
* `DownloadAttempt`: `{ "resource_url": str, "strategy": str, "start": iso, "end": iso, "bytes": int, "ok": bool, "error": str|null }`

**Helper NDJSON**: funzioni per append atomico riga per riga.

---

## Config YAML di esempio (`~/.anacsync/anacsync.yaml`)

```yaml
root_dir: "/database/JSON"
base_url: "https://dati.anticorruzione.it/opendata"

crawler:
  page_start: 1
  empty_page_stop_after: 2
  delay_ms_min: 300
  delay_ms_max: 700
  max_concurrency: 1
  respect_robots: false   # se in futuro servisse

http:
  timeout_connect_s: 10
  timeout_read_s: 60
  http2: true
  headers:
    User-Agent: "anacsync/0.1 (+contact@example.com)"
    Accept-Encoding: "identity"

downloader:
  retries_per_strategy: 3
  switch_after_seconds_without_progress: 300
  strategies: ["s1_dynamic", "s2_sparse", "s3_curl", "s4_shortconn", "s5_tailfirst"]
  dynamic_chunks_mb: [2, 6, 12]
  sparse_segment_mb: 4
  snail_chunks_kb: 1024
  overlap_bytes: 32768
  enable_curl: true
  curl_path: "curl"
  rate_limit_rps: 1

sorting:
  - if: "slug matches '^ocds-appalti-ordinari'"
    move_to: "/database/JSON/aggiudicazioni_json"
  - if: "filename matches 'subappalti_.*\\.json'"
    move_to: "/database/JSON/subappalti_json"
  - if: "slug contains 'stazioni-appaltanti'"
    move_to: "/database/JSON/stazioni-appaltanti_json"
  - default: "/database/JSON/_unsorted"

logging:
  level: "INFO"
  file: "~/.anacsync/anacsync.log"
```

---

## Contratti funzioni principali (interfacce)

```python
# crawler.py
def crawl_all(cfg) -> dict:
    """
    Raccoglie dataset e risorse da tutte le pagine.
    Aggiorna catalog/datasets.jsonl e catalog/resources.jsonl (append/merge).
    Ritorna statistiche (pagine_viste, dataset_nuovi, risorse_nuove, risorse_aggiornate).
    """

# inventory.py
def scan_local(cfg) -> dict:
    """
    Scansiona root_dir, aggiorna local/inventory.jsonl.
    Calcola sha256 in streaming solo per file nuovi o mtime cambiato.
    Tenta riconciliazione con risorse via sidecar/heuristiche.
    """

# planner.py
def make_plan(cfg, only_missing=True, filter_slug: str | None = None) -> list[dict]:
    """
    Confronta catalogo e inventario, produce lista PlanItem e scrive plans/plan-*.jsonl.
    """

# downloader/manager.py
def run_plan(cfg, plan: list[dict]) -> dict:
    """
    Esegue il piano invocando le strategie in cascata.
    Scrive downloads/history.jsonl, aggiorna sidecar *.meta.json.
    Ritorna metriche (ok, failed, bytes_totali).
    """

# sorter.py
def sort_all(cfg) -> dict:
    """
    Applica le regole dichiarative al set di file scaricati.
    Move atomici, crea cartelle se mancano.
    Ritorna report (mossi_per_regola, non_matchati).
    """
```

**Interfaccia comune strategie di download**

```python
class StrategyBase:
    name: str
    def fetch(self, url: str, dest_path: Path, meta: dict, cfg: dict) -> DownloadResult:
        """Scarica (o riprende) fino a completamento oppure solleva eccezione.
        Usa e aggiorna `dest_path.with_suffix(dest.suffix + ".part")` e il sidecar meta.
        """

@dataclass
class DownloadResult:
    ok: bool
    bytes_written: int
    strategy: str
    etag: str | None = None
    error: str | None = None
```

---

## Requisiti di robustezza

* **Respectful scraping**: jitter fra richieste, header condizionali `If-None-Match`/`If-Modified-Since` quando ri-crawli.
* **Fallback protocollo**: spegni HTTP/2 se instabile (toggle in config), forza `Accept-Encoding: identity`.
* **Gestione server fragili**: in S4 usa `Connection: close`, in S2 ordina i segmenti in modo non sequenziale.
* **Rilevamento zero-progress**: se `bytes_written` non aumenta per X secondi → cambia strategia.
* **Atomicità filesystem**: sempre `.part` + fsync + `os.replace`.
* **Error handling**: eccezioni annotate e salvate in `downloads/history.jsonl`.
* **Idempotenza**: se un file è già completo e hash combacia, **skip**.

---

## Logging & osservabilità

* Console con `rich` (progresso per file, tabella finale con `ok/failed`, throughput medio).
* File log configurabile.
* Comando `report` che mostra: `presenti`, `mancanti`, `da aggiornare`, `orfani`, `corrupted`.

---

## Qualità del codice

* Python 3.11+.
* Tipi `typing` completi + docstring.
* `ruff` + `black` + `mypy` (tollerante).
* Struttura pacchetto con `pyproject.toml` e entry-point console `anacsync`.
* Piccola suite `pytest`:

  * parser HTML (snapshot dei selettori/estrattori)
  * hashing streaming su file finti grandi
  * strategie S1 e S2 su `http.server` locale simulando interruzioni
  * unit test del sorter con regole regex

---

## Note di parsing HTML (resiliente)

* Preferisci **selectolax** o `lxml.html` con selettori robusti.
* I link ai dataset sono in `/opendata/dataset/<slug>`; estrai `title` visibile.
* Nelle pagine dataset, le risorse hanno link `/resource/<uuid>` o direttamente link al file.
* Gestisci redirect e `Content-Disposition`.
* Se il markup cambia, fallback a **pattern URL** (regex) per non rompere.

---

## Sicurezza & permessi

* Scrivi tutto come utente normale; crea le cartelle mancanti in `/database/JSON` e in `~/.anacsync`.
* Non memorizzare token/credenziali.
* Non saturare la banda: `rate_limit_rps` configurabile.

---

## Accettazione (Definition of Done)

* `anacsync crawl` crea/aggiorna `catalog/*.jsonl` popolati.
* `anacsync scan` crea/aggiorna `local/inventory.jsonl`.
* `anacsync plan` produce `plans/plan-*.jsonl` con almeno 10 campi per riga (vedi `PlanItem`).
* `anacsync download` scarica almeno un file reale ANAC con **S1**; se si forza un errore a metà (interrompendo la rete), al successivo avvio riprende e completa; se falliscono 2 tentativi con S1, passa a **S2**; se configurato `enable_curl: true`, S3 può essere invocata.
* Ogni file completo ha `*.meta.json` coerente e `sha256` calcolato.
* `anacsync sort` sposta i file nelle cartelle previste; i non matchati vanno in `_unsorted`.
* `anacsync report` elenca chiaramente cosa è *mancante*, *da aggiornare*, *orfano*, *corrotto*.
* Codice tipato, lintato, test di base verdi.

---

## Extra (opzionali ma graditi)

* Flag `--dry-run` per `crawl/plan/download/sort`.
* `--output json` per `report`.
* Script `Makefile` con target `fmt`, `lint`, `test`, `run`.
* Dockerfile minimale (non obbligatorio).

---

### Importante

* **Nessun database**: persistenza **solo** su file JSON/NDJSON + sidecar `.meta.json`.
* Strategie di download **diverse** tra loro (Range dinamico, segmenti sparsi con bitmap, tool esterno curl, connessioni brevi, tail-first).
* Progetta il codice per essere **estendibile**: aggiungere S6 in futuro deve essere facile.

**Genera ora lo scheletro completo del progetto “anacsync” conforme a queste specifiche, inclusi file e moduli, con docstring e TODO dove servono.**

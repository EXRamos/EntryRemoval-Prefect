# Prefect Cloud deployment for `entry_remove.py`

This scaffold wraps your existing `entry_remove.py` CLI as a Prefect 2.x flow so you can schedule or trigger it from Prefect Cloud.

## Repo layout

```
prefect_entry_remove_project/
├── entry_remove.py              # <- place your script here (copied from your upload)
├── flows/
│   └── entry_remove_flow.py     # Prefect flow wrapper
├── prefect.yaml                 # Project + deployment configuration
├── requirements.txt
└── .prefectignore
```

> **Note**  
> The flow calls your script via `python entry_remove.py -f ... -t ... -e ...` and does not change its behavior.

## Quick start (local worker + Process work pool)

This is the simplest path—no Docker image required.

1. **Install dependencies**
   ```bash
   python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Log in to Prefect Cloud**
   ```bash
   prefect cloud login
   ```
   - Choose your _workspace_ when prompted (or set with `prefect config set PREFECT_API_URL=...`).

3. **Create a work pool of type _Process_** (one time)
   ```bash
   prefect work-pool create process-pool --type process
   ```

4. **Start a worker attached to that pool** (keep this running)
   ```bash
   prefect worker start --pool process-pool
   ```

5. **Deploy the flow**
   Edit `prefect.yaml` and set:
   ```yaml
   work_pool:
     name: process-pool
     work_queue_name: default
   ```
   Then run:
   ```bash
   prefect deploy --name prod
   ```

6. **Provide inputs (files)**
   Make sure the following exist relative to the project root _on the worker machine_:
   ```
   data/manifest.xlsx
   data/template.xlsx
   data/entries.tsv
   ```
   You can change these paths in your deployment parameters.

7. **Run it**
   - From UI: open your deployment `entry-remove-project/prod` and click **Run**.
   - CLI:
     ```bash
     prefect deployment run 'entry-remove-project/prod'        -p manifest_path=data/manifest.xlsx        -p template_path=data/template.xlsx        -p entries_path=data/entries.tsv
     ```

## Alternative: Docker work pool

If you need an isolated environment with pinned libraries:

1. Create a Docker work pool:
   ```bash
   prefect work-pool create docker-pool --type docker
   ```

2. Build an image (example):
   ```Dockerfile
   FROM prefecthq/prefect:2-python3.11
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt
   COPY . .
   ```
   Build & push your image to a registry you can pull from your workers.

3. In `prefect.yaml` set:
   ```yaml
   work_pool:
     name: docker-pool
     work_queue_name: default
     job_variables:
       image: ghcr.io/YOUR_ORG/entry-remove:latest
   ```

4. Start a Docker worker and `prefect deploy`.

## Notes about your script flags

The wrapper expects your script to accept:
- `-f / --file` for the input manifest
- `-t / --template` for the template Excel (must include a `Dictionary` sheet)
- `-e / --entry` for the TSV of node_ids to remove

If you later remove the template dependency or add new flags, just update `flows/entry_remove_flow.py` accordingly.

## Observability

The flow publishes a small **Markdown artifact** with the return code and the last part of stdout/stderr to the Prefect UI. Your script also writes an `*_log.txt` and a cleaned workbook (`*_EntRemoveYYYYMMDD.xlsx`) in the project directory.

## Triggering with different parameters

Update deployment parameters in `prefect.yaml` or pass them at runtime:

```bash
prefect deployment run 'entry-remove-project/prod'   -p manifest_path=/absolute/path/manifest.xlsx   -p template_path=/absolute/path/template.xlsx   -p entries_path=/absolute/path/entries.tsv
```

## Common gotchas

- The **Process** worker runs jobs on the same machine—make sure the file paths you use are **reachable on the worker**.
- If you see `ModuleNotFoundError` for `openpyxl` or `XlsxWriter`, check that your worker environment has installed `requirements.txt`.
- If you need to read/write large Excel files, consider increasing worker memory or moving to a Docker work pool.

# Prefect Cloud deployment for `entry_remove.py` (S3-first with local fallback)

Workspace:
- Account: `90cb3bf5-1af1-44fa-8a6d-a1f111368e02`
- Workspace: `468a881e-3696-47aa-ae2e-d6942a047666`

## Login & target the workspace
```bash
prefect cloud login
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/90cb3bf5-1af1-44fa-8a6d-a1f111368e02/workspaces/468a881e-3696-47aa-ae2e-d6942a047666"
```

## Prepare the repo
```bash
git clone https://github.com/EXRamos/EntryRemoval-Prefect
cd EntryRemoval-Prefect
# add or update: flows/entry_remove_flow.py, prefect.yaml, requirements.txt, .prefectignore
```

## Install & create a work pool
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

prefect work-pool create process-pool --type process   # or: docker-pool --type docker
prefect worker start --pool process-pool
```

## Git is public — no token needed
If you later make the repo private, set `GIT_AUTH_TOKEN` on the worker or create a Prefect GitHub block.

## AWS creds for S3
Provide one of:
- IAM Role on the worker
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION=us-east-1`
- `~/.aws/credentials`

## Configure `prefect.yaml`
- **Defaults** point to S3. Change `YOUR-BUCKET` and paths.
- Set your work pool:
```yaml
work_pool:
  name: process-pool        # or docker-pool
  work_queue_name: default
```

## Deploy
```bash
prefect deploy --name prod
```

## Run — S3-first (recommended)
```bash
prefect deployment run 'entry-remove-project/prod' \
  -p manifest_path="s3://YOUR-BUCKET/path/to/manifest.xlsx" \
  -p template_path="s3://YOUR-BUCKET/path/to/template.xlsx" \
  -p entries_path="s3://YOUR-BUCKET/path/to/entries.tsv" \
  -p s3_output_prefix="s3://YOUR-BUCKET/outputs/entry-remove/"
```

### Local fallback when needed
```bash
prefect deployment run 'entry-remove-project/prod' \
  -p manifest_path="data/manifest.xlsx" \
  -p template_path="data/template.xlsx" \
  -p entries_path="data/entries.tsv" \
  -p s3_output_prefix="s3://YOUR-BUCKET/outputs/entry-remove/"
```

## Behavior recap
- Any `s3://...` input is auto-downloaded to a temp dir and used by your CLI.
- The flow invokes your script unchanged:
  `python entry_remove.py -f <manifest> -t <template> -e <entries>`
- All outputs matching `*_EntRemove*.xlsx` are uploaded to the `s3_output_prefix` you provide.
- A Markdown artifact with return code + stdout/stderr tail and uploaded file list appears in the Prefect UI.

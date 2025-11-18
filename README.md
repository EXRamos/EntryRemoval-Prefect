# Entry Removal – Prefect Cloud + ECS

**S3-First Processing**

This project packages your `entry_remove.py` script into a Prefect-managed workflow that can run:

* Locally (default)
* As a Prefect Cloud deployment
* On AWS ECS using your Prefect ECS work pool
* Using S3 as the primary input/output storage system

By default, the deployment **clones the GitHub repo into the ECS container** at run time (via Prefect’s `git_clone` step).
**No Docker image is required** unless you choose to use one.

---

# 1. Project Overview

The project consists of:

* **`entry_remove.py`**
  Your main script that removes entries from CCDI manifests.

* **`flows/entry_remove_flow.py`**
  Prefect flow that:

  * Accepts UI parameters (s3_bucket, manifest_key, template_key, entries_key, output_prefix)
  * Downloads input files from S3 if keys are provided
  * Calls `entry_remove.py` inside the execution environment
  * Uploads output files back to S3
  * Publishes a Prefect artifact summary

* **`prefect.yaml`**
  Deployment configuration that:

  * Defines parameters
  * Pulls your GitHub repo automatically at runtime
  * Targets an ECS work pool

* **`requirements.txt`**
  Python dependencies into whatever environment executes the flow.

---

# 2. Local Development Setup (Default)

Clone the project and install dependencies:

```bash
git clone https://github.com/EXRamos/EntryRemoval-Prefect
cd EntryRemoval-Prefect

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

(Optional) If accessing S3 locally:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

---

# 3. Setup Prefect Cloud Access (CCDI Workspace)

```bash
prefect cloud login
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/<ACCOUNT_ID>/workspaces/<CCDI_WORKSPACE_ID>"
```

Check active workspace:

```bash
prefect config view | grep PREFECT_API_URL
```

---

# 4. Running Locally

### **Quick local run (default)**

Place test files under:

```
data/manifest.xlsx
data/template.xlsx
data/entries.tsv
```

Then simply:

```bash
python flows/entry_remove_flow.py
```

The flow will:

* Use **local paths** (ignored if S3 keys are provided)
* Run `entry_remove.py`
* Save output files in the project root
* Publish a local Prefect artifact summary (if Prefect is connected)

---

# 5. Running via Prefect Cloud

The `prefect.yaml` file instructs Prefect to:

* Pull your GitHub repo at runtime
* Execute inside the default environment of your **ECS work pool**

### Deploy from your local machine:

```bash
prefect deploy --name ccdi-prod
```

A deployment called:

```
entry-remove-project / ccdi-prod
```

appears in your CCDI workspace.

---

# 6. UI Parameters (CCDI Deployment)

You can run the deployment using the Prefect UI or CLI.

### **S3-first run example**

```bash
prefect deployment run 'entry-remove-project/ccdi-prod' \
  -p s3_bucket="ccdi-validation" \
  -p manifest_key="incoming/user123/manifest.xlsx" \
  -p template_key="templates/template.xlsx" \
  -p entries_key="incoming/user123/remove.tsv" \
  -p output_prefix="outputs/entry-remove/user123/"
```

The flow will:

1. Build S3 URLs:

   ```
   s3://ccdi-validation/<manifest_key>
   s3://ccdi-validation/<template_key>
   s3://ccdi-validation/<entries_key>
   ```
2. Download each into `/app/data/` inside ECS
3. Run `entry_remove.py`
4. Upload outputs matching:

   ```
   *_EntRemove*.xlsx
   ```

   to:

   ```
   s3://ccdi-validation/<output_prefix>
   ```
5. Publish a summary artifact in Prefect Cloud

---

# 7. Local Fallback with Paths (No S3)

```bash
prefect deployment run 'entry-remove-project/ccdi-prod' \
  -p manifest_path="data/manifest.xlsx" \
  -p template_path="data/template.xlsx" \
  -p entries_path="data/entries.tsv" \
  -p output_prefix="outputs/local-test/"
```

---

# 8. Output File Behavior

### Local mode

* Output manifests are saved in the **project root**
* Pattern matched by: `*_EntRemove*.xlsx`

### ECS mode

* Output files written inside container working directory (`/app`)
* Uploaded to S3 using the provided `output_prefix`


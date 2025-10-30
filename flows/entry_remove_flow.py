from __future__ import annotations

import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# Optional S3 support
try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


def _is_s3_path(p: str) -> bool:
    return isinstance(p, str) and p.startswith("s3://")


def _split_s3_url(url: str) -> Tuple[str, str]:
    assert url.startswith("s3://"), "Not an S3 URL"
    without = url.removeprefix("s3://")
    bucket, key = without.split("/", 1)
    return bucket, key


@task
def stage_inputs(manifest_path: str, template_path: str, entries_path: str) -> Tuple[str, str, str, Optional[str]]:
    """
    Download inputs from S3 to a temp dir if paths are s3://...,
    else validate local paths. Returns resolved paths and the temp dir (if used).
    """
    logger = get_run_logger()
    tmpdir = None
    s3_used = any(_is_s3_path(p) for p in (manifest_path, template_path, entries_path))

    if s3_used:
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 paths; install boto3 and configure AWS creds.")
        import boto3 as _boto3
        s3 = _boto3.client("s3")
        import tempfile as _tempfile
        tmpdir = _tempfile.mkdtemp(prefix="entry-remove-")

        def dl(src: str) -> str:
            if _is_s3_path(src):
                b, k = _split_s3_url(src)
                dst = str(Path(tmpdir) / Path(k).name)
                logger.info("Downloading %s -> %s", src, dst)
                s3.download_file(b, k, dst)
                return dst
            else:
                p = Path(src).expanduser().resolve()
                if not p.exists():
                    raise FileNotFoundError(f"Local input not found: {p}")
                return str(p)

        m = dl(manifest_path)
        t = dl(template_path)
        e = dl(entries_path)
        return m, t, e, tmpdir

    # Local-only
    def check(p: str) -> str:
        path = Path(p).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        return str(path)

    return check(manifest_path), check(template_path), check(entries_path), tmpdir


@task
def run_entry_remove_script(manifest: str, template: str, entries: str, python_exe: Optional[str] = None) -> dict:
    """
    Run entry_remove.py as a subprocess. Returns stdout, stderr, and return code.
    """
    logger = get_run_logger()
    python = python_exe or sys.executable
    cmd = f'{python} entry_remove.py -f "{manifest}" -t "{template}" -e "{entries}"'
    logger.info("Running command: %s", cmd)

    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1])  # repo root where entry_remove.py should live
    )
    logger.info("Return code: %s", proc.returncode)
    if proc.stdout:
        logger.info("STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("STDERR:\n%s", proc.stderr)

    return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


@task
def ship_outputs_to_s3(outputs_glob: str, s3_output_prefix: Optional[str]):
    """
    Upload files matching outputs_glob to s3_output_prefix if provided.
    s3_output_prefix like: s3://my-bucket/path/prefix/
    """
    if not s3_output_prefix:
        return None
    if boto3 is None:
        raise RuntimeError("boto3 is required for S3 upload; install boto3 and configure AWS creds.")

    from pathlib import Path
    import boto3 as _boto3

    s3 = _boto3.client("s3")
    bucket, prefix = _split_s3_url(s3_output_prefix.rstrip("/") + "/")
    uploaded = []
    for p in Path(".").glob(outputs_glob):
        if p.is_file():
            key = f"{prefix}{p.name}"
            s3.upload_file(str(p), bucket, key)
            uploaded.append(f"s3://{bucket}/{key}")
    return uploaded


@task
def publish_run_summary(result: dict, uploaded: Optional[list] = None):
    md = [
        "# entry_remove.py run summary",
        f"**Return code:** `{result['returncode']}`",
    ]
    if result.get("stdout"):
        md += ["## Stdout", "```", result["stdout"][-8000:], "```"]
    if result.get("stderr"):
        md += ["## Stderr", "```", result["stderr"][-8000:], "```"]
    if uploaded:
        md.append("## Uploaded outputs")
        md += [f"- {u}" for u in uploaded]
    create_markdown_artifact(key="entry-remove-summary", markdown="\n".join(md))


from prefect import flow

@flow(name="entry-remove-flow")
def run_entry_remove(
    manifest_path: str = "s3://YOUR-BUCKET/path/to/manifest.xlsx",
    template_path: str = "s3://YOUR-BUCKET/path/to/template.xlsx",
    entries_path: str = "s3://YOUR-BUCKET/path/to/entries.tsv",
    outputs_glob: str = "*_EntRemove*.xlsx",
    s3_output_prefix: Optional[str] = "s3://YOUR-BUCKET/outputs/entry-remove/",
    python_exe: Optional[str] = None,
):
    """
    Prefect flow that invokes your entry_remove.py CLI.
    - S3-first defaults for inputs and output upload prefix
    - Local fallback supported by passing non-s3 paths
    """
    m, t, e, _tmpdir = stage_inputs(manifest_path, template_path, entries_path)
    result = run_entry_remove_script(m, t, e, python_exe)
    uploaded = ship_outputs_to_s3(outputs_glob, s3_output_prefix)
    publish_run_summary(result, uploaded)
    return result


if __name__ == "__main__":
    run_entry_remove()

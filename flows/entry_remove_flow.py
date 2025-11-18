from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# Optional S3 support
try:
    import boto3  # type: ignore
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
def stage_inputs(manifest_path: str, template_path: str, entries_path: str) -> Tuple[str, str, str]:
    """
    Local-first defaults under ./data; if any path is s3://..., download to ./data at runtime.
    """
    logger = get_run_logger()
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    def resolve(p: str) -> str:
        if _is_s3_path(p):
            if boto3 is None:
                raise RuntimeError("boto3 is required for S3 paths; install boto3 and configure AWS creds.")
            b, k = _split_s3_url(p)
            dst = data_dir / Path(k).name
            logger.info("Downloading %s -> %s", p, dst)
            import boto3 as _boto3  # local import
            _boto3.client("s3").download_file(b, k, str(dst))
            return str(dst.resolve())
        # local path
        path = Path(p).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        return str(path.resolve())

    return resolve(manifest_path), resolve(template_path), resolve(entries_path)


@task
def run_entry_remove_script(manifest: str, template: str, entries: str, python_exe: Optional[str] = None) -> dict:
    """
    Run entry_remove.py as a subprocess. Returns stdout, stderr, and return code.
    """
    logger = get_run_logger()
    python = python_exe or sys.executable
    cmd = f'{shlex.quote(python)} entry_remove.py -f {shlex.quote(manifest)} -t {shlex.quote(template)} -e {shlex.quote(entries)}'
    logger.info("Running command: %s", cmd)

    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1])  # repo/project root where entry_remove.py should live
    )
    logger.info("Return code: %s", proc.returncode)
    if proc.stdout:
        logger.info("STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("STDERR:\n%s", proc.stderr)

    return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


@task
def ship_outputs_to_s3(outputs_glob: str, s3_output_prefix: Optional[str] = None):
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

    bucket, prefix = _split_s3_url(s3_output_prefix.rstrip('/') + '/')
    s3 = _boto3.client("s3")
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


@flow(name="entry-remove-flow")
def run_entry_remove(
    # Local-first defaults for convenient local testing
    manifest_path: str = "data/manifest.xlsx",
    template_path: str = "data/template.xlsx",
    entries_path: str = "data/entries.tsv",
    # You can still push outputs to S3 when desired
    outputs_glob: str = "*_EntRemove*.xlsx",
    s3_output_prefix: Optional[str] = None,
    python_exe: Optional[str] = None,
):
    """
    Prefect flow that invokes your entry_remove.py CLI.
    Local-first defaults; S3 supported via s3:// parameters and s3_output_prefix.
    """
    m, t, e = stage_inputs(manifest_path, template_path, entries_path)
    result = run_entry_remove_script(m, t, e, python_exe)
    uploaded = ship_outputs_to_s3(outputs_glob, s3_output_prefix)
    publish_run_summary(result, uploaded)
    return result


# Optional local dev server aligned with the training doc's 'serve' guidance. :contentReference[oaicite:1]{index=1}
def serve_locally():
    run_entry_remove.serve(
        name="entry-remove-local",
        tags=["local", "dev"],
        parameters={
            "manifest_path": "data/manifest.xlsx",
            "template_path": "data/template.xlsx",
            "entries_path": "data/entries.tsv",
            "outputs_glob": "*_EntRemove*.xlsx",
            "s3_output_prefix": None,
        },
    )


if __name__ == "__main__":
    # Default behavior: run the flow locally (no worker required)
    run_entry_remove()
    # For an interactive local deployment server, uncomment:
    # serve_locally()

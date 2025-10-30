from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


@task
def ensure_paths(manifest_path: str, template_path: str, entries_path: str):
    """Fail early if any path is missing."""
    for p in (manifest_path, template_path, entries_path):
        path = Path(p)
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
    return str(Path(manifest_path).resolve()), str(Path(template_path).resolve()), str(Path(entries_path).resolve())


@task
def run_entry_remove_script(manifest: str, template: str, entries: str, python_exe: Optional[str] = None) -> dict:
    """
    Run the attached entry_remove.py script as a subprocess.
    Returns stdout, stderr, and return code.
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
        cwd=str(Path(__file__).resolve().parents[1])  # project root where entry_remove.py will live
    )
    logger.info("Return code: %s", proc.returncode)
    if proc.stdout:
        logger.info("STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("STDERR:\n%s", proc.stderr)

    return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


@task
def publish_run_summary(result: dict):
    """
    Publish a small artifact so you can see outputs in the Prefect UI.
    """
    md = [
        "# entry_remove.py run summary",
        "",
        f"**Return code:** `{result['returncode']}`",
    ]
    if result.get("stdout"):
        md.append("## Stdout")
        md.append("```")
        md.append(result["stdout"][-8000:])  # clip to avoid super long artifacts
        md.append("```")
    if result.get("stderr"):
        md.append("## Stderr")
        md.append("```")
        md.append(result["stderr"][-8000:])
        md.append("```")
    create_markdown_artifact(key="entry-remove-summary", markdown="\n".join(md))


@flow(name="entry-remove-flow")
def run_entry_remove(
    manifest_path: str = "data/manifest.xlsx",
    template_path: str = "data/template.xlsx",
    entries_path: str = "data/entries.tsv",
    python_exe: Optional[str] = None,
):
    """
    Prefect flow that invokes the user's entry_remove.py CLI.

    Parameters
    ----------
    manifest_path : str
        Path to the CCDI manifest (.xlsx/.csv/.tsv).
    template_path : str
        Path to the template Excel with a 'Dictionary' sheet.
    entries_path : str
        Path to TSV containing node_ids to remove.
    python_exe : Optional[str]
        Alternative Python executable. Defaults to current interpreter.
    """
    manifest, template, entries = ensure_paths(manifest_path, template_path, entries_path)
    result = run_entry_remove_script(manifest, template, entries, python_exe)
    publish_run_summary(result)
    return result


if __name__ == "__main__":
    # Useful for local testing without Prefect
    run_entry_remove()

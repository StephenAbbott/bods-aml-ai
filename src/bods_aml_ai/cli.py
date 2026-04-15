"""CLI entry point for the BODS -> AML AI pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from bods_aml_ai.config import TransformConfig
from bods_aml_ai.pipeline import AMLPipeline


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def main(verbose: bool):
    """Transform BODS v0.4 data into Google AML AI input format."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@main.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option(
    "--output-dir", "-o",
    default="output",
    help="Output directory for NDJSON files (default: output/)",
)
@click.option(
    "--no-account-links",
    is_flag=True,
    default=False,
    help="Skip generating synthetic AccountPartyLink rows",
)
@click.option(
    "--validity-time",
    default=None,
    help="Override validity_start_time (RFC 3339 timestamp). Defaults to now.",
)
def transform(
    input_path: str,
    output_dir: str,
    no_account_links: bool,
    validity_time: str | None,
):
    """Transform a BODS JSON or JSONL file into AML AI table files.

    INPUT_PATH can be a single JSON file (array of statements) or a
    JSONL file (one statement per line).

    Outputs NDJSON files in OUTPUT_DIR:
      - party.ndjson
      - party_supplementary_data.ndjson
      - account_party_link.ndjson (unless --no-account-links)
    """
    config = TransformConfig(
        output_dir=output_dir,
        include_account_party_links=not no_account_links,
    )
    if validity_time:
        config.validity_start_time = validity_time

    pipeline = AMLPipeline(config)
    total = pipeline.process_file(input_path)

    if total == 0:
        click.echo("No BODS statements found in input file.", err=True)
        return

    counts = pipeline.finalize()

    click.echo(f"\nBODS -> AML AI transformation complete:")
    click.echo(f"  Input: {total} BODS statements")
    for table, count in sorted(counts.items()):
        click.echo(f"  {table}: {count} rows -> {output_dir}/{table}.ndjson")


@main.command()
@click.argument("input_dir", type=click.Path(exists=True))
@click.option(
    "--output-dir", "-o",
    default="output",
    help="Output directory for NDJSON files (default: output/)",
)
@click.option(
    "--no-account-links",
    is_flag=True,
    default=False,
    help="Skip generating synthetic AccountPartyLink rows",
)
def batch(
    input_dir: str,
    output_dir: str,
    no_account_links: bool,
):
    """Process all JSON/JSONL files in a directory.

    Reads every .json and .jsonl file in INPUT_DIR and transforms
    them into AML AI table files.
    """
    config = TransformConfig(
        output_dir=output_dir,
        include_account_party_links=not no_account_links,
    )

    pipeline = AMLPipeline(config)
    input_path = Path(input_dir)

    files = sorted(
        list(input_path.glob("*.json")) + list(input_path.glob("*.jsonl"))
    )
    if not files:
        click.echo(f"No JSON/JSONL files found in {input_dir}", err=True)
        return

    total = 0
    for f in files:
        try:
            count = pipeline.process_file(f)
            click.echo(f"  {f.name}: {count} statements")
            total += count
        except Exception as e:
            click.echo(f"  {f.name}: ERROR - {e}", err=True)

    if total == 0:
        click.echo("No BODS statements found in any input file.", err=True)
        return

    counts = pipeline.finalize()

    click.echo(f"\nBODS -> AML AI transformation complete:")
    click.echo(f"  Input: {total} BODS statements from {len(files)} files")
    for table, count in sorted(counts.items()):
        click.echo(f"  {table}: {count} rows -> {output_dir}/{table}.ndjson")


if __name__ == "__main__":
    main()

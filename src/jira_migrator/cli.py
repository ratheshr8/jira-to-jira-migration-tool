from __future__ import annotations

import json

import typer
from rich.console import Console

from jira_migrator.config import get_settings
from jira_migrator.db import Database
from jira_migrator.jira_client import JiraClient
from jira_migrator.migrator import JiraMigrator


app = typer.Typer(help="Resumable Jira Cloud migration CLI.")
console = Console()


def build_migrator() -> JiraMigrator:
    settings = get_settings()
    source = JiraClient(
        base_url=settings.source_jira_base_url,
        email=settings.source_jira_email,
        api_token=settings.source_jira_api_token,
        ssl_verify=settings.jira_ssl_verify,
        ca_bundle=settings.jira_ca_bundle,
        trust_env=settings.jira_trust_env,
    )
    target = JiraClient(
        base_url=settings.target_jira_base_url,
        email=settings.target_jira_email,
        api_token=settings.target_jira_api_token,
        ssl_verify=settings.jira_ssl_verify,
        ca_bundle=settings.jira_ca_bundle,
        trust_env=settings.jira_trust_env,
    )
    database = Database(settings.database_url)
    mapping = settings.load_mapping()
    return JiraMigrator(
        source=source,
        target=target,
        database=database,
        mapping=mapping,
        default_target_issue_type=settings.default_target_issue_type,
    )


@app.command()
def init_db() -> None:
    """Create Postgres tables used for checkpointing and id mapping."""
    settings = get_settings()
    database = Database(settings.database_url)
    database.init_schema()
    console.print("[green]Database schema initialized[/green]")


@app.command()
def validate() -> None:
    """Validate Jira credentials and API access."""
    migrator = build_migrator()
    migrator.validate()


@app.command()
def inspect_project(project: str = typer.Option(..., "--project", help="Source project key")) -> None:
    """Print source project summary to help build mappings."""
    migrator = build_migrator()
    summary = migrator.inspect_project(project)
    console.print_json(json.dumps(summary))


@app.command()
def migrate_project(
    source_project: str = typer.Option(..., "--source-project", help="Source project key"),
    target_project: str = typer.Option(..., "--target-project", help="Target project key"),
    skip_migrated: bool = typer.Option(
        False,
        "--skip-migrated",
        help="Skip source issues that already exist in migration mapping DB.",
    ),
) -> None:
    """Migrate source project issues, comments, attachments, and statuses."""
    migrator = build_migrator()
    migrator.migrate_project(source_project, target_project, skip_migrated=skip_migrated)


@app.command()
def migrate_filters(
    source_project: str = typer.Option(..., "--source-project", help="Source project key"),
    target_project: str = typer.Option(..., "--target-project", help="Target project key"),
    skip_migrated: bool = typer.Option(
        False,
        "--skip-migrated",
        help="Skip filters already present in migration mapping DB.",
    ),
) -> None:
    """Migrate Jira filters and rewrite JQL project keys."""
    migrator = build_migrator()
    migrator.migrate_filters(source_project, target_project, skip_migrated=skip_migrated)


@app.command()
def migrate_dashboards(
    source_project: str = typer.Option(..., "--source-project", help="Source project key"),
    target_project: str = typer.Option(..., "--target-project", help="Target project key"),
    skip_migrated: bool = typer.Option(
        False,
        "--skip-migrated",
        help="Skip dashboards already present in migration mapping DB.",
    ),
) -> None:
    """Migrate Jira dashboards with best-effort gadget copy."""
    migrator = build_migrator()
    migrator.migrate_dashboards(source_project, target_project, skip_migrated=skip_migrated)


if __name__ == "__main__":
    app()

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row


SCHEMA_SQL = """
create table if not exists migration_runs (
    id bigserial primary key,
    source_project_key text not null,
    target_project_key text not null,
    started_at timestamptz not null default now()
);

create table if not exists issue_map (
    source_issue_key text primary key,
    target_issue_key text not null,
    migration_run_id bigint not null references migration_runs(id),
    created_at timestamptz not null default now()
);

create table if not exists comment_map (
    source_comment_id text primary key,
    target_comment_id text not null,
    source_issue_key text not null,
    target_issue_key text not null,
    created_at timestamptz not null default now()
);

create table if not exists attachment_map (
    source_attachment_id text primary key,
    target_attachment_id text not null,
    source_issue_key text not null,
    target_issue_key text not null,
    created_at timestamptz not null default now()
);

create table if not exists filter_map (
    source_filter_id text primary key,
    target_filter_id text not null,
    migration_run_id bigint not null references migration_runs(id),
    created_at timestamptz not null default now()
);

create table if not exists dashboard_map (
    source_dashboard_id text primary key,
    target_dashboard_id text not null,
    migration_run_id bigint not null references migration_runs(id),
    created_at timestamptz not null default now()
);
"""


class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            yield conn

    def init_schema(self) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            conn.commit()

    def create_run(self, source_project_key: str, target_project_key: str) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into migration_runs (source_project_key, target_project_key)
                values (%s, %s)
                returning id
                """,
                (source_project_key, target_project_key),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])

    def get_target_issue_key(self, source_issue_key: str) -> str | None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select target_issue_key from issue_map where source_issue_key = %s",
                (source_issue_key,),
            )
            row = cur.fetchone()
            return row["target_issue_key"] if row else None

    def save_issue_map(self, source_issue_key: str, target_issue_key: str, run_id: int) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into issue_map (source_issue_key, target_issue_key, migration_run_id)
                values (%s, %s, %s)
                on conflict (source_issue_key) do update
                set target_issue_key = excluded.target_issue_key,
                    migration_run_id = excluded.migration_run_id
                """,
                (source_issue_key, target_issue_key, run_id),
            )
            conn.commit()

    def comment_exists(self, source_comment_id: str) -> bool:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select 1 from comment_map where source_comment_id = %s",
                (source_comment_id,),
            )
            return cur.fetchone() is not None

    def save_comment_map(
        self,
        source_comment_id: str,
        target_comment_id: str,
        source_issue_key: str,
        target_issue_key: str,
    ) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into comment_map (
                    source_comment_id, target_comment_id, source_issue_key, target_issue_key
                )
                values (%s, %s, %s, %s)
                on conflict (source_comment_id) do nothing
                """,
                (source_comment_id, target_comment_id, source_issue_key, target_issue_key),
            )
            conn.commit()

    def attachment_exists(self, source_attachment_id: str) -> bool:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select 1 from attachment_map where source_attachment_id = %s",
                (source_attachment_id,),
            )
            return cur.fetchone() is not None

    def save_attachment_map(
        self,
        source_attachment_id: str,
        target_attachment_id: str,
        source_issue_key: str,
        target_issue_key: str,
    ) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into attachment_map (
                    source_attachment_id, target_attachment_id, source_issue_key, target_issue_key
                )
                values (%s, %s, %s, %s)
                on conflict (source_attachment_id) do nothing
                """,
                (source_attachment_id, target_attachment_id, source_issue_key, target_issue_key),
            )
            conn.commit()

    def get_target_filter_id(self, source_filter_id: str) -> str | None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select target_filter_id from filter_map where source_filter_id = %s",
                (source_filter_id,),
            )
            row = cur.fetchone()
            return row["target_filter_id"] if row else None

    def save_filter_map(self, source_filter_id: str, target_filter_id: str, run_id: int) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into filter_map (source_filter_id, target_filter_id, migration_run_id)
                values (%s, %s, %s)
                on conflict (source_filter_id) do update
                set target_filter_id = excluded.target_filter_id,
                    migration_run_id = excluded.migration_run_id
                """,
                (source_filter_id, target_filter_id, run_id),
            )
            conn.commit()

    def get_target_dashboard_id(self, source_dashboard_id: str) -> str | None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "select target_dashboard_id from dashboard_map where source_dashboard_id = %s",
                (source_dashboard_id,),
            )
            row = cur.fetchone()
            return row["target_dashboard_id"] if row else None

    def save_dashboard_map(self, source_dashboard_id: str, target_dashboard_id: str, run_id: int) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into dashboard_map (source_dashboard_id, target_dashboard_id, migration_run_id)
                values (%s, %s, %s)
                on conflict (source_dashboard_id) do update
                set target_dashboard_id = excluded.target_dashboard_id,
                    migration_run_id = excluded.migration_run_id
                """,
                (source_dashboard_id, target_dashboard_id, run_id),
            )
            conn.commit()

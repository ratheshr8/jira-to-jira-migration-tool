from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class JiraIssue:
    key: str
    summary: str
    description: dict[str, Any] | None
    issue_type: str
    priority: str | None
    status: str | None
    labels: list[str] = field(default_factory=list)
    assignee_account_id: str | None = None
    due_date: str | None = None
    parent_key: str | None = None
    raw_fields: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JiraComment:
    id: str
    body: dict[str, Any] | None
    author_display_name: str
    author_account_id: str | None
    created: str | None


@dataclass(slots=True)
class JiraAttachment:
    id: str
    filename: str
    content_url: str
    mime_type: str | None
    author_display_name: str | None
    created: str | None

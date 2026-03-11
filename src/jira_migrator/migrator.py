from __future__ import annotations

from io import BytesIO
import re
from typing import Any

from rich.console import Console

from jira_migrator.config import MappingConfig
from jira_migrator.db import Database
from jira_migrator.jira_client import JiraClient, text_to_adf
from jira_migrator.models import JiraComment, JiraIssue


console = Console()


class JiraMigrator:
    def __init__(
        self,
        source: JiraClient,
        target: JiraClient,
        database: Database,
        mapping: MappingConfig,
        default_target_issue_type: str,
    ) -> None:
        self.source = source
        self.target = target
        self.database = database
        self.mapping = mapping
        self.default_target_issue_type = default_target_issue_type

    def validate(self) -> None:
        source_me = self.source.myself()
        target_me = self.target.myself()
        console.print(f"[green]Source OK:[/green] {source_me.get('emailAddress', source_me.get('displayName'))}")
        console.print(f"[green]Target OK:[/green] {target_me.get('emailAddress', target_me.get('displayName'))}")

    def inspect_project(self, project_key: str) -> dict[str, Any]:
        project = self.source.get_project(project_key)
        issues = self.source.iter_issues(project_key)
        statuses = sorted({issue.status for issue in issues if issue.status})
        issue_types = sorted({issue.issue_type for issue in issues if issue.issue_type})
        priorities = sorted({issue.priority for issue in issues if issue.priority})
        return {
            "project": {"key": project.get("key"), "name": project.get("name")},
            "issue_count": len(issues),
            "statuses": statuses,
            "issue_types": issue_types,
            "priorities": priorities,
        }

    def migrate_project(
        self, source_project_key: str, target_project_key: str, *, skip_migrated: bool = False
    ) -> None:
        run_id = self.database.create_run(source_project_key, target_project_key)
        issues = self.source.iter_issues(source_project_key)
        console.print(f"[cyan]Found {len(issues)} source issues[/cyan]")
        processed_issues: list[JiraIssue] = []
        skipped_existing = 0

        for index, issue in enumerate(issues, start=1):
            if skip_migrated and self.database.get_target_issue_key(issue.key):
                console.print(f"[bold]{index}/{len(issues)}[/bold] Skipping {issue.key} (already migrated)")
                skipped_existing += 1
                continue
            console.print(f"[bold]{index}/{len(issues)}[/bold] Migrating {issue.key}")
            target_issue_key = self._ensure_issue(issue, target_project_key, run_id)
            self._migrate_comments(issue, target_issue_key)
            self._migrate_attachments(issue, target_issue_key)
            self._sync_status(issue, target_issue_key)
            processed_issues.append(issue)

        console.print("[cyan]Linking parent-child relationships…[/cyan]")
        issues_to_link = processed_issues if skip_migrated else issues
        self._link_parents(issues, issues_to_link)
        if skip_migrated:
            console.print(f"[green]Skipped already migrated issues: {skipped_existing}[/green]")

    def migrate_filters(
        self, source_project_key: str, target_project_key: str, *, skip_migrated: bool = False
    ) -> None:
        run_id = self.database.create_run(source_project_key, target_project_key)
        source_filters = self.source.list_filters()
        console.print(f"[cyan]Found {len(source_filters)} source filters[/cyan]")
        migrated = skipped = failed = 0

        for source_filter in source_filters:
            source_filter_id = str(source_filter.get("id") or "")
            if not source_filter_id:
                continue
            if skip_migrated and self.database.get_target_filter_id(source_filter_id):
                skipped += 1
                continue

            source_name = str(source_filter.get("name") or f"Migrated Filter {source_filter_id}")
            source_jql = str(source_filter.get("jql") or "")
            if not source_jql:
                skipped += 1
                continue
            target_jql = self._rewrite_filter_jql(source_jql, source_project_key, target_project_key)
            payload: dict[str, Any] = {
                "name": source_name,
                "jql": target_jql,
                "favourite": bool(source_filter.get("favourite", False)),
            }
            description = source_filter.get("description")
            if description:
                payload["description"] = description
            share_permissions = source_filter.get("sharePermissions")
            if isinstance(share_permissions, list):
                payload["sharePermissions"] = share_permissions

            try:
                created = self.target.create_filter(payload)
            except RuntimeError as exc:
                # Most common cross-tenant problem for filters is share permissions.
                if "sharepermissions" in str(exc).lower():
                    payload.pop("sharePermissions", None)
                    created = self.target.create_filter(payload)
                else:
                    failed += 1
                    console.print(
                        f"[yellow]  Could not migrate filter {source_filter_id} ({source_name!r}): {exc}[/yellow]"
                    )
                    continue

            target_filter_id = str(created["id"])
            self.database.save_filter_map(source_filter_id, target_filter_id, run_id)
            migrated += 1

        console.print(
            f"[green]Filters: {migrated} migrated, {skipped} skipped, {failed} failed.[/green]"
        )

    def migrate_dashboards(
        self, source_project_key: str, target_project_key: str, *, skip_migrated: bool = False
    ) -> None:
        run_id = self.database.create_run(source_project_key, target_project_key)
        source_dashboards = self.source.list_dashboards()
        console.print(f"[cyan]Found {len(source_dashboards)} source dashboards[/cyan]")
        migrated = skipped = failed = copied_gadgets = 0

        for source_dashboard in source_dashboards:
            source_dashboard_id = str(source_dashboard.get("id") or "")
            if not source_dashboard_id:
                continue
            if skip_migrated and self.database.get_target_dashboard_id(source_dashboard_id):
                skipped += 1
                continue

            try:
                full_dashboard = self.source.get_dashboard(source_dashboard_id)
            except RuntimeError:
                full_dashboard = source_dashboard

            payload: dict[str, Any] = {
                "name": str(full_dashboard.get("name") or f"Migrated Dashboard {source_dashboard_id}"),
            }
            description = full_dashboard.get("description")
            if description:
                payload["description"] = description
            share_permissions = full_dashboard.get("sharePermissions")
            if isinstance(share_permissions, list):
                payload["sharePermissions"] = share_permissions
            edit_permissions = full_dashboard.get("editPermissions")
            if isinstance(edit_permissions, list):
                payload["editPermissions"] = edit_permissions

            try:
                created_dashboard = self.target.create_dashboard(payload)
            except RuntimeError as exc:
                if "permissions" in str(exc).lower():
                    payload.pop("sharePermissions", None)
                    payload.pop("editPermissions", None)
                    created_dashboard = self.target.create_dashboard(payload)
                else:
                    failed += 1
                    console.print(
                        f"[yellow]  Could not migrate dashboard {source_dashboard_id}: {exc}[/yellow]"
                    )
                    continue

            target_dashboard_id = str(created_dashboard["id"])
            self.database.save_dashboard_map(source_dashboard_id, target_dashboard_id, run_id)
            migrated += 1

            try:
                source_gadgets = self.source.list_dashboard_gadgets(source_dashboard_id)
            except RuntimeError as exc:
                console.print(
                    f"[yellow]  Dashboard {source_dashboard_id}: could not read gadgets: {exc}[/yellow]"
                )
                continue

            for gadget in source_gadgets:
                payload = self._build_dashboard_gadget_payload(
                    gadget, source_project_key, target_project_key
                )
                if not payload:
                    continue
                try:
                    self.target.add_dashboard_gadget(target_dashboard_id, payload)
                    copied_gadgets += 1
                except RuntimeError:
                    # Fall back to module-key-only payload for stricter API validators.
                    module_key = payload.get("moduleKey")
                    if not module_key:
                        continue
                    try:
                        self.target.add_dashboard_gadget(
                            target_dashboard_id,
                            {"moduleKey": module_key},
                        )
                        copied_gadgets += 1
                    except RuntimeError as exc:
                        console.print(
                            f"[yellow]  Could not copy gadget on dashboard {source_dashboard_id}: {exc}[/yellow]"
                        )

        console.print(
            f"[green]Dashboards: {migrated} migrated, {skipped} skipped, {failed} failed; "
            f"gadgets copied: {copied_gadgets}.[/green]"
        )

    def _ensure_issue(self, issue: JiraIssue, target_project_key: str, run_id: int) -> str:
        existing = self.database.get_target_issue_key(issue.key)
        if existing:
            return existing

        issue_type_name = self.mapping.issue_types.get(issue.issue_type, issue.issue_type or self.default_target_issue_type)
        priority_name = self.mapping.priorities.get(issue.priority or "", issue.priority)
        assignee_account_id = self.mapping.users.get(issue.assignee_account_id or "", issue.assignee_account_id)

        fields: dict[str, Any] = {
            "summary": issue.summary,
            "issuetype": {"name": issue_type_name or self.default_target_issue_type},
            "labels": issue.labels,
            "description": self._decorate_description(issue),
        }
        if priority_name:
            fields["priority"] = {"name": priority_name}
        if assignee_account_id:
            fields["assignee"] = {"accountId": assignee_account_id}
        if issue.due_date:
            fields["duedate"] = issue.due_date

        created: dict[str, Any] | None = None
        for _ in range(5):
            try:
                created = self.target.create_issue(target_project_key, fields)
                break
            except RuntimeError as exc:
                error = str(exc).lower()
                if "issuetype" in error:
                    fields["issuetype"] = {"name": self.default_target_issue_type}
                    continue
                if "priority" in error and priority_name:
                    current_priority = fields.get("priority")
                    if isinstance(current_priority, dict):
                        # Some Jira configs expect priority as plain string.
                        fields["priority"] = priority_name
                    else:
                        fields.pop("priority", None)
                        console.print(
                            f"[yellow]Priority {priority_name!r} rejected in target; "
                            f"creating {issue.key} without priority.[/yellow]"
                        )
                    continue
                if "assignee" in error:
                    fields.pop("assignee", None)
                    console.print(
                        f"[yellow]Assignee {assignee_account_id!r} not found in target; "
                        f"creating {issue.key} unassigned.[/yellow]"
                    )
                    continue
                raise
        if created is None:
            raise RuntimeError(f"Could not create {issue.key} after applying field fallbacks.")

        target_issue_key = created["key"]
        self.database.save_issue_map(issue.key, target_issue_key, run_id)
        return target_issue_key

    def _decorate_description(self, issue: JiraIssue) -> dict[str, Any]:
        assignee_name = ((issue.raw_fields.get("assignee") or {}).get("displayName") or "Unassigned")
        created_date = str(issue.raw_fields.get("created") or "unknown")
        source_text = self._adf_to_plain_text(issue.description)
        if source_text:
            return text_to_adf(
                f"Source issue: {issue.key}\n"
                f"Source assignee: {assignee_name}\n"
                f"Source created: {created_date}\n\n"
                f"{source_text}"
            )
        return text_to_adf(
            f"Source issue: {issue.key}\n"
            f"Source assignee: {assignee_name}\n"
            f"Source created: {created_date}"
        )

    def _adf_to_plain_text(self, adf: dict[str, Any] | None) -> str:
        if not adf:
            return ""
        blocks = adf.get("content")
        if not isinstance(blocks, list):
            return ""

        lines: list[str] = []
        for block in blocks:
            block_text = self._extract_text_from_adf_node(block).strip()
            if block_text:
                lines.append(block_text)
        return "\n".join(lines).strip()

    def _extract_text_from_adf_node(self, node: Any) -> str:
        if isinstance(node, list):
            return "".join(self._extract_text_from_adf_node(child) for child in node)
        if not isinstance(node, dict):
            return ""

        node_type = node.get("type")
        if node_type == "text":
            return str(node.get("text") or "")
        if node_type == "hardBreak":
            return "\n"

        content = node.get("content")
        if isinstance(content, list):
            return "".join(self._extract_text_from_adf_node(child) for child in content)
        return ""

    def _migrate_comments(self, source_issue: JiraIssue, target_issue_key: str) -> None:
        for comment in self.source.get_comments(source_issue.key):
            if self.database.comment_exists(comment.id):
                continue
            body = self._decorate_comment(comment)
            created = self.target.add_comment(target_issue_key, body)
            self.database.save_comment_map(comment.id, created["id"], source_issue.key, target_issue_key)

    def _decorate_comment(self, comment: JiraComment) -> dict[str, Any]:
        prefix = (
            f"Source author: {comment.author_display_name}\n"
            f"Source created: {comment.created or 'unknown'}"
        )
        original_text = self._adf_to_plain_text(comment.body)
        if original_text:
            return text_to_adf(f"{prefix}\n\n{original_text}")
        return text_to_adf(prefix)

    def _migrate_attachments(self, source_issue: JiraIssue, target_issue_key: str) -> None:
        for attachment in self.source.get_attachments(source_issue):
            if self.database.attachment_exists(attachment.id):
                continue
            try:
                content = self.source.download_attachment(attachment.content_url)
                uploaded = self.target.upload_attachment(
                    target_issue_key,
                    attachment.filename,
                    BytesIO(content),
                )
                target_attachment_id = uploaded[0]["id"]
                self.database.save_attachment_map(
                    attachment.id,
                    target_attachment_id,
                    source_issue.key,
                    target_issue_key,
                )
            except Exception as exc:
                console.print(
                    f"[yellow]  Attachment {attachment.filename!r} on {source_issue.key} skipped: {exc}[/yellow]"
                )

    def _link_parents(self, all_issues: list[JiraIssue], issues_to_link: list[JiraIssue]) -> None:
        linked = skipped = 0
        issues_by_key = {issue.key: issue for issue in all_issues}
        for issue in issues_to_link:
            if not issue.parent_key:
                continue
            target_child_key = self.database.get_target_issue_key(issue.key)
            target_parent_key = self.database.get_target_issue_key(issue.parent_key)
            if not target_child_key or not target_parent_key:
                console.print(
                    f"[yellow]  Cannot link {issue.key} → {issue.parent_key}: "
                    "one or both not in migration DB.[/yellow]"
                )
                skipped += 1
                continue
            source_parent_issue = issues_by_key.get(issue.parent_key)
            try:
                self.target.set_issue_parent(target_child_key, target_parent_key)
                linked += 1
            except RuntimeError as exc:
                # In Jira classic projects, Epic-to-child is often stored in the Epic Link custom field.
                if source_parent_issue and (source_parent_issue.issue_type or "").lower() == "epic":
                    try:
                        self.target.set_issue_epic_link(target_child_key, target_parent_key)
                        linked += 1
                        continue
                    except RuntimeError as epic_exc:
                        console.print(
                            f"[yellow]  Could not link {target_child_key} to epic "
                            f"{target_parent_key} via parent or Epic Link: {exc}; {epic_exc}[/yellow]"
                        )
                        skipped += 1
                        continue
                console.print(
                    f"[yellow]  Could not link {target_child_key} → parent "
                    f"{target_parent_key}: {exc}[/yellow]"
                )
                skipped += 1
        console.print(f"[green]Parent links: {linked} set, {skipped} skipped.[/green]")

    def _sync_status(self, source_issue: JiraIssue, target_issue_key: str) -> None:
        source_status = source_issue.status
        if not source_status:
            return

        target_status = self.mapping.statuses.get(source_status, source_status)
        transitions = self.target.get_transitions(target_issue_key)
        for transition in transitions:
            to_status = (transition.get("to") or {}).get("name")
            if to_status == target_status:
                self.target.transition_issue(target_issue_key, transition["id"])
                return

        console.print(
            f"[yellow]Could not transition {target_issue_key} to '{target_status}'. "
            "Add or fix a status mapping and rerun.[/yellow]"
        )

    def _rewrite_filter_jql(self, jql: str, source_project_key: str, target_project_key: str) -> str:
        rewritten = jql
        project_key_map = {source_project_key: target_project_key, **self.mapping.project_keys}
        for source_key, target_key in project_key_map.items():
            if not source_key or not target_key:
                continue
            # Keep issue keys valid (e.g. ABC-123 -> SET-123).
            rewritten = re.sub(
                rf"\b{re.escape(source_key)}-(?=\d+\b)",
                f"{target_key}-",
                rewritten,
                flags=re.IGNORECASE,
            )
            # Project-key tokens can appear in clauses like:
            # project = ABC, project in (ABC, DEF), parent = ABC-1 (already handled above).
            # Quote target to avoid JQL reserved-word failures (e.g. SET).
            rewritten = re.sub(
                rf"\b{re.escape(source_key)}\b(?!-\d+\b)",
                f'"{target_key}"',
                rewritten,
                flags=re.IGNORECASE,
            )
        rewritten = self._rewrite_issue_types_in_jql(rewritten)
        return rewritten

    def _rewrite_issue_types_in_jql(self, jql: str) -> str:
        rewritten = jql
        for source_type, target_type in self.mapping.issue_types.items():
            if not source_type or not target_type:
                continue
            # Replace quoted values first.
            rewritten = re.sub(
                rf'"{re.escape(source_type)}"',
                f'"{target_type}"',
                rewritten,
                flags=re.IGNORECASE,
            )
            rewritten = re.sub(
                rf"'{re.escape(source_type)}'",
                f'"{target_type}"',
                rewritten,
                flags=re.IGNORECASE,
            )
            # Replace unquoted standalone values.
            rewritten = re.sub(
                rf"\b{re.escape(source_type)}\b",
                f'"{target_type}"',
                rewritten,
                flags=re.IGNORECASE,
            )
        return rewritten

    def _build_dashboard_gadget_payload(
        self, gadget: dict[str, Any], source_project_key: str, target_project_key: str
    ) -> dict[str, Any] | None:
        module_key = gadget.get("moduleKey")
        uri = gadget.get("uri")
        if not module_key and not uri:
            return None

        payload: dict[str, Any] = {}
        if module_key:
            payload["moduleKey"] = module_key
        if uri:
            payload["uri"] = uri
        if gadget.get("title"):
            payload["title"] = gadget["title"]
        if gadget.get("color"):
            payload["color"] = gadget["color"]
        if gadget.get("position"):
            payload["position"] = gadget["position"]

        for source_key, value in gadget.items():
            if source_key in payload or source_key in {"id", "dashboardId"}:
                continue
            payload[source_key] = self._rewrite_dashboard_config_values(
                source_key,
                value,
                source_project_key,
                target_project_key,
            )
        return payload

    def _rewrite_dashboard_config_values(
        self, key: str, value: Any, source_project_key: str, target_project_key: str
    ) -> Any:
        key_lower = key.lower()
        if isinstance(value, dict):
            return {
                child_key: self._rewrite_dashboard_config_values(
                    str(child_key),
                    child_value,
                    source_project_key,
                    target_project_key,
                )
                for child_key, child_value in value.items()
            }
        if isinstance(value, list):
            return [
                self._rewrite_dashboard_config_values(
                    key,
                    item,
                    source_project_key,
                    target_project_key,
                )
                for item in value
            ]
        if isinstance(value, str):
            if "jql" in key_lower:
                return self._rewrite_filter_jql(value, source_project_key, target_project_key)
            if "filter" in key_lower and value.isdigit():
                mapped_filter_id = self.database.get_target_filter_id(value)
                if mapped_filter_id:
                    return mapped_filter_id
            return value
        if isinstance(value, int) and "filter" in key_lower:
            mapped_filter_id = self.database.get_target_filter_id(str(value))
            if mapped_filter_id and mapped_filter_id.isdigit():
                return int(mapped_filter_id)
        return value

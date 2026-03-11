from __future__ import annotations

import ssl
from typing import Any, BinaryIO

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from jira_migrator.models import JiraAttachment, JiraComment, JiraIssue


def text_to_adf(text: str) -> dict[str, Any]:
    paragraphs = []
    for line in text.splitlines() or [""]:
        paragraph: dict[str, Any] = {"type": "paragraph"}
        if line:
            paragraph["content"] = [{"type": "text", "text": line}]
        else:
            # Empty paragraph must not contain an empty text node.
            paragraph["content"] = []
        paragraphs.append(
            paragraph
        )
    return {"type": "doc", "version": 1, "content": paragraphs}


def _quote_jql_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


class JiraClient:
    def __init__(
        self,
        base_url: str,
        email: str,
        api_token: str,
        *,
        ssl_verify: bool = True,
        ca_bundle: str | None = None,
        trust_env: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        verify: bool | ssl.SSLContext
        if not ssl_verify:
            verify = False
        elif ca_bundle:
            verify = ssl.create_default_context(cafile=ca_bundle)
        else:
            # Prefer the OS trust store so corporate/intercept CAs installed on the
            # machine are honored without needing a separate PEM bundle.
            verify = ssl.create_default_context()

        self.client = httpx.Client(
            base_url=self.base_url,
            auth=(email, api_token),
            timeout=120.0,
            headers={"Accept": "application/json"},
            verify=verify,
            trust_env=trust_env,
        )
        self._epic_link_field_id: str | None = None
        self._did_resolve_epic_link_field = False

    def close(self) -> None:
        self.client.close()

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = self.client.request(method, path, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            response = exc.response
            detail = response.text
            try:
                payload = response.json()
                error_messages = payload.get("errorMessages") or []
                field_errors = payload.get("errors") or {}
                parts = [message for message in error_messages if message]
                parts.extend(f"{key}: {value}" for key, value in field_errors.items())
                if parts:
                    detail = " | ".join(parts)
            except ValueError:
                pass
            raise RuntimeError(
                f"Jira API request failed with HTTP {response.status_code} for {response.request.url}. {detail}"
            ) from exc
        except httpx.ConnectError as exc:
            message = str(exc)
            if "CERTIFICATE_VERIFY_FAILED" in message:
                raise RuntimeError(
                    "TLS certificate verification failed. If your network uses an internal or "
                    "corporate CA, set JIRA_CA_BUNDLE to that CA certificate file. As a last "
                    "resort for testing, set JIRA_SSL_VERIFY=false."
                ) from exc
            raise

    def myself(self) -> dict[str, Any]:
        return self._request("GET", "/rest/api/3/myself").json()

    def get_project(self, project_key: str) -> dict[str, Any]:
        return self._request("GET", f"/rest/api/3/project/{project_key}").json()

    def list_filters(self) -> list[dict[str, Any]]:
        filters: list[dict[str, Any]] = []
        start_at = 0
        while True:
            data = self._request(
                "GET",
                "/rest/api/3/filter/search",
                params={
                    "startAt": start_at,
                    "maxResults": 50,
                    "expand": "description,owner,jql,sharePermissions,favourite",
                },
            ).json()
            batch = data.get("values") or data.get("filters") or []
            if not batch:
                break
            filters.extend(batch)
            start_at += len(batch)
            total = int(data.get("total", 0))
            if total and start_at >= total:
                break
        return filters

    def create_filter(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/rest/api/3/filter", json=payload).json()

    def list_dashboards(self) -> list[dict[str, Any]]:
        dashboards: list[dict[str, Any]] = []
        start_at = 0
        while True:
            data = self._request(
                "GET",
                "/rest/api/3/dashboard/search",
                params={"startAt": start_at, "maxResults": 50},
            ).json()
            batch = data.get("values") or data.get("dashboards") or []
            if not batch:
                break
            dashboards.extend(batch)
            start_at += len(batch)
            total = int(data.get("total", 0))
            if total and start_at >= total:
                break
        return dashboards

    def get_dashboard(self, dashboard_id: str) -> dict[str, Any]:
        return self._request("GET", f"/rest/api/3/dashboard/{dashboard_id}").json()

    def create_dashboard(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/rest/api/3/dashboard", json=payload).json()

    def list_dashboard_gadgets(self, dashboard_id: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/rest/api/3/dashboard/{dashboard_id}/gadget").json()
        return data.get("gadgets") or data.get("items") or data.get("values") or []

    def add_dashboard_gadget(self, dashboard_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/rest/api/3/dashboard/{dashboard_id}/gadget", json=payload).json()

    def search_issues(
        self, project_key: str, next_page_token: str | None = None, max_results: int = 50
    ) -> dict[str, Any]:
        epic_link_field_id = self.get_epic_link_field_id()
        payload: dict[str, Any] = {
            "jql": f"project = {_quote_jql_value(project_key)} ORDER BY created ASC",
            "maxResults": max_results,
            "fields": [
                "summary",
                "description",
                "created",
                "issuetype",
                "priority",
                "status",
                "labels",
                "assignee",
                "duedate",
                "attachment",
                "comment",
                "parent",
            ],
        }
        if epic_link_field_id:
            payload["fields"].append(epic_link_field_id)
        if next_page_token is not None:
            payload["nextPageToken"] = next_page_token
        return self._request("POST", "/rest/api/3/search/jql", json=payload).json()

    def iter_issues(self, project_key: str) -> list[JiraIssue]:
        issues: list[JiraIssue] = []
        next_page_token: str | None = None
        while True:
            data = self.search_issues(project_key, next_page_token=next_page_token)
            batch = data.get("issues", [])
            if not batch:
                break

            for issue in batch:
                fields = issue["fields"]
                parent_key = (fields.get("parent") or {}).get("key") or None
                if not parent_key:
                    parent_key = self._extract_epic_link_parent(fields)
                issues.append(
                    JiraIssue(
                        key=issue["key"],
                        summary=fields.get("summary") or "",
                        description=fields.get("description"),
                        issue_type=(fields.get("issuetype") or {}).get("name") or "Task",
                        priority=(fields.get("priority") or {}).get("name"),
                        status=(fields.get("status") or {}).get("name"),
                        labels=fields.get("labels") or [],
                        assignee_account_id=(fields.get("assignee") or {}).get("accountId"),
                        due_date=fields.get("duedate"),
                        parent_key=parent_key,
                        raw_fields=fields,
                    )
                )

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

        return issues

    def get_comments(self, issue_key: str) -> list[JiraComment]:
        data = self._request(
            "GET",
            f"/rest/api/3/issue/{issue_key}/comment",
            params={"maxResults": 1000},
        ).json()
        comments = []
        for item in data.get("comments", []):
            author = item.get("author") or {}
            comments.append(
                JiraComment(
                    id=item["id"],
                    body=item.get("body"),
                    author_display_name=author.get("displayName") or "Unknown",
                    author_account_id=author.get("accountId"),
                    created=item.get("created"),
                )
            )
        return comments

    def get_attachments(self, issue: JiraIssue) -> list[JiraAttachment]:
        attachments = []
        for item in issue.raw_fields.get("attachment") or []:
            author = item.get("author") or {}
            attachments.append(
                JiraAttachment(
                    id=item["id"],
                    filename=item["filename"],
                    content_url=item["content"],
                    mime_type=item.get("mimeType"),
                    author_display_name=author.get("displayName"),
                    created=item.get("created"),
                )
            )
        return attachments

    def create_issue(self, target_project_key: str, fields: dict[str, Any]) -> dict[str, Any]:
        payload = {"fields": {"project": {"key": target_project_key}, **fields}}
        return self._request("POST", "/rest/api/3/issue", json=payload).json()

    def add_comment(self, issue_key: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/rest/api/3/issue/{issue_key}/comment",
            json={"body": body},
        ).json()

    def get_transitions(self, issue_key: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/rest/api/3/issue/{issue_key}/transitions").json()
        return data.get("transitions", [])

    def transition_issue(self, issue_key: str, transition_id: str) -> None:
        self._request(
            "POST",
            f"/rest/api/3/issue/{issue_key}/transitions",
            json={"transition": {"id": transition_id}},
        )

    def set_issue_parent(self, issue_key: str, parent_key: str) -> None:
        self._request(
            "PUT",
            f"/rest/api/3/issue/{issue_key}",
            json={"fields": {"parent": {"key": parent_key}}},
        )

    def set_issue_epic_link(self, issue_key: str, epic_key: str) -> None:
        epic_link_field_id = self.get_epic_link_field_id()
        if not epic_link_field_id:
            raise RuntimeError("Epic Link custom field was not found in Jira metadata.")
        self._request(
            "PUT",
            f"/rest/api/3/issue/{issue_key}",
            json={"fields": {epic_link_field_id: epic_key}},
        )

    def download_attachment(self, attachment_url: str) -> bytes:
        # Jira Cloud attachment endpoints often respond with 303 and redirect to
        # Atlassian's media CDN for the actual file download.
        response = self.client.get(attachment_url, follow_redirects=True)
        response.raise_for_status()
        return response.content

    def upload_attachment(self, issue_key: str, filename: str, content: BinaryIO) -> list[dict[str, Any]]:
        response = self._request(
            "POST",
            f"/rest/api/3/issue/{issue_key}/attachments",
            files={"file": (filename, content)},
            headers={"X-Atlassian-Token": "no-check"},
        )
        return response.json()

    def get_epic_link_field_id(self) -> str | None:
        if self._did_resolve_epic_link_field:
            return self._epic_link_field_id
        self._did_resolve_epic_link_field = True
        fields = self._request("GET", "/rest/api/3/field").json()
        for field in fields:
            schema = field.get("schema") or {}
            if schema.get("custom") == "com.pyxis.greenhopper.jira:gh-epic-link":
                self._epic_link_field_id = field.get("id")
                break
        return self._epic_link_field_id

    def _extract_epic_link_parent(self, fields: dict[str, Any]) -> str | None:
        epic_link_field_id = self.get_epic_link_field_id()
        if not epic_link_field_id:
            return None
        value = fields.get(epic_link_field_id)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, dict):
            key = value.get("key")
            if isinstance(key, str) and key:
                return key
        return None

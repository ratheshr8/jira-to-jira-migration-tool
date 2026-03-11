from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MappingConfig(BaseModel):
    statuses: dict[str, str] = Field(default_factory=dict)
    users: dict[str, str] = Field(default_factory=dict)
    issue_types: dict[str, str] = Field(default_factory=dict)
    priorities: dict[str, str] = Field(default_factory=dict)
    project_keys: dict[str, str] = Field(default_factory=dict)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    source_jira_base_url: str
    source_jira_email: str
    source_jira_api_token: str

    target_jira_base_url: str
    target_jira_email: str
    target_jira_api_token: str

    database_url: str
    jira_mapping_file: str = "./mapping.json"
    default_target_issue_type: str = "Task"
    jira_ssl_verify: bool = True
    jira_ca_bundle: str | None = None
    jira_trust_env: bool = True

    def load_mapping(self) -> MappingConfig:
        path = Path(self.jira_mapping_file)
        if not path.exists():
            return MappingConfig()

        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return MappingConfig.model_validate(data)


def get_settings() -> Settings:
    return Settings()

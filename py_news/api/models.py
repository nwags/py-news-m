"""Response models for the local-first API surface."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ApiAugmentationMetaResponse(BaseModel):
    augmentation_available: bool = False
    augmentation_types_present: list[str] = Field(default_factory=list)
    last_augmented_at: str | None = None
    augmentation_stale: bool | None = None
    inspect_path: str | None = None


class ArticleSummaryResponse(BaseModel):
    article_id: str
    provider: str
    provider_document_id: str | None = None
    source_name: str | None = None
    source_domain: str | None = None
    url: str | None = None
    canonical_url: str | None = None
    title: str | None = None
    published_at: str | None = None
    language: str | None = None
    section: str | None = None
    byline: str | None = None
    summary_text: str | None = None
    snippet: str | None = None
    imported_at: str | None = None
    resolution_source: str | None = None
    resolution_strategy: str | None = None
    resolution_reason_code: str | None = None
    resolution_status_code: int | None = None
    resolution_message: str | None = None
    resolution_auth_env_var: str | None = None
    resolution_auth_configured: bool | None = None
    resolution_remote_attempted: bool = False
    local_write_performed: bool = False
    resolution_mode: str | None = None
    resolution_provider_requested: str | None = None
    resolution_provider_used: str | None = None
    resolution_served_from: str | None = None
    resolution_persisted_locally: bool | None = None
    resolution_rate_limited: bool = False
    resolution_retry_count: int = 0
    resolution_deferred_until: str | None = None
    augmentation_meta: ApiAugmentationMetaResponse = Field(default_factory=ApiAugmentationMetaResponse)


class ArticlesListResponse(BaseModel):
    items: list[ArticleSummaryResponse]
    count: int
    limit: int
    offset: int


class ArticleContentArtifactResponse(BaseModel):
    artifact_type: str
    artifact_path: str
    exists_locally: bool
    file_exists: bool


class ArticleContentResponse(BaseModel):
    article_id: str
    content_available: bool
    preferred_text: str | None = None
    resolution_source: str | None = None
    resolution_strategy: str | None = None
    resolution_reason_code: str | None = None
    resolution_status_code: int | None = None
    resolution_message: str | None = None
    resolution_auth_env_var: str | None = None
    resolution_auth_configured: bool | None = None
    resolution_remote_attempted: bool = False
    local_write_performed: bool = False
    resolution_mode: str | None = None
    resolution_provider_requested: str | None = None
    resolution_provider_used: str | None = None
    resolution_served_from: str | None = None
    resolution_persisted_locally: bool | None = None
    resolution_rate_limited: bool = False
    resolution_retry_count: int = 0
    resolution_deferred_until: str | None = None
    augmentation_meta: ApiAugmentationMetaResponse = Field(default_factory=ApiAugmentationMetaResponse)
    artifacts: list[ArticleContentArtifactResponse] = Field(default_factory=list)

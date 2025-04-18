# app/models/table.py
from typing import Dict, List, Optional, Union, Any, Literal
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import date
from app.models.namespace import Namespace, PageToken

class TableIdentifier(BaseModel):
    namespace: Namespace
    name: str

class ListTablesResponse(BaseModel):
    next_page_token: Optional[PageToken] = Field(None, alias='next-page-token')
    identifiers: Optional[List[TableIdentifier]] = Field(None, unique_items=True)

# Schema-related models
class StructField(BaseModel):
    id: int
    name: str
    type: Any  # This will be a recursive reference to Type
    required: bool
    doc: Optional[str] = None
    initial_default: Optional[Any] = Field(None, alias='initial-default')
    write_default: Optional[Any] = Field(None, alias='write-default')

class StructType(BaseModel):
    type: Literal["struct"] = "struct"
    fields: List[StructField]

class Schema(StructType):
    schema_id: Optional[int] = Field(None, alias='schema-id')
    identifier_field_ids: Optional[List[int]] = Field(None, alias='identifier-field-ids')

# Partition-related models
class Transform(BaseModel):
    __root__: str

class PartitionField(BaseModel):
    field_id: Optional[int] = Field(None, alias='field-id')
    source_id: int = Field(..., alias='source-id')
    name: str
    transform: Transform

class PartitionSpec(BaseModel):
    spec_id: Optional[int] = Field(None, alias='spec-id')
    fields: List[PartitionField]

# Sort order models
class SortDirection(BaseModel):
    __root__: Literal["asc", "desc"]

class NullOrder(BaseModel):
    __root__: Literal["nulls-first", "nulls-last"]

class SortField(BaseModel):
    source_id: int = Field(..., alias='source-id')
    transform: Transform
    direction: SortDirection
    null_order: NullOrder = Field(..., alias='null-order')

class SortOrder(BaseModel):
    order_id: int = Field(..., alias='order-id')
    fields: List[SortField]

# Snapshot models
class Summary(BaseModel):
    operation: Literal["append", "replace", "overwrite", "delete"]

class Snapshot(BaseModel):
    snapshot_id: int = Field(..., alias='snapshot-id')
    parent_snapshot_id: Optional[int] = Field(None, alias='parent-snapshot-id')
    sequence_number: Optional[int] = Field(None, alias='sequence-number')
    timestamp_ms: int = Field(..., alias='timestamp-ms')
    manifest_list: str = Field(..., alias='manifest-list', description="Location of the snapshot's manifest list file")
    summary: Summary
    schema_id: Optional[int] = Field(None, alias='schema-id')

class SnapshotReference(BaseModel):
    type: Literal["tag", "branch"]
    snapshot_id: int = Field(..., alias='snapshot-id')
    max_ref_age_ms: Optional[int] = Field(None, alias='max-ref-age-ms')
    max_snapshot_age_ms: Optional[int] = Field(None, alias='max-snapshot-age-ms')
    min_snapshots_to_keep: Optional[int] = Field(None, alias='min-snapshots-to-keep')

class TableMetadata(BaseModel):
    format_version: int = Field(..., alias='format-version', ge=1, le=2)
    table_uuid: str = Field(..., alias='table-uuid')
    location: Optional[str] = None
    last_updated_ms: Optional[int] = Field(None, alias='last-updated-ms')
    properties: Optional[Dict[str, str]] = None
    schemas: Optional[List[Schema]] = None
    current_schema_id: Optional[int] = Field(None, alias='current-schema-id')
    last_column_id: Optional[int] = Field(None, alias='last-column-id')
    partition_specs: Optional[List[PartitionSpec]] = Field(None, alias='partition-specs')
    default_spec_id: Optional[int] = Field(None, alias='default-spec-id')
    last_partition_id: Optional[int] = Field(None, alias='last-partition-id')
    sort_orders: Optional[List[SortOrder]] = Field(None, alias='sort-orders')
    default_sort_order_id: Optional[int] = Field(None, alias='default-sort-order-id')
    snapshots: Optional[List[Snapshot]] = None
    refs: Optional[Dict[str, SnapshotReference]] = None
    current_snapshot_id: Optional[int] = Field(None, alias='current-snapshot-id')
    last_sequence_number: Optional[int] = Field(None, alias='last-sequence-number')

# Table creation and modification models
class CreateTableRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: Optional[PartitionSpec] = Field(None, alias='partition-spec')
    write_order: Optional[SortOrder] = Field(None, alias='write-order')
    stage_create: Optional[bool] = None
    properties: Optional[Dict[str, str]] = None

class RegisterTableRequest(BaseModel):
    name: str
    metadata_location: str = Field(..., alias='metadata-location')
    overwrite: Optional[bool] = Field(False, description="Whether to overwrite table metadata if the table already exists")

class StorageCredential(BaseModel):
    prefix: str = Field(..., description="Indicates a storage location prefix where the credential is relevant.")
    config: Dict[str, str]

class LoadTableResult(BaseModel):
    metadata_location: Optional[str] = Field(None, alias='metadata-location', description="May be null if the table is staged as part of a transaction")
    metadata: TableMetadata
    config: Optional[Dict[str, str]] = None
    storage_credentials: Optional[List[StorageCredential]] = Field(None, alias='storage-credentials')

class LoadCredentialsResponse(BaseModel):
    storage_credentials: List[StorageCredential] = Field(..., alias='storage-credentials')

# Table update models
class TableRequirement(BaseModel):
    type: str

class TableUpdate(BaseModel):
    action: str



class CommitTableRequest(BaseModel):
    identifier: Optional[TableIdentifier] = Field(None, description="Table identifier to update; must be present for CommitTransactionRequest")
    requirements: List[TableRequirement]
    updates: List[TableUpdate]

class CommitTransactionRequest(BaseModel):
    table_changes: List[CommitTableRequest] = Field(..., alias='table-changes')

class CommitTableResponse(BaseModel):
    metadata_location: str = Field(..., alias='metadata-location')
    metadata: TableMetadata

# Scan planning models
class PlanStatus(BaseModel):
    __root__: Literal["completed", "submitted", "cancelled", "failed"] = Field(..., description="Status of a server-side planning operation")

class FileScanTask(BaseModel):
    data_file: Any  # Simplified for brevity
    delete_file_references: Optional[List[int]] = Field(None, alias='delete-file-references')
    residual_filter: Optional[Any] = Field(None, alias='residual-filter')

class PlanTask(BaseModel):
    __root__: str = Field(..., description="An opaque string representing a unit of work for scan planning")

class ScanTasks(BaseModel):
    delete_files: Optional[List[Any]] = Field(None, alias='delete-files')
    file_scan_tasks: Optional[List[FileScanTask]] = Field(None, alias='file-scan-tasks')
    plan_tasks: Optional[List[PlanTask]] = Field(None, alias='plan-tasks')

# Table metrics models
class CounterResult(BaseModel):
    unit: str
    value: int

class TimerResult(BaseModel):
    time_unit: str = Field(..., alias='time-unit')
    count: int
    total_duration: int = Field(..., alias='total-duration')

class MetricResult(BaseModel):
    __root__: Union[CounterResult, TimerResult]

class Metrics(BaseModel):
    __root__: Optional[Dict[str, MetricResult]] = None

class ReportMetricsRequest(BaseModel):
    table_name: str = Field(..., alias='table-name')
    snapshot_id: int = Field(..., alias='snapshot-id')
    report_type: str = Field(..., alias='report-type')
    metrics: Metrics
    metadata: Optional[Dict[str, str]] = None

# Rename table model
class RenameTableRequest(BaseModel):
    source: TableIdentifier
    destination: TableIdentifier


class TableCredential(BaseModel):
    config: Dict[str, str] = Field(..., description="Credential configuration")

class CreateTableRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schema_: Schema = Field(..., alias='schema')
    partition_spec: Optional[PartitionSpec] = Field(None, alias='partition-spec')
    write_order: Optional[SortOrder] = Field(None, alias='write-order')
    stage_create: Optional[bool] = None
    properties: Optional[Dict[str, str]] = None
    credentials: Optional[TableCredential] = None  # Add this field
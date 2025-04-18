# app/services/table.py
import json
import time
import uuid
from typing import Dict, List, Optional, Union, Any
from app.database import db
from app.models.namespace import Namespace
from app.models.table import (
    TableIdentifier, ListTablesResponse, CreateTableRequest, RegisterTableRequest,
    LoadTableResult, CommitTableRequest, CommitTableResponse, StorageCredential,
    LoadCredentialsResponse, TableMetadata, PageToken, Schema, Snapshot, Summary,
    PartitionSpec, SortOrder, ReportMetricsRequest, RenameTableRequest
)
from app.services.namespace import NamespaceService
from app.utils.logger import logger
import base64

class TableService:
    
    @staticmethod
    def encode_page_token(value: str) -> str:
        """Encode a page token value"""
        return base64.b64encode(value.encode()).decode()
    
    @staticmethod
    def decode_page_token(token: str) -> str:
        """Decode a page token value"""
        return base64.b64decode(token.encode()).decode()
    
    @staticmethod
    async def list_tables(
        namespace_levels: List[str],
        page_token: Optional[str] = None,
        page_size: Optional[int] = None
    ) -> ListTablesResponse:
        """
        List all table identifiers under a given namespace.
        """
        logger.info(f"Listing tables in namespace: {namespace_levels}")
        
        # Verify namespace exists
        namespace_exists = await NamespaceService.namespace_exists(namespace_levels)
        if not namespace_exists:
            logger.warning(f"Namespace not found: {namespace_levels}")
            raise ValueError(f"Namespace not found: {namespace_levels}")
        
        # Get namespace ID
        query = """
        SELECT id FROM namespaces WHERE levels = $1
        """
        namespace_record = await db.fetch_one(query, namespace_levels)
        namespace_id = namespace_record["id"]
        
        # Build query for tables
        tables_query = """
        SELECT name FROM tables 
        WHERE namespace_id = $1
        """
        params = [namespace_id]
        
        # Handle page token
        if page_token:
            try:
                last_seen = TableService.decode_page_token(page_token)
                tables_query += " AND name > $2"
                params.append(last_seen)
                logger.debug(f"Using page token, starting after: {last_seen}")
            except Exception as e:
                logger.error(f"Invalid page token: {page_token}", exc_info=True)
                raise ValueError(f"Invalid page token: {page_token}")
        
        # Add ordering
        tables_query += " ORDER BY name"
        
        # Add limit for pagination
        if page_size:
            # Request one more than needed to check if there are more results
            tables_query += f" LIMIT ${len(params) + 1}"
            params.append(page_size + 1)
            logger.debug(f"Using page size: {page_size}")
        
        # Execute query
        try:
            logger.debug(f"Executing query: {tables_query}")
            table_records = await db.fetch_all(tables_query, *params)
            
            # Handle pagination
            has_more = False
            if page_size and len(table_records) > page_size:
                has_more = True
                table_records = table_records[:page_size]
            
            # Convert to model
            identifiers = [
                TableIdentifier(
                    namespace=Namespace(__root__=namespace_levels),
                    name=record["name"]
                ) for record in table_records
            ]
            
            logger.info(f"Found {len(identifiers)} tables in namespace {namespace_levels}")
            
            # Build response
            response = ListTablesResponse(identifiers=identifiers)
            
            # Add next page token if there are more results
            if has_more:
                last_table = table_records[-1]["name"]
                next_token = TableService.encode_page_token(last_table)
                response.next_page_token = PageToken(__root__=next_token)
                logger.debug(f"More tables exist, generated next page token")
            
            return response
            
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def table_exists(namespace_levels: List[str], table_name: str) -> bool:
        """
        Check if a table exists within a namespace.
        """
        logger.info(f"Checking if table exists: {namespace_levels}.{table_name}")
        
        query = """
        SELECT EXISTS(
            SELECT 1 FROM tables t
            JOIN namespaces n ON t.namespace_id = n.id
            WHERE n.levels = $1 AND t.name = $2
        )
        """
        
        try:
            result = await db.fetch_one(query, namespace_levels, table_name)
            exists = result and result["exists"]
            logger.info(f"Table {namespace_levels}.{table_name} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def create_table(
        namespace_levels: List[str], 
        request: CreateTableRequest,
        x_iceberg_access_delegation: Optional[str] = None
    ) -> LoadTableResult:
        """
        Create a new table in the given namespace.
        """
        logger.info(f"Creating table {request.name} in namespace {namespace_levels}")
        
        # Verify namespace exists
        namespace_exists = await NamespaceService.namespace_exists(namespace_levels)
        if not namespace_exists:
            logger.warning(f"Namespace not found: {namespace_levels}")
            raise ValueError(f"Namespace not found: {namespace_levels}")
        
        # Check if table already exists
        table_exists = await TableService.table_exists(namespace_levels, request.name)
        if table_exists:
            logger.warning(f"Table already exists: {namespace_levels}.{request.name}")
            raise ValueError(f"Table already exists: {namespace_levels}.{request.name}")
        
        # Get namespace ID
        query = """
        SELECT id FROM namespaces WHERE levels = $1
        """
        namespace_record = await db.fetch_one(query, namespace_levels)
        namespace_id = namespace_record["id"]
        
        # Generate table UUID and other metadata
        table_uuid = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        format_version = 2  # Default to the latest version
        
        # Define warehouse location if not provided
        location = request.location
        if not location:
            # Default location based on namespace and table name
            location = f"s3://default-warehouse/{'.'.join(namespace_levels)}/{request.name}"
            logger.debug(f"Using default location: {location}")
        
        try:
            async with db.transaction():
                # Process schema
                schema_json = json.loads(request.schema_.json(by_alias=True))
                schema_id = 0  # Initial schema ID
                
                # Calculate last column ID based on schema
                last_column_id = 0
                for field in request.schema_.fields:
                    if field.id > last_column_id:
                        last_column_id = field.id
                
                # Process partition spec
                partition_spec_json = None
                spec_id = 0
                last_partition_id = 0
                
                if request.partition_spec:
                    partition_spec_json = json.loads(request.partition_spec.json(by_alias=True))
                    for field in request.partition_spec.fields:
                        if field.field_id and field.field_id > last_partition_id:
                            last_partition_id = field.field_id
                else:
                    # Create empty default partition spec
                    partition_spec_json = {"spec-id": 0, "fields": []}
                
                # Process sort order
                sort_order_json = None
                sort_order_id = 0
                
                if request.write_order:
                    sort_order_json = json.loads(request.write_order.json(by_alias=True))
                    sort_order_id = request.write_order.order_id
                else:
                    # Create empty default sort order
                    sort_order_json = {"order-id": 0, "fields": []}
                
                # Convert properties
                properties = request.properties or {}
                
                # Insert table record
                table_insert_query = """
                INSERT INTO tables (
                    namespace_id, name, table_uuid, location, 
                    last_updated_ms, last_column_id, schema_id, 
                    current_schema_id, default_spec_id, last_partition_id,
                    default_sort_order_id, properties, format_version
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                ) RETURNING id
                """
                
                table_record = await db.fetch_one(
                    table_insert_query,
                    namespace_id, request.name, table_uuid, location,
                    now_ms, last_column_id, schema_id, schema_id, spec_id,
                    last_partition_id, sort_order_id, json.dumps(properties),
                    format_version
                )
                
                table_id = table_record["id"]
                logger.debug(f"Created table record with ID: {table_id}")
                
                # Insert schema
                schema_insert_query = """
                INSERT INTO schemas (table_id, schema_id, schema_json)
                VALUES ($1, $2, $3)
                """
                await db.execute(schema_insert_query, table_id, schema_id, json.dumps(schema_json))
                logger.debug(f"Added schema {schema_id} to table {table_id}")
                
                # Insert partition spec
                spec_insert_query = """
                INSERT INTO partition_specs (table_id, spec_id, spec_json)
                VALUES ($1, $2, $3)
                """
                await db.execute(spec_insert_query, table_id, spec_id, json.dumps(partition_spec_json))
                logger.debug(f"Added partition spec {spec_id} to table {table_id}")
                
                # Insert sort order
                order_insert_query = """
                INSERT INTO sort_orders (table_id, order_id, order_json)
                VALUES ($1, $2, $3)
                """
                await db.execute(order_insert_query, table_id, sort_order_id, json.dumps(sort_order_json))
                logger.debug(f"Added sort order {sort_order_id} to table {table_id}")
                
                # Prepare response metadata
                table_metadata = TableMetadata(
                    format_version=format_version,
                    table_uuid=table_uuid,
                    location=location,
                    last_updated_ms=now_ms,
                    properties=properties,
                    schemas=[request.schema_],
                    current_schema_id=schema_id,
                    last_column_id=last_column_id,
                    partition_specs=[request.partition_spec] if request.partition_spec else [PartitionSpec(spec_id=0, fields=[])],
                    default_spec_id=spec_id,
                    last_partition_id=last_partition_id,
                    sort_orders=[request.write_order] if request.write_order else [SortOrder(order_id=0, fields=[])],
                    default_sort_order_id=sort_order_id,
                    snapshots=[],
                    refs={},
                    current_snapshot_id=None,
                    last_sequence_number=0
                )
                
                # Generate metadata location
                metadata_location = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"
                logger.info(f"Created table {request.name} in namespace {namespace_levels} with UUID {table_uuid}")
                
                # Get table configuration
                config = await TableService.get_table_config(table_id)
                
                # Get storage credentials if requested
                storage_credentials = None
                if x_iceberg_access_delegation:
                    storage_credentials = await TableService.get_storage_credentials(table_id)
                
                return LoadTableResult(
                    metadata_location=metadata_location,
                    metadata=table_metadata,
                    config=config,
                    storage_credentials=storage_credentials
                )
                
        except ValueError:
            # Re-raise ValueError for not found or table exists
            raise
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def get_table_config(table_id: int) -> Dict[str, str]:
        """
        Get table-specific configuration.
        """
        # In a real implementation, this would fetch config from a database
        # For now, return some default configuration
        return {
            "client.region": "us-west-2",
            "s3.access-key-id": "XXX", 
            "s3.secret-access-key": "XXX"
        }
    
    @staticmethod
    async def get_storage_credentials(table_id: int) -> List[StorageCredential]:
        """
        Get storage credentials for a table.
        """
        # In a real implementation, this would fetch credentials from a database
        # For now, return some example credentials
        return [
            StorageCredential(
                prefix="s3://",
                config={
                    "region": "us-west-2",
                    "access-key-id": "XXX",
                    "secret-access-key": "XXX"
                }
            )
        ]
    
    @staticmethod
    async def load_table(
        namespace_levels: List[str],
        table_name: str,
        snapshots: Optional[str] = None,
        x_iceberg_access_delegation: Optional[str] = None,
        if_none_match: Optional[str] = None
    ) -> LoadTableResult:
        """
        Load a table's metadata.
        """
        logger.info(f"Loading table {namespace_levels}.{table_name}")
        
        # Build query to get table details
        query = """
        SELECT t.id, t.table_uuid, t.location, t.current_snapshot_id, t.last_sequence_number,
               t.last_updated_ms, t.last_column_id, t.schema_id, t.current_schema_id,
               t.default_spec_id, t.last_partition_id, t.default_sort_order_id,
               t.properties, t.format_version, t.row_lineage, t.next_row_id
        FROM tables t
        JOIN namespaces n ON t.namespace_id = n.id
        WHERE n.levels = $1 AND t.name = $2
        """
        
        try:
            # Check if table exists
            table_record = await db.fetch_one(query, namespace_levels, table_name)
            
            if not table_record:
                logger.warning(f"Table not found: {namespace_levels}.{table_name}")
                raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
            
            table_id = table_record["id"]
            logger.debug(f"Found table with ID: {table_id}")
            
            # Generate ETag from table_uuid and last_updated_ms
            table_uuid = table_record["table_uuid"]
            last_updated_ms = table_record["last_updated_ms"]
            etag = f'"{table_uuid}-{last_updated_ms}"'
            
            # Check If-None-Match header
            if if_none_match and if_none_match == etag:
                logger.info(f"Table {namespace_levels}.{table_name} not modified, returning 304")
                return None  # Signal to the router to return 304 Not Modified
            
            # Fetch schemas
            schemas_query = """
            SELECT schema_id, schema_json FROM schemas WHERE table_id = $1
            """
            schema_records = await db.fetch_all(schemas_query, table_id)
            schemas = []
            
            for record in schema_records:
                schema_json = record["schema_json"]
                if isinstance(schema_json, str):
                    schema_json = json.loads(schema_json)
                schema = Schema.parse_obj(schema_json)
                schemas.append(schema)
            
            # Fetch partition specs
            specs_query = """
            SELECT spec_id, spec_json FROM partition_specs WHERE table_id = $1
            """
            spec_records = await db.fetch_all(specs_query, table_id)
            partition_specs = []
            
            for record in spec_records:
                spec_json = record["spec_json"]
                if isinstance(spec_json, str):
                    spec_json = json.loads(spec_json)
                spec = PartitionSpec.parse_obj(spec_json)
                partition_specs.append(spec)
            
            # Fetch sort orders
            orders_query = """
            SELECT order_id, order_json FROM sort_orders WHERE table_id = $1
            """
            order_records = await db.fetch_all(orders_query, table_id)
            sort_orders = []
            
            for record in order_records:
                order_json = record["order_json"]
                if isinstance(order_json, str):
                    order_json = json.loads(order_json)
                order = SortOrder.parse_obj(order_json)
                sort_orders.append(order)
            
            # Fetch snapshots
            snapshots_query = """
            SELECT snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
                   manifest_list, summary, schema_id
            FROM snapshots
            WHERE table_id = $1
            """
            
            # Add filtering based on snapshots parameter
            if snapshots == "refs":
                snapshots_query += """
                AND snapshot_id IN (SELECT snapshot_id FROM snapshot_refs WHERE table_id = $1)
                """
            
            snapshot_records = await db.fetch_all(snapshots_query, table_id)
            snapshots_list = []
            
            for record in snapshot_records:
                summary_json = record["summary"]
                if isinstance(summary_json, str):
                    summary_json = json.loads(summary_json)
                
                snapshot = Snapshot(
                    snapshot_id=record["snapshot_id"],
                    parent_snapshot_id=record["parent_snapshot_id"],
                    sequence_number=record["sequence_number"],
                    timestamp_ms=record["timestamp_ms"],
                    manifest_list=record["manifest_list"],
                    summary=Summary.parse_obj(summary_json),
                    schema_id=record["schema_id"]
                )
                snapshots_list.append(snapshot)
            
            # Fetch snapshot references
            refs_query = """
            SELECT name, snapshot_id, type, min_snapshots_to_keep,
                   max_snapshot_age_ms, max_ref_age_ms
            FROM snapshot_refs
            WHERE table_id = $1
            """
            ref_records = await db.fetch_all(refs_query, table_id)
            refs = {}
            
            for record in ref_records:
                ref = {
                    "type": record["type"],
                    "snapshot-id": record["snapshot_id"],
                }
                
                if record["min_snapshots_to_keep"] is not None:
                    ref["min-snapshots-to-keep"] = record["min_snapshots_to_keep"]
                
                if record["max_snapshot_age_ms"] is not None:
                    ref["max-snapshot-age-ms"] = record["max_snapshot_age_ms"]
                    
                if record["max_ref_age_ms"] is not None:
                    ref["max-ref-age-ms"] = record["max_ref_age_ms"]
                
                refs[record["name"]] = ref
            
            # Handle properties
            properties = table_record["properties"]
            if isinstance(properties, str):
                properties = json.loads(properties)
            
            # Construct table metadata
            table_metadata = TableMetadata(
                format_version=table_record["format_version"],
                table_uuid=table_record["table_uuid"],
                location=table_record["location"],
                last_updated_ms=table_record["last_updated_ms"],
                properties=properties,
                schemas=schemas,
                current_schema_id=table_record["current_schema_id"],
                last_column_id=table_record["last_column_id"],
                partition_specs=partition_specs,
                default_spec_id=table_record["default_spec_id"],
                last_partition_id=table_record["last_partition_id"],
                sort_orders=sort_orders,
                default_sort_order_id=table_record["default_sort_order_id"],
                snapshots=snapshots_list,
                refs=refs,
                current_snapshot_id=table_record["current_snapshot_id"],
                last_sequence_number=table_record["last_sequence_number"]
            )
            
            # Generate metadata location
            metadata_location = f"{table_record['location']}/metadata/current.metadata.json"
            logger.info(f"Loaded table {namespace_levels}.{table_name}")
            
            # Get table configuration
            config = await TableService.get_table_config(table_id)
            
            # Get storage credentials if requested
            storage_credentials = None
            if x_iceberg_access_delegation:
                storage_credentials = await TableService.get_storage_credentials(table_id)
            
            return LoadTableResult(
                metadata_location=metadata_location,
                metadata=table_metadata,
                config=config,
                storage_credentials=storage_credentials
            ), etag
            
        except ValueError:
            # Re-raise ValueError for not found
            raise
        except Exception as e:
            logger.error(f"Error loading table: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def drop_table(
        namespace_levels: List[str],
        table_name: str,
        purge_requested: bool = False
    ) -> None:
        """
        Drop a table from the catalog.
        """
        logger.info(f"Dropping table {namespace_levels}.{table_name}, purge_requested: {purge_requested}")
        
        # Get namespace ID
        namespace_query = """
        SELECT id FROM namespaces WHERE levels = $1
        """
        
        try:
            namespace_record = await db.fetch_one(namespace_query, namespace_levels)
            
            if not namespace_record:
                logger.warning(f"Namespace not found: {namespace_levels}")
                raise ValueError(f"Namespace not found: {namespace_levels}")
            
            namespace_id = namespace_record["id"]
            
            # Check if table exists and get its location
            table_query = """
            SELECT id, location FROM tables WHERE namespace_id = $1 AND name = $2
            """
            table_record = await db.fetch_one(table_query, namespace_id, table_name)
            
            if not table_record:
                logger.warning(f"Table not found: {namespace_levels}.{table_name}")
                raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
            
            table_id = table_record["id"]
            location = table_record["location"]
            
            # Delete the table (cascade will delete related records)
            delete_query = """
            DELETE FROM tables WHERE id = $1
            """
            await db.execute(delete_query, table_id)
            
            logger.info(f"Dropped table {namespace_levels}.{table_name}")
            
            # If purge is requested, we would clean up data files here
            if purge_requested:
                logger.info(f"Purge requested for table {namespace_levels}.{table_name} at location {location}")
                # In a real implementation, this would schedule a data purge job
                pass
                
        except ValueError:
            # Re-raise ValueError for not found
            raise
        except Exception as e:
            logger.error(f"Error dropping table: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def load_credentials(
        namespace_levels: List[str],
        table_name: str
    ) -> LoadCredentialsResponse:
        """
        Load credentials for a table from the catalog.
        """
        logger.info(f"Loading credentials for table {namespace_levels}.{table_name}")
        
        # Check if table exists and get its location
        query = """
        SELECT t.id, t.location FROM tables t
        JOIN namespaces n ON t.namespace_id = n.id
        WHERE n.levels = $1 AND t.name = $2
        """
        table_record = await db.fetch_one(query, namespace_levels, table_name)
        
        if not table_record:
            logger.warning(f"Table not found: {namespace_levels}.{table_name}")
            raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
        
        table_id = table_record["id"]
        location = table_record["location"]
        
        # Fetch credentials from storage_credentials table
        # First try table-specific credentials
        query = """
        SELECT prefix, config FROM storage_credentials
        WHERE table_id = $1 OR
            $2 LIKE CONCAT(prefix, '%')
        ORDER BY LENGTH(prefix) DESC
        """
        
        credential_records = await db.fetch_all(query, table_id, location)
        
        # Convert results to StorageCredential models
        credentials = []
        for record in credential_records:
            config = record["config"]
            if isinstance(config, str):
                config = json.loads(config)
            
            credentials.append(
                StorageCredential(
                    prefix=record["prefix"],
                    config=config
                )
            )
        
        logger.info(f"Loaded {len(credentials)} credential(s) for table {namespace_levels}.{table_name}")
        return LoadCredentialsResponse(storage_credentials=credentials)
    
    @staticmethod
    async def rename_table(
        request: RenameTableRequest
    ) -> None:
        """
        Rename a table from one name to another, possibly in a different namespace.
        """
        source_namespace = request.source.namespace.__root__
        source_name = request.source.name
        destination_namespace = request.destination.namespace.__root__
        destination_name = request.destination.name
        
        logger.info(f"Renaming table {source_namespace}.{source_name} to {destination_namespace}.{destination_name}")
        
        try:
            # Verify source namespace exists
            source_namespace_exists = await NamespaceService.namespace_exists(source_namespace)
            if not source_namespace_exists:
                logger.warning(f"Source namespace not found: {source_namespace}")
                raise ValueError(f"Source namespace not found: {source_namespace}")
            
            # Verify destination namespace exists
            destination_namespace_exists = await NamespaceService.namespace_exists(destination_namespace)
            if not destination_namespace_exists:
                logger.warning(f"Destination namespace not found: {destination_namespace}")
                raise ValueError(f"Destination namespace not found: {destination_namespace}")
            
            # Verify source table exists
            source_table_exists = await TableService.table_exists(source_namespace, source_name)
            if not source_table_exists:
                logger.warning(f"Source table not found: {source_namespace}.{source_name}")
                raise ValueError(f"Source table not found: {source_namespace}.{source_name}")
            
            # Verify destination table does not exist
            destination_table_exists = await TableService.table_exists(destination_namespace, destination_name)
            if destination_table_exists:
                logger.warning(f"Destination table already exists: {destination_namespace}.{destination_name}")
                raise ValueError(f"Destination table already exists: {destination_namespace}.{destination_name}")
            
            # Get namespace IDs
            source_namespace_query = """
            SELECT id FROM namespaces WHERE levels = $1
            """
            source_namespace_record = await db.fetch_one(source_namespace_query, source_namespace)
            source_namespace_id = source_namespace_record["id"]
            
            destination_namespace_query = """
            SELECT id FROM namespaces WHERE levels = $1
            """
            destination_namespace_record = await db.fetch_one(destination_namespace_query, destination_namespace)
            destination_namespace_id = destination_namespace_record["id"]
            
            # Update table record
            update_query = """
            UPDATE tables 
            SET namespace_id = $1, name = $2, updated_at = NOW()
            WHERE namespace_id = $3 AND name = $4
            """
            
            await db.execute(update_query, destination_namespace_id, destination_name, source_namespace_id, source_name)
            
            logger.info(f"Successfully renamed table {source_namespace}.{source_name} to {destination_namespace}.{destination_name}")
            
        except ValueError:
            # Re-raise ValueError for not found or table exists
            raise
        except Exception as e:
            logger.error(f"Error renaming table: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def report_metrics(
        namespace_levels: List[str],
        table_name: str,
        request: ReportMetricsRequest
    ) -> None:
        """
        Submit metrics about table operations.
        """
        logger.info(f"Reporting metrics for table {namespace_levels}.{table_name}")
        
        # Check if table exists
        table_exists = await TableService.table_exists(namespace_levels, table_name)
        if not table_exists:
            logger.warning(f"Table not found: {namespace_levels}.{table_name}")
            raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
        
        # Get table ID
        query = """
        SELECT t.id FROM tables t
        JOIN namespaces n ON t.namespace_id = n.id
        WHERE n.levels = $1 AND t.name = $2
        """
        table_record = await db.fetch_one(query, namespace_levels, table_name)
        table_id = table_record["id"]
        
        # Store metrics in database
        metrics_json = json.loads(request.metrics.json(by_alias=True))
        metadata_json = request.metadata or {}
        
        # Determine the type of metrics report (scan or commit) and store appropriately
        if hasattr(request, 'filter') and hasattr(request, 'schema_id'):
            # This is a scan report
            insert_query = """
            INSERT INTO operation_metrics (
                table_id, report_type, snapshot_id, filter_json, 
                schema_id, projected_field_ids, projected_field_names,
                metrics_json, metadata_json, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            """
            # Convert filter to JSON if present
            filter_json = None
            if hasattr(request, 'filter') and request.filter:
                filter_json = json.loads(request.filter.json(by_alias=True))
                
            await db.execute(
                insert_query, 
                table_id, 
                request.report_type, 
                request.snapshot_id, 
                json.dumps(filter_json) if filter_json else None,
                getattr(request, 'schema_id', None),
                getattr(request, 'projected_field_ids', None),
                getattr(request, 'projected_field_names', None),
                json.dumps(metrics_json),
                json.dumps(metadata_json) if metadata_json else None
            )
        else:
            # This is a commit report
            insert_query = """
            INSERT INTO operation_metrics (
                table_id, report_type, snapshot_id, sequence_number,
                operation, metrics_json, metadata_json, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            """
            await db.execute(
                insert_query, 
                table_id, 
                request.report_type, 
                request.snapshot_id,
                getattr(request, 'sequence_number', None),
                getattr(request, 'operation', None),
                json.dumps(metrics_json),
                json.dumps(metadata_json) if metadata_json else None
            )
            
        logger.info(f"Recorded metrics for table {namespace_levels}.{table_name}, report type: {request.report_type}")
# app/services/table.py
import json
import time
import uuid
from typing import Dict, List, Optional, Union, Any, Tuple
from app.database import db
from app.models.namespace import Namespace
from app.models.table import (
    TableIdentifier, ListTablesResponse, CreateTableRequest, RegisterTableRequest,
    LoadTableResult, CommitTableRequest, CommitTableResponse, StorageCredential,
    LoadCredentialsResponse, TableMetadata, PageToken, Schema, Snapshot, Summary,
    PartitionSpec, SortOrder, ReportMetricsRequest, RenameTableRequest, 
    TableRequirement, CommitTransactionRequest
)
from app.services.namespace import NamespaceService
from app.utils.logger import logger
import base64
from app.services.credential import CredentialService
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
    async def get_default_warehouse_location() -> str:
        """Get the default warehouse location from catalog config."""
        try:
            query = """
            SELECT config_json->'defaults'->>'warehouse.location' as warehouse_location
            FROM catalog_config
            LIMIT 1
            """
            result = await db.fetch_one(query)
            
            if result and result["warehouse_location"]:
                warehouse_location = result["warehouse_location"]
                logger.debug(f"Using configured warehouse location: {warehouse_location}")
                return warehouse_location
            
            # Fallback to default if not configured
            logger.warning("Default warehouse location not configured, using fallback value")
            return "s3://default-warehouse"
        except Exception as e:
            logger.error(f"Error fetching default warehouse location: {str(e)}", exc_info=True)
            return "s3://default-warehouse"  # Fallback to default

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
            # # Default location based on namespace and table name
            # location = f"s3://default-warehouse/{'.'.join(namespace_levels)}/{request.name}"
            # logger.debug(f"Using default location: {location}")
            # Get default warehouse location from config
            default_warehouse = await TableService.get_default_warehouse_location()
            location = f"{default_warehouse}/{'.'.join(namespace_levels)}/{request.name}"
            logger.debug(f"Using default location: {location}")
        try:
            async with db.transaction():
                # Process schema - Ensure schema_id is properly set
                schema_json = json.loads(request.schema_.json(by_alias=True))
                schema_id = 0  # Initial schema ID
                
                # Add schema_id to schema_json if not already present
                if "schema-id" not in schema_json:
                    schema_json["schema-id"] = schema_id
                
                # Check for identifier fields (primary keys)
                identifier_field_ids = []
                for field in request.schema_.fields:
                    # We can add logic here to detect primary key fields 
                    # For now, we'll set an empty array which is valid for tables without primary keys
                    if hasattr(field, 'is_primary_key') and field.is_primary_key:
                        identifier_field_ids.append(field.id)
                
                # Add identifier_field_ids to schema if we found any
                if identifier_field_ids:
                    schema_json["identifier-field-ids"] = identifier_field_ids
                
                # Calculate last column ID based on schema
                last_column_id = 0
                for field in request.schema_.fields:
                    if field.id > last_column_id:
                        last_column_id = field.id
                
                # Process partition spec - Ensure spec_id is properly set
                partition_spec_json = None
                spec_id = 0
                last_partition_id = 0
                
                if request.partition_spec:
                    partition_spec_json = json.loads(request.partition_spec.json(by_alias=True))
                    
                    # Add spec-id if not present
                    if "spec-id" not in partition_spec_json:
                        partition_spec_json["spec-id"] = spec_id
                    
                    # Ensure all partition fields have field_id
                    if "fields" in partition_spec_json:
                        for field in partition_spec_json["fields"]:
                            if "field-id" not in field or field["field-id"] is None:
                                last_partition_id += 1
                                field["field-id"] = last_partition_id
                            else:
                                field_id = field["field-id"]
                                if field_id > last_partition_id:
                                    last_partition_id = field_id
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

                # Handle credentials if provided
                if hasattr(request, 'credentials') and request.credentials:
                    # Process credentials
                    prefix = namespace_levels[0] if namespace_levels else 'default'
                    warehouse_parts = location.split('/')
                    warehouse = '/'.join(warehouse_parts[:3]) + '/'  # e.g., s3://bucket/
                    
                    existing_creds = await CredentialService.get_credentials_for_location(location)
                    
                    if not existing_creds:
                        cred_id = await CredentialService.upsert_credentials(
                            prefix,
                            warehouse,
                            request.credentials.config,
                            None
                        )
                        logger.debug(f"Added credentials with ID: {cred_id} for warehouse: {warehouse}")
                
                # Prepare response metadata
                table_metadata = TableMetadata.parse_obj({
                    "format-version": format_version,
                    "table-uuid": table_uuid,
                    "location": location,
                    "last-updated-ms": now_ms,
                    "properties": properties,
                    "schemas": [request.schema_],
                    "current-schema-id": schema_id,
                    "last-column-id": last_column_id,
                    "partition-specs": [request.partition_spec] if request.partition_spec else [PartitionSpec.parse_obj({"spec-id": 0, "fields": []})],
                    "default-spec-id": spec_id,
                    "last-partition-id": last_partition_id,
                    "sort-orders": [request.write_order] if request.write_order else [SortOrder.parse_obj({"order-id": 0, "fields": []})],
                    "default-sort-order-id": sort_order_id,
                    "snapshots": [],
                    "refs": {},
                    "current-snapshot-id": None,
                    "last-sequence-number": 0
                })
                
                # Generate metadata location - This is critical!
                metadata_location = f"{location}/metadata/00000-{uuid.uuid4()}.metadata.json"
                logger.info(f"Created table {request.name} in namespace {namespace_levels} with UUID {table_uuid}")
                
                # Get table configuration
                config = await TableService.get_table_config(table_id)
                
                # Get storage credentials if requested
                storage_credentials = None
                if x_iceberg_access_delegation:
                    storage_credentials = await TableService.get_storage_credentials(table_id)
                
                # Return with proper metadata_location
                return LoadTableResult(
                    metadata_location=metadata_location,  # Ensure this is set
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
        Get table-specific configuration from credentials.
        """
        logger.debug(f"Getting table config for table ID: {table_id}")
        
        # Get table location
        location_query = """
        SELECT location FROM tables WHERE id = $1
        """
        location_record = await db.fetch_one(location_query, table_id)
        
        if not location_record:
            logger.warning(f"No table found with ID: {table_id}")
            return {}
            
        location = location_record["location"]
        logger.debug(f"Table location: {location}")
        
        # Get all credentials
        all_creds_query = """
        SELECT prefix, warehouse, config FROM storage_credentials
        WHERE table_id IS NULL
        """
        all_creds = await db.fetch_all(all_creds_query)
        logger.debug(f"Found {len(all_creds)} total credentials to check")
        
        # Find matching credential by direct string comparison
        matched_cred = None
        for cred in all_creds:
            warehouse = cred["warehouse"]
            logger.debug(f"Checking if location '{location}' starts with warehouse '{warehouse}'")
            if location.startswith(warehouse):
                logger.debug(f"MATCH FOUND: '{location}' starts with '{warehouse}'")
                matched_cred = cred
                break
        
        if matched_cred:
            # Convert credential to table config
            config = matched_cred["config"]
            logger.debug(f"Credential config --->: {config}")
            if isinstance(config, str):
                config = json.loads(config)
                logger.debug(f"Credential config: {config}")
            
            logger.info(f"Using credential with prefix={matched_cred['prefix']}, warehouse={matched_cred['warehouse']}")
            
            # Convert the credential format to table config format
            table_config = {}
            
            # Map credential keys to config keys
            if "region" in config:
                table_config["client.region"] = config["region"]
            
            if "access-key-id" in config:
                table_config["s3.access-key-id"] = config["access-key-id"]
            
            if "secret-access-key" in config:
                table_config["s3.secret-access-key"] = config["secret-access-key"]
            
            if "session-token" in config:
                table_config["s3.session-token"] = config["session-token"]
            
            if "use-instance-credentials" in config and config["use-instance-credentials"] == "true":
                table_config["s3.use-instance-credentials"] = "true"
            
            logger.debug(f"Generated table config: {table_config}")
            return table_config
        
        # Fallback to defaults only when needed
        logger.warning(f"No matching credentials found for {location}, using defaults")
        return {
            "client.region": "us-east-1", 
            "s3.use-instance-credentials": "true"
        }
    
    @staticmethod
    async def get_table_basic_info(
        namespace_levels: List[str],
        table_name: str,
        if_none_match: Optional[str] = None
    ) -> Tuple[int, str, Optional[Dict]]:
        """Get basic table info and check if it matches the ETag."""
        query = """
        SELECT t.id, t.table_uuid, t.last_updated_ms, t.format_version
        FROM tables t
        JOIN namespaces n ON t.namespace_id = n.id
        WHERE n.levels = $1 AND t.name = $2
        """
        
        table_record = await db.fetch_one(query, namespace_levels, table_name)
        
        if not table_record:
            logger.warning(f"Table not found: {namespace_levels}.{table_name}")
            raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
        
        table_id = table_record["id"]
        table_uuid = str(table_record["table_uuid"])
        last_updated_ms = table_record["last_updated_ms"]
        
        # Generate ETag
        etag = f'"{table_uuid}-{last_updated_ms}"'
        
        # Check If-None-Match header
        if if_none_match and if_none_match == etag:
            logger.info(f"Table {namespace_levels}.{table_name} not modified, returning 304")
            return table_id, etag, None
        
        # Return basic table metadata
        return table_id, etag, {
            "format-version": table_record["format_version"],
            "table-uuid": table_uuid,
        }
    
    # Simple in-memory cache for table metadata to support 304 responses
    _table_metadata_cache = {}

    @staticmethod
    async def cache_table_metadata(namespace_levels: List[str], table_name: str, metadata: Dict) -> None:
        """Cache table metadata for future 304 responses."""
        cache_key = f"{'.'.join(namespace_levels)}.{table_name}"
        TableService._table_metadata_cache[cache_key] = metadata

    @staticmethod
    async def get_cached_table_metadata(namespace_levels: List[str], table_name: str) -> Optional[Dict]:
        """Get cached table metadata."""
        cache_key = f"{'.'.join(namespace_levels)}.{table_name}"
        return TableService._table_metadata_cache.get(cache_key)
    
    # @staticmethod
    # async def build_table_response(table_id: int, basic_metadata: Dict, snapshots: Optional[str] = None) -> LoadTableResult:
    #     """Build the full table response including all metadata."""
    #     # Fetch the remaining table details
    #     query = """
    #     SELECT t.location, t.current_snapshot_id, t.last_sequence_number,
    #         t.last_column_id, t.schema_id, t.current_schema_id,
    #         t.default_spec_id, t.last_partition_id, t.default_sort_order_id,
    #         t.properties, t.row_lineage, t.next_row_id, t.last_updated_ms
    #     FROM tables t
    #     WHERE t.id = $1
    #     """
        
    #     table_record = await db.fetch_one(query, table_id)
        
    #     # Fetch schemas
    #     schemas_query = """
    #     SELECT schema_id, schema_json FROM schemas WHERE table_id = $1
    #     """
    #     schema_records = await db.fetch_all(schemas_query, table_id)
    #     schemas = []
        
    #     for record in schema_records:
    #         schema_json = record["schema_json"]
    #         if isinstance(schema_json, str):
    #             schema_json = json.loads(schema_json)
    #         schema = Schema.parse_obj(schema_json)
    #         schemas.append(schema)
        
    #     # Fetch partition specs
    #     specs_query = """
    #     SELECT spec_id, spec_json FROM partition_specs WHERE table_id = $1
    #     """
    #     spec_records = await db.fetch_all(specs_query, table_id)
    #     partition_specs = []
        
    #     for record in spec_records:
    #         spec_json = record["spec_json"]
    #         if isinstance(spec_json, str):
    #             spec_json = json.loads(spec_json)
    #         spec = PartitionSpec.parse_obj(spec_json)
    #         partition_specs.append(spec)
        
    #     # Fetch sort orders
    #     orders_query = """
    #     SELECT order_id, order_json FROM sort_orders WHERE table_id = $1
    #     """
    #     order_records = await db.fetch_all(orders_query, table_id)
    #     sort_orders = []
        
    #     for record in order_records:
    #         order_json = record["order_json"]
    #         if isinstance(order_json, str):
    #             order_json = json.loads(order_json)
    #         order = SortOrder.parse_obj(order_json)
    #         sort_orders.append(order)
        
    #     # Fetch snapshots
    #     snapshots_query = """
    #     SELECT snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
    #         manifest_list, summary, schema_id
    #     FROM snapshots
    #     WHERE table_id = $1
    #     """
        
    #     # Add filtering based on snapshots parameter
    #     if snapshots == "refs":
    #         snapshots_query += """
    #         AND snapshot_id IN (SELECT snapshot_id FROM snapshot_refs WHERE table_id = $1)
    #         """
        
    #     snapshot_records = await db.fetch_all(snapshots_query, table_id)
    #     snapshots_list = []
        
    #     for record in snapshot_records:
    #         summary_json = record["summary"]
    #         if isinstance(summary_json, str):
    #             summary_json = json.loads(summary_json)
            
    #         snapshot = Snapshot(
    #             snapshot_id=record["snapshot_id"],
    #             parent_snapshot_id=record["parent_snapshot_id"],
    #             sequence_number=record["sequence_number"],
    #             timestamp_ms=record["timestamp_ms"],
    #             manifest_list=record["manifest_list"],
    #             summary=Summary.parse_obj(summary_json),
    #             schema_id=record["schema_id"]
    #         )
    #         snapshots_list.append(snapshot)
        
    #     # Fetch snapshot references
    #     refs_query = """
    #     SELECT name, snapshot_id, type, min_snapshots_to_keep,
    #         max_snapshot_age_ms, max_ref_age_ms
    #     FROM snapshot_refs
    #     WHERE table_id = $1
    #     """
    #     ref_records = await db.fetch_all(refs_query, table_id)
    #     refs = {}
        
    #     for record in ref_records:
    #         ref = {
    #             "type": record["type"],
    #             "snapshot-id": record["snapshot_id"],
    #         }
            
    #         if record["min_snapshots_to_keep"] is not None:
    #             ref["min-snapshots-to-keep"] = record["min_snapshots_to_keep"]
            
    #         if record["max_snapshot_age_ms"] is not None:
    #             ref["max-snapshot-age-ms"] = record["max_snapshot_age_ms"]
                
    #         if record["max_ref_age_ms"] is not None:
    #             ref["max-ref-age-ms"] = record["max_ref_age_ms"]
            
    #         refs[record["name"]] = ref
        
    #     # Try to fetch statistics if available
    #     statistics_list = []
    #     partition_statistics_list = []
        
    #     try:
    #         # Fetch table statistics
    #         statistics_query = """
    #         SELECT snapshot_id, statistics_path, file_size_in_bytes,
    #             file_footer_size_in_bytes, blob_metadata
    #         FROM table_statistics
    #         WHERE table_id = $1
    #         """
    #         statistics_records = await db.fetch_all(statistics_query, table_id)
            
    #         for record in statistics_records:
    #             blob_metadata_json = record["blob_metadata"]
    #             if isinstance(blob_metadata_json, str):
    #                 blob_metadata_json = json.loads(blob_metadata_json)
                    
    #             from app.models.table import StatisticsFile, BlobMetadata
    #             statistics = StatisticsFile(
    #                 snapshot_id=record["snapshot_id"],
    #                 statistics_path=record["statistics_path"],
    #                 file_size_in_bytes=record["file_size_in_bytes"],
    #                 file_footer_size_in_bytes=record["file_footer_size_in_bytes"],
    #                 blob_metadata=[BlobMetadata.parse_obj(bm) for bm in blob_metadata_json]
    #             )
    #             statistics_list.append(statistics)
            
    #         # Fetch partition statistics
    #         partition_query = """
    #         SELECT snapshot_id, statistics_path, file_size_in_bytes
    #         FROM partition_statistics
    #         WHERE table_id = $1
    #         """
    #         partition_records = await db.fetch_all(partition_query, table_id)
            
    #         for record in partition_records:
    #             from app.models.table import PartitionStatisticsFile
    #             partition_stats = PartitionStatisticsFile(
    #                 snapshot_id=record["snapshot_id"],
    #                 statistics_path=record["statistics_path"],
    #                 file_size_in_bytes=record["file_size_in_bytes"]
    #             )
    #             partition_statistics_list.append(partition_stats)
    #     except Exception as e:
    #         logger.warning(f"Error fetching statistics: {str(e)}. Continuing without statistics.")
        
    #     # Handle properties
    #     properties = table_record["properties"]
    #     if isinstance(properties, str):
    #         properties = json.loads(properties)
        
    #     # Construct table metadata
    #     table_metadata_dict = {
    #         "format-version": basic_metadata["format-version"],
    #         "table-uuid": basic_metadata["table-uuid"],
    #         "location": table_record["location"],
    #         "last-updated-ms": basic_metadata.get("last-updated-ms", table_record["last_updated_ms"]),
    #         "properties": properties or {},
    #         "schemas": schemas,
    #         "current-schema-id": table_record["current_schema_id"],
    #         "last-column-id": table_record["last_column_id"],
    #         "partition-specs": partition_specs,
    #         "default-spec-id": table_record["default_spec_id"],
    #         "last-partition-id": table_record["last_partition_id"],
    #         "sort-orders": sort_orders,
    #         "default-sort-order-id": table_record["default_sort_order_id"],
    #         "snapshots": snapshots_list,
    #         "refs": refs,
    #         "current-snapshot-id": table_record["current_snapshot_id"],
    #         "last-sequence-number": table_record["last_sequence_number"]
    #     }
        
    #     # Add optional fields if they exist
    #     if statistics_list:
    #         table_metadata_dict["statistics"] = statistics_list
        
    #     if partition_statistics_list:
    #         table_metadata_dict["partition-statistics"] = partition_statistics_list
        
    #     if table_record.get("row_lineage") is not None:
    #         table_metadata_dict["row-lineage"] = table_record["row_lineage"]
        
    #     if table_record.get("next_row_id") is not None:
    #         table_metadata_dict["next-row-id"] = table_record["next_row_id"]
        
    #     # Parse into TableMetadata object
    #     table_metadata = TableMetadata.parse_obj(table_metadata_dict)
        
    #     # Generate metadata location
    #     metadata_location = f"{table_record['location']}/metadata/current.metadata.json"
        
    #     # Get table configuration and credentials
    #     config = await TableService.get_table_config(table_id)
    #     storage_credentials = await TableService.get_storage_credentials(table_id)
        
    #     # Cache the table metadata for future 304 responses
    #     namespace_from_metadata = await TableService.get_table_namespace(table_id)
    #     table_name = await TableService.get_table_name(table_id)
    #     await TableService.cache_table_metadata(namespace_from_metadata, table_name, table_metadata.dict(by_alias=True))
        
    #     # Return the LoadTableResult
    #     return LoadTableResult(
    #         metadata_location=metadata_location,
    #         metadata=table_metadata,
    #         config=config,
    #         storage_credentials=storage_credentials
    #     )

    @staticmethod
    async def build_table_response(table_id: int, basic_metadata: Dict, snapshots: Optional[str] = None) -> LoadTableResult:
        """Build the full table response including all metadata."""
        # Fetch the remaining table details
        query = """
        SELECT t.location, t.current_snapshot_id, t.last_sequence_number,
            t.last_column_id, t.schema_id, t.current_schema_id,
            t.default_spec_id, t.last_partition_id, t.default_sort_order_id,
            t.properties, t.row_lineage, t.next_row_id, t.last_updated_ms
        FROM tables t
        WHERE t.id = $1
        """
        
        table_record = await db.fetch_one(query, table_id)
        
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
            
            # # Ensure schema_id is set
            # if "schema-id" not in schema_json and record["schema_id"] is not None:
            #     schema_json["schema-id"] = record["schema_id"]
            # Ensure schema_id is set - this is critical!
            if "schema-id" not in schema_json or schema_json["schema-id"] is None:
                schema_json["schema-id"] = record["schema_id"]
                logger.debug(f"Added missing schema-id {record['schema_id']} to schema")

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
            
            # Ensure spec_id is set
            # if "spec-id" not in spec_json and record["spec_id"] is not None:
            #     spec_json["spec-id"] = record["spec_id"]
            if "spec-id" not in spec_json or spec_json["spec-id"] is None:
                spec_json["spec-id"] = record["spec_id"]
                logger.debug(f"Added missing spec-id {record['spec_id']} to partition spec")

            # Ensure all partition fields have field_id
            last_field_id = table_record["last_partition_id"]
            if "fields" in spec_json:
                for field in spec_json["fields"]:
                    if "field-id" not in field or field["field-id"] is None:
                        last_field_id += 1
                        field["field-id"] = last_field_id
                        logger.debug(f"Added missing field-id {last_field_id} to partition field")
            
            spec = PartitionSpec.parse_obj(spec_json)
            partition_specs.append(spec)
            # if "fields" in spec_json:
            #     for field in spec_json["fields"]:
            #         if "field-id" not in field or field["field-id"] is None:
            #             # Generate a field-id if missing
            #             field["field-id"] = table_record["last_partition_id"] + 1
                
            # spec = PartitionSpec.parse_obj(spec_json)
            # partition_specs.append(spec)
        
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
        table_metadata_dict = {
            "format-version": basic_metadata["format-version"],
            "table-uuid": basic_metadata["table-uuid"],
            "location": table_record["location"],
            "last-updated-ms": basic_metadata.get("last-updated-ms", table_record["last_updated_ms"]),
            "properties": properties or {},
            "schemas": schemas,
            "current-schema-id": table_record["current_schema_id"],
            "last-column-id": table_record["last_column_id"],
            "partition-specs": partition_specs,
            "default-spec-id": table_record["default_spec_id"],
            "last-partition-id": table_record["last_partition_id"],
            "sort-orders": sort_orders,
            "default-sort-order-id": table_record["default_sort_order_id"],
            "snapshots": snapshots_list,
            "refs": refs,
            "current-snapshot-id": table_record["current_snapshot_id"],
            "last-sequence-number": table_record["last_sequence_number"]
        }
        
        # Add optional fields if they exist
        if table_record.get("row_lineage") is not None:
            table_metadata_dict["row-lineage"] = table_record["row_lineage"]
        
        if table_record.get("next_row_id") is not None:
            table_metadata_dict["next-row-id"] = table_record["next_row_id"]
        
        # Parse into TableMetadata object
        table_metadata = TableMetadata.parse_obj(table_metadata_dict)
        
        # Generate metadata location - This is critical!
        metadata_location = f"{table_record['location']}/metadata/current.metadata.json"
        
        # Get table configuration and credentials
        config = await TableService.get_table_config(table_id)
        storage_credentials = await TableService.get_storage_credentials(table_id)
        
        # Create the full response dictionary for caching
        result_dict = {
            "metadata-location": metadata_location,  # Ensure this is set
            "metadata": table_metadata.dict(by_alias=True),
            "config": config,
            "storage-credentials": [c.dict(by_alias=True) for c in storage_credentials] if storage_credentials else None
        }
        
        # Cache the table metadata for future 304 responses
        namespace_from_metadata = await TableService.get_table_namespace(table_id)
        table_name = await TableService.get_table_name(table_id)
        await TableService.cache_table_metadata(namespace_from_metadata, table_name, result_dict)
        
        # Return the LoadTableResult
        return LoadTableResult(
            metadata_location=metadata_location,  # Ensure metadata_location is included
            metadata=table_metadata,
            config=config,
            storage_credentials=storage_credentials
        )

    @staticmethod
    async def get_table_namespace(table_id: int) -> List[str]:
        """Get the namespace for a table by ID."""
        query = """
        SELECT n.levels FROM namespaces n
        JOIN tables t ON t.namespace_id = n.id
        WHERE t.id = $1
        """
        
        record = await db.fetch_one(query, table_id)
        return record["levels"] if record else []

    @staticmethod
    async def get_table_name(table_id: int) -> str:
        """Get the name of a table by ID."""
        query = """
        SELECT name FROM tables
        WHERE id = $1
        """
        
        record = await db.fetch_one(query, table_id)
        return record["name"] if record else ""
    
    @staticmethod
    async def get_storage_credentials(table_id: int) -> List[StorageCredential]:
        """
        Get storage credentials for a table.
        """
        # Get the table location
        query = """
        SELECT location FROM tables
        WHERE id = $1
        """
        logger.debug(f"Fetching location for table ID: {table_id}")
        table_record = await db.fetch_one(query, table_id)
        if not table_record:
            logger.warning(f"No table found with ID: {table_id}")
            return []
        
        location = table_record["location"]
        logger.debug(f"Found table location: {location}")
        
        # 1. Try table-specific credentials
        query = """
        SELECT prefix, warehouse, config FROM storage_credentials
        WHERE table_id = $1
        """
        cred_records = await db.fetch_all(query, table_id)
        logger.debug(f"Found {len(cred_records) if cred_records else 0} table-specific credentials")
        
        if not cred_records or len(cred_records) == 0:
            # 2. Try location-based credentials - use simplified exact prefix matching
            query = """
            SELECT prefix, warehouse, config FROM storage_credentials
            WHERE table_id IS NULL AND $1 LIKE (warehouse || '%')
            ORDER BY LENGTH(warehouse) DESC
            """
            logger.debug(f"Executing query for location {location}: {query}")
            cred_records = await db.fetch_all(query, location)
            logger.debug(f"Found {len(cred_records) if cred_records else 0} location-based credentials for {location}")

            # If still no records, try a more direct approach
            if not cred_records or len(cred_records) == 0:
                # 3. Direct query to debug
                query = """
                SELECT prefix, warehouse, config FROM storage_credentials
                WHERE table_id IS NULL
                """
                all_records = await db.fetch_all(query)
                logger.debug(f"All available global credentials: {len(all_records)}")
                
                for record in all_records:
                    warehouse = record["warehouse"]
                    logger.debug(f"Checking if {location} starts with {warehouse}")
                    if location.startswith(warehouse):
                        logger.debug(f"Match found for warehouse: {warehouse}")
                        cred_records = [record]
                        break
        
        # Convert results to StorageCredential models
        credentials = []
        for record in cred_records:
            config = record["config"]
            if isinstance(config, str):
                config = json.loads(config)
            
            credentials.append(
                StorageCredential(
                    prefix=record["warehouse"], 
                    config=config
                )
            )
        
        logger.info(f"Returning {len(credentials)} credentials for table ID: {table_id}")
        for cred in credentials:
            logger.debug(f"Credential prefix: {cred.prefix}, config: {cred.config}")
        
        return credentials
    
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
            
            # for record in schema_records:
            #     schema_json = record["schema_json"]
            #     if isinstance(schema_json, str):
            #         schema_json = json.loads(schema_json)
            #     schema = Schema.parse_obj(schema_json)
            #     schemas.append(schema)
            
            # When processing schemas in build_table_response
            for record in schema_records:
                schema_json = record["schema_json"]
                if isinstance(schema_json, str):
                    schema_json = json.loads(schema_json)
                
                # Ensure schema_id is set - this is critical!
                if "schema-id" not in schema_json or schema_json["schema-id"] is None:
                    schema_json["schema-id"] = record["schema_id"]
                    logger.debug(f"Added missing schema-id {record['schema_id']} to schema")
                
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
                
                # Ensure spec_id is set - this is critical!
                if "spec-id" not in spec_json or spec_json["spec-id"] is None:
                    spec_json["spec-id"] = record["spec_id"]
                    logger.debug(f"Added missing spec-id {record['spec_id']} to partition spec")
                
                # Ensure all partition fields have field_id
                last_field_id = table_record["last_partition_id"]
                if "fields" in spec_json:
                    for field in spec_json["fields"]:
                        if "field-id" not in field or field["field-id"] is None:
                            last_field_id += 1
                            field["field-id"] = last_field_id
                            logger.debug(f"Added missing field-id {last_field_id} to partition field")
                
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
            # table_metadata = TableMetadata(
            #     format_version=table_record["format_version"],
            #     table_uuid=table_record["table_uuid"],
            #     location=table_record["location"],
            #     last_updated_ms=table_record["last_updated_ms"],
            #     properties=properties,
            #     schemas=schemas,
            #     current_schema_id=table_record["current_schema_id"],
            #     last_column_id=table_record["last_column_id"],
            #     partition_specs=partition_specs,
            #     default_spec_id=table_record["default_spec_id"],
            #     last_partition_id=table_record["last_partition_id"],
            #     sort_orders=sort_orders,
            #     default_sort_order_id=table_record["default_sort_order_id"],
            #     snapshots=snapshots_list,
            #     refs=refs,
            #     current_snapshot_id=table_record["current_snapshot_id"],
            #     last_sequence_number=table_record["last_sequence_number"]
            # )

            # Construct table metadata
            table_metadata = TableMetadata.parse_obj({
                "format-version": table_record["format_version"],
                "table-uuid": str(table_record["table_uuid"]),
                "location": table_record["location"],
                "last-updated-ms": table_record["last_updated_ms"],
                "properties": properties,
                "schemas": schemas,
                "current-schema-id": table_record["current_schema_id"],
                "last-column-id": table_record["last_column_id"],
                "partition-specs": partition_specs,
                "default-spec-id": table_record["default_spec_id"],
                "last-partition-id": table_record["last_partition_id"],
                "sort-orders": sort_orders,
                "default-sort-order-id": table_record["default_sort_order_id"],
                "snapshots": snapshots_list,
                "refs": refs,
                "current-snapshot-id": table_record["current_snapshot_id"],
                "last-sequence-number": table_record["last_sequence_number"]
            })
            
            # Generate metadata location
            metadata_location = f"{table_record['location']}/metadata/current.metadata.json"
            logger.info(f"Loaded table {namespace_levels}.{table_name}")
            
            # Get table configuration
            config = await TableService.get_table_config(table_id)
            
            # Get storage credentials if requested
            # the below code will vend credentials only with header x-iceberg-access-delegation
            # commenting this out for now as we are not using it
            # storage_credentials = None
            # if x_iceberg_access_delegation:
            #     storage_credentials = await TableService.get_storage_credentials(table_id)
            
            # the below code will vend credentials for all tables without any header
            storage_credentials = await TableService.get_storage_credentials(table_id)
            logger.debug(f"Found {len(storage_credentials)} credentials for table {table_id}")
            # Use parse_obj to handle aliased field properly
            result = LoadTableResult.parse_obj({
                "metadata-location": metadata_location,
                "metadata": table_metadata.dict(by_alias=True),
                "config": config,
                "storage-credentials": storage_credentials
            })
            return result, etag
            
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
        """Load credentials for a table from the catalog."""
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
        
        # 1. Try table-specific credentials
        query = """
        SELECT prefix, warehouse, config FROM storage_credentials
        WHERE table_id = $1
        """
        cred_records = await db.fetch_all(query, table_id)
        
        if not cred_records:
            # 2. Try location-based credentials
            query = """
            SELECT prefix, warehouse, config FROM storage_credentials
            WHERE table_id IS NULL AND 
                $1 LIKE CONCAT(warehouse, '%')
            ORDER BY LENGTH(warehouse) DESC
            """
            cred_records = await db.fetch_all(query, location)
        
        if not cred_records:
            # 3. Try namespace prefix-based credentials
            namespace_prefix = namespace_levels[0] if namespace_levels else 'default'
            query = """
            SELECT prefix, warehouse, config FROM storage_credentials
            WHERE table_id IS NULL AND
                prefix = $1
            ORDER BY LENGTH(warehouse) DESC
            """
            cred_records = await db.fetch_all(query, namespace_prefix)
        
        # Convert results to StorageCredential models
        credentials = []
        for record in cred_records:
            config = record["config"]
            if isinstance(config, str):
                config = json.loads(config)
            
            credentials.append(
                StorageCredential(
                    prefix=record["warehouse"],  # This matches the API model
                    config=config
                )
            )
        
        logger.info(f"Loaded {len(credentials)} credential(s) for table {namespace_levels}.{table_name}")
        
        # Use parse_obj to handle field aliases
        return LoadCredentialsResponse.parse_obj({
            "storage-credentials": credentials
        })
    
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

    @staticmethod
    async def update_table(
        namespace_levels: List[str],
        table_name: str,
        request: CommitTableRequest
    ) -> CommitTableResponse:
        """
        Update table metadata by applying a series of updates.
        Handles table evolution including schema evolution, partition evolution,
        snapshot management, etc.
        """
        logger.info(f"Processing table update for {namespace_levels}.{table_name}")
        
        # Get namespace ID and table ID
        namespace_id = await TableService._get_namespace_id(namespace_levels)
        if namespace_id is None:
            raise ValueError(f"Namespace not found: {namespace_levels}")
        
        # Get table details
        query = """
        SELECT t.id, t.table_uuid, t.location, t.current_snapshot_id, t.last_sequence_number,
            t.last_updated_ms, t.last_column_id, t.schema_id, t.current_schema_id,
            t.default_spec_id, t.last_partition_id, t.default_sort_order_id,
            t.properties, t.format_version, t.row_lineage, t.next_row_id
        FROM tables t
        WHERE t.namespace_id = $1 AND t.name = $2
        """
        
        table_record = await db.fetch_one(query, namespace_id, table_name)
        if not table_record:
            raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
        
        table_id = table_record["id"]
        table_uuid = str(table_record["table_uuid"])
        location = table_record["location"]
        
        try:
            async with db.transaction():
                # Verify all requirements are met
                for requirement in request.requirements:
                    if not await TableService._validate_requirement(table_id, table_record, requirement):
                        requirement_type = getattr(requirement, "type", "Unknown")
                        raise ValueError(f"Table requirement not met: {requirement_type}")
                
                # Process all updates
                for update in request.updates:
                    await TableService._apply_update(table_id, table_record, update.__root__)
                    
                # Reload table record after updates
                table_record = await db.fetch_one(query, namespace_id, table_name)
                
                # Generate new metadata location
                now_ms = int(time.time() * 1000)
                metadata_file_uuid = uuid.uuid4()
                metadata_location = f"{location}/metadata/{table_record['format_version']:05d}-{metadata_file_uuid}.metadata.json"
                
                # Create metadata log entry
                log_query = """
                INSERT INTO metadata_log (table_id, metadata_file, timestamp_ms)
                VALUES ($1, $2, $3)
                """
                await db.execute(log_query, table_id, metadata_location, now_ms)
                
                # Construct the updated table metadata
                table_metadata = await TableService._build_table_metadata(table_id)
                
                logger.info(f"Successfully updated table {namespace_levels}.{table_name}")
                
                # Return updated metadata
                return CommitTableResponse(
                    metadata_location=metadata_location,
                    metadata=table_metadata
                )
        except ValueError:
            # Re-raise ValueError
            raise
        except Exception as e:
            logger.error(f"Error updating table: {str(e)}", exc_info=True)
            raise

    @staticmethod
    async def _get_namespace_id(namespace_levels: List[str]) -> Optional[int]:
        """Helper method to get namespace ID from namespace levels."""
        query = """
        SELECT id FROM namespaces WHERE levels = $1
        """
        record = await db.fetch_one(query, namespace_levels)
        return record["id"] if record else None

    @staticmethod
    async def _validate_requirement(
        table_id: int,
        table_record: Dict,
        requirement: TableRequirement
    ) -> bool:
        """Validate that a table requirement is met."""
        requirement_type = getattr(requirement, "type", None)
        logger.debug(f"Validating requirement type: {requirement_type}")
        
        if requirement_type == "assert-create":
            # Table must not exist (this should never be true here since we already loaded the table)
            return False
        
        elif requirement_type == "assert-table-uuid":
            # Table UUID must match
            return str(table_record["table_uuid"]) == requirement.uuid
        
        elif requirement_type == "assert-ref-snapshot-id":
            # Check if ref exists and points to the right snapshot
            query = """
            SELECT snapshot_id FROM snapshot_refs 
            WHERE table_id = $1 AND name = $2
            """
            record = await db.fetch_one(query, table_id, requirement.ref)
            
            if requirement.snapshot_id is None:
                # Ref must not exist
                return record is None
            else:
                # Ref must exist and point to the right snapshot
                return record and record["snapshot_id"] == requirement.snapshot_id
        
        elif requirement_type == "assert-last-assigned-field-id":
            # Last column ID must match
            return table_record["last_column_id"] == requirement.last_assigned_field_id
        
        elif requirement_type == "assert-current-schema-id":
            # Current schema ID must match
            return table_record["current_schema_id"] == requirement.current_schema_id
        
        elif requirement_type == "assert-last-assigned-partition-id":
            # Last partition ID must match
            return table_record["last_partition_id"] == requirement.last_assigned_partition_id
        
        elif requirement_type == "assert-default-spec-id":
            # Default spec ID must match
            return table_record["default_spec_id"] == requirement.default_spec_id
        
        elif requirement_type == "assert-default-sort-order-id":
            # Default sort order ID must match
            return table_record["default_sort_order_id"] == requirement.default_sort_order_id
        
        # Unknown requirement type
        logger.warning(f"Unknown requirement type: {requirement_type}")
        return False

    @staticmethod
    async def _apply_update(
        table_id: int,
        table_record: Dict,
        update: Any
    ) -> None:
        """Apply a single update to the table."""
        update_type = getattr(update, "action", None)
        logger.info(f"Applying update: {update_type}")
        
        if update_type == "assign-uuid":
            # Update table UUID
            query = """
            UPDATE tables SET table_uuid = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, update.uuid, table_id)
        
        elif update_type == "upgrade-format-version":
            # Upgrade format version
            query = """
            UPDATE tables SET format_version = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, update.format_version, table_id)
        
        elif update_type == "add-schema":
            # Add new schema
            schema_json = json.loads(update.schema_.json(by_alias=True))
            
            # Set schema_id if not already set
            schema_id = schema_json.get("schema-id")
            if schema_id is None:
                # Get the highest schema ID and increment
                query = """
                SELECT MAX(schema_id) as max_schema_id FROM schemas WHERE table_id = $1
                """
                record = await db.fetch_one(query, table_id)
                max_schema_id = record["max_schema_id"] if record and record["max_schema_id"] is not None else -1
                schema_id = max_schema_id + 1
                schema_json["schema-id"] = schema_id
            
            # Calculate last_column_id
            last_column_id = table_record["last_column_id"]
            for field in schema_json.get("fields", []):
                if field.get("id", 0) > last_column_id:
                    last_column_id = field["id"]
            
            # Insert new schema
            query = """
            INSERT INTO schemas (table_id, schema_id, schema_json)
            VALUES ($1, $2, $3)
            """
            await db.execute(query, table_id, schema_id, json.dumps(schema_json))
            
            # Update table's last_column_id
            query = """
            UPDATE tables SET last_column_id = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, last_column_id, table_id)
        
        elif update_type == "set-current-schema":
            # Set current schema
            schema_id = update.schema_id
            
            # If schema_id is -1, set to the latest schema
            if schema_id == -1:
                query = """
                SELECT MAX(schema_id) as schema_id FROM schemas WHERE table_id = $1
                """
                record = await db.fetch_one(query, table_id)
                schema_id = record["schema_id"] if record else 0
            
            # Update current schema ID
            query = """
            UPDATE tables SET current_schema_id = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, schema_id, table_id)
        
        elif update_type == "add-spec":
            # Add partition spec
            spec_json = json.loads(update.spec.json(by_alias=True))
            
            # Set spec_id if not already set
            spec_id = spec_json.get("spec-id")
            if spec_id is None:
                # Get the highest spec ID and increment
                query = """
                SELECT MAX(spec_id) as max_spec_id FROM partition_specs WHERE table_id = $1
                """
                record = await db.fetch_one(query, table_id)
                max_spec_id = record["max_spec_id"] if record and record["max_spec_id"] is not None else -1
                spec_id = max_spec_id + 1
                spec_json["spec-id"] = spec_id
            
            # Calculate last_partition_id
            last_partition_id = table_record["last_partition_id"]
            for field in spec_json.get("fields", []):
                if "field-id" in field and field["field-id"] is not None:
                    if field["field-id"] > last_partition_id:
                        last_partition_id = field["field-id"]
                else:
                    # Auto-assign field-id if missing
                    last_partition_id += 1
                    field["field-id"] = last_partition_id
            
            # Insert new partition spec
            query = """
            INSERT INTO partition_specs (table_id, spec_id, spec_json)
            VALUES ($1, $2, $3)
            """
            await db.execute(query, table_id, spec_id, json.dumps(spec_json))
            
            # Update table's last_partition_id
            query = """
            UPDATE tables SET last_partition_id = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, last_partition_id, table_id)
        
        elif update_type == "set-default-spec":
            # Set default partition spec
            spec_id = update.spec_id
            
            # If spec_id is -1, set to the latest spec
            if spec_id == -1:
                query = """
                SELECT MAX(spec_id) as spec_id FROM partition_specs WHERE table_id = $1
                """
                record = await db.fetch_one(query, table_id)
                spec_id = record["spec_id"] if record else 0
            
            # Update default spec ID
            query = """
            UPDATE tables SET default_spec_id = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, spec_id, table_id)
        
        elif update_type == "add-sort-order":
            # Add sort order
            order_json = json.loads(update.sort_order.json(by_alias=True))
            order_id = order_json.get("order-id")
            
            # Insert new sort order
            query = """
            INSERT INTO sort_orders (table_id, order_id, order_json)
            VALUES ($1, $2, $3)
            """
            await db.execute(query, table_id, order_id, json.dumps(order_json))
        
        elif update_type == "set-default-sort-order":
            # Set default sort order
            order_id = update.sort_order_id
            
            # If order_id is -1, set to the latest order
            if order_id == -1:
                query = """
                SELECT MAX(order_id) as order_id FROM sort_orders WHERE table_id = $1
                """
                record = await db.fetch_one(query, table_id)
                order_id = record["order_id"] if record else 0
            
            # Update default sort order ID
            query = """
            UPDATE tables SET default_sort_order_id = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, order_id, table_id)
        
        elif update_type == "add-snapshot":
            # Add snapshot
            snapshot_json = json.loads(update.snapshot.json(by_alias=True))
            snapshot_id = snapshot_json.get("snapshot-id")
            parent_snapshot_id = snapshot_json.get("parent-snapshot-id")
            sequence_number = snapshot_json.get("sequence-number")
            timestamp_ms = snapshot_json.get("timestamp-ms")
            manifest_list = snapshot_json.get("manifest-list")
            summary = snapshot_json.get("summary", {})
            schema_id = snapshot_json.get("schema-id")
            
            # Insert new snapshot
            query = """
            INSERT INTO snapshots (
                table_id, snapshot_id, parent_snapshot_id, sequence_number,
                timestamp_ms, manifest_list, summary, schema_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """
            await db.execute(
                query, table_id, snapshot_id, parent_snapshot_id, sequence_number,
                timestamp_ms, manifest_list, json.dumps(summary), schema_id
            )
            
            # Update table's current_snapshot_id and last_sequence_number
            query = """
            UPDATE tables SET 
                current_snapshot_id = $1, 
                last_sequence_number = GREATEST(last_sequence_number, $2),
                updated_at = NOW()
            WHERE id = $3
            """
            await db.execute(query, snapshot_id, sequence_number, table_id)
        
        elif update_type == "set-snapshot-ref":
            # Set snapshot reference (branch/tag)
            ref_name = update.ref_name
            snapshot_id = update.snapshot_id
            ref_type = update.type
            min_snapshots_to_keep = getattr(update, "min_snapshots_to_keep", None)
            max_snapshot_age_ms = getattr(update, "max_snapshot_age_ms", None)
            max_ref_age_ms = getattr(update, "max_ref_age_ms", None)
            
            # Check if ref already exists
            query = """
            SELECT id FROM snapshot_refs
            WHERE table_id = $1 AND name = $2
            """
            record = await db.fetch_one(query, table_id, ref_name)
            
            if record:
                # Update existing ref
                query = """
                UPDATE snapshot_refs
                SET snapshot_id = $1, type = $2, min_snapshots_to_keep = $3,
                    max_snapshot_age_ms = $4, max_ref_age_ms = $5, updated_at = NOW()
                WHERE table_id = $6 AND name = $7
                """
                await db.execute(
                    query, snapshot_id, ref_type, min_snapshots_to_keep,
                    max_snapshot_age_ms, max_ref_age_ms, table_id, ref_name
                )
            else:
                # Insert new ref
                query = """
                INSERT INTO snapshot_refs (
                    table_id, name, snapshot_id, type,
                    min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """
                await db.execute(
                    query, table_id, ref_name, snapshot_id, ref_type,
                    min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms
                )
        
        elif update_type == "remove-snapshots":
            # Remove snapshots
            snapshot_ids = update.snapshot_ids
            
            # Delete snapshots
            query = """
            DELETE FROM snapshots
            WHERE table_id = $1 AND snapshot_id = ANY($2)
            """
            await db.execute(query, table_id, snapshot_ids)
        
        elif update_type == "remove-snapshot-ref":
            # Remove snapshot reference
            ref_name = update.ref_name
            
            # Delete ref
            query = """
            DELETE FROM snapshot_refs
            WHERE table_id = $1 AND name = $2
            """
            await db.execute(query, table_id, ref_name)
        
        elif update_type == "set-location":
            # Set table location
            new_location = update.location
            
            # Update location
            query = """
            UPDATE tables SET location = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, new_location, table_id)
        
        elif update_type == "set-properties":
            # Set table properties
            updates = update.updates
            
            # Get current properties
            current_properties = table_record["properties"]
            if isinstance(current_properties, str):
                current_properties = json.loads(current_properties)
            elif current_properties is None:
                current_properties = {}
            
            # Update properties
            for key, value in updates.items():
                current_properties[key] = value
            
            # Save updated properties
            query = """
            UPDATE tables SET properties = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, json.dumps(current_properties), table_id)
        
        elif update_type == "remove-properties":
            # Remove table properties
            removals = update.removals
            
            # Get current properties
            current_properties = table_record["properties"]
            if isinstance(current_properties, str):
                current_properties = json.loads(current_properties)
            elif current_properties is None:
                current_properties = {}
            
            # Remove properties
            for key in removals:
                if key in current_properties:
                    del current_properties[key]
            
            # Save updated properties
            query = """
            UPDATE tables SET properties = $1, updated_at = NOW()
            WHERE id = $2
            """
            await db.execute(query, json.dumps(current_properties), table_id)
        
        elif update_type == "set-statistics":
            # Set table statistics
            statistics = update.statistics
            snapshot_id = statistics.snapshot_id
            statistics_path = statistics.statistics_path
            file_size_in_bytes = statistics.file_size_in_bytes
            file_footer_size_in_bytes = statistics.file_footer_size_in_bytes
            blob_metadata_json = json.loads(json.dumps([b.dict(by_alias=True) for b in statistics.blob_metadata]))
            
            # Check if statistics already exist for this snapshot
            query = """
            SELECT id FROM table_statistics
            WHERE table_id = $1 AND snapshot_id = $2
            """
            record = await db.fetch_one(query, table_id, snapshot_id)
            
            if record:
                # Update existing statistics
                query = """
                UPDATE table_statistics
                SET statistics_path = $1, file_size_in_bytes = $2,
                    file_footer_size_in_bytes = $3, blob_metadata = $4
                WHERE table_id = $5 AND snapshot_id = $6
                """
                await db.execute(
                    query, statistics_path, file_size_in_bytes,
                    file_footer_size_in_bytes, json.dumps(blob_metadata_json),
                    table_id, snapshot_id
                )
            else:
                # Insert new statistics
                query = """
                INSERT INTO table_statistics (
                    table_id, snapshot_id, statistics_path,
                    file_size_in_bytes, file_footer_size_in_bytes, blob_metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                """
                await db.execute(
                    query, table_id, snapshot_id, statistics_path,
                    file_size_in_bytes, file_footer_size_in_bytes, json.dumps(blob_metadata_json)
                )
        
        elif update_type == "set-partition-statistics":
            # Set partition statistics
            partition_statistics = update.partition_statistics
            snapshot_id = partition_statistics.snapshot_id
            statistics_path = partition_statistics.statistics_path
            file_size_in_bytes = partition_statistics.file_size_in_bytes
            
            # Check if partition statistics already exist for this snapshot
            query = """
            SELECT id FROM partition_statistics
            WHERE table_id = $1 AND snapshot_id = $2
            """
            record = await db.fetch_one(query, table_id, snapshot_id)
            
            if record:
                # Update existing statistics
                query = """
                UPDATE partition_statistics
                SET statistics_path = $1, file_size_in_bytes = $2
                WHERE table_id = $3 AND snapshot_id = $4
                """
                await db.execute(
                    query, statistics_path, file_size_in_bytes, table_id, snapshot_id
                )
            else:
                # Insert new statistics
                query = """
                INSERT INTO partition_statistics (
                    table_id, snapshot_id, statistics_path, file_size_in_bytes
                )
                VALUES ($1, $2, $3, $4)
                """
                await db.execute(
                    query, table_id, snapshot_id, statistics_path, file_size_in_bytes
                )
        
        elif update_type == "remove-statistics":
            # Remove statistics
            snapshot_id = update.snapshot_id
            
            # Delete statistics
            query = """
            DELETE FROM table_statistics
            WHERE table_id = $1 AND snapshot_id = $2
            """
            await db.execute(query, table_id, snapshot_id)
        
        elif update_type == "remove-partition-statistics":
            # Remove partition statistics
            snapshot_id = update.snapshot_id
            
            # Delete partition statistics
            query = """
            DELETE FROM partition_statistics
            WHERE table_id = $1 AND snapshot_id = $2
            """
            await db.execute(query, table_id, snapshot_id)
        
        elif update_type == "remove-partition-specs":
            # Remove partition specs
            spec_ids = update.spec_ids
            
            # Delete partition specs
            query = """
            DELETE FROM partition_specs
            WHERE table_id = $1 AND spec_id = ANY($2)
            """
            await db.execute(query, table_id, spec_ids)
        
        elif update_type == "remove-schemas":
            # Remove schemas
            schema_ids = update.schema_ids
            
            # Delete schemas
            query = """
            DELETE FROM schemas
            WHERE table_id = $1 AND schema_id = ANY($2)
            """
            await db.execute(query, table_id, schema_ids)
        
        elif update_type == "enable-row-lineage":
            # Enable row lineage
            query = """
            UPDATE tables SET row_lineage = TRUE, updated_at = NOW()
            WHERE id = $1
            """
            await db.execute(query, table_id)
        
        else:
            # Unknown update type
            logger.warning(f"Unknown update type: {update_type}")
            raise ValueError(f"Unsupported update type: {update_type}")

    @staticmethod
    async def _build_table_metadata(table_id: int) -> TableMetadata:
        """Build complete table metadata object."""
        # Get table details
        query = """
        SELECT t.table_uuid, t.location, t.current_snapshot_id, t.last_sequence_number,
            t.last_updated_ms, t.last_column_id, t.schema_id, t.current_schema_id,
            t.default_spec_id, t.last_partition_id, t.default_sort_order_id,
            t.properties, t.format_version, t.row_lineage, t.next_row_id
        FROM tables t
        WHERE t.id = $1
        """
        
        table_record = await db.fetch_one(query, table_id)
        
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
            
            # Ensure schema_id is set
            if "schema-id" not in schema_json and record["schema_id"] is not None:
                schema_json["schema-id"] = record["schema_id"]
                
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
            
            # Ensure spec_id is set
            if "spec-id" not in spec_json and record["spec_id"] is not None:
                spec_json["spec-id"] = record["spec_id"]
                
            # Ensure all partition fields have field_id
            if "fields" in spec_json:
                for field in spec_json["fields"]:
                    if "field-id" not in field or field["field-id"] is None:
                        # Generate a field-id if missing
                        field["field-id"] = table_record["last_partition_id"] + 1
                
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
        table_metadata_dict = {
            "format-version": table_record["format_version"],
            "table-uuid": str(table_record["table_uuid"]),
            "location": table_record["location"],
            "last-updated-ms": table_record["last_updated_ms"],
            "properties": properties or {},
            "schemas": schemas,
            "current-schema-id": table_record["current_schema_id"],
            "last-column-id": table_record["last_column_id"],
            "partition-specs": partition_specs,
            "default-spec-id": table_record["default_spec_id"],
            "last-partition-id": table_record["last_partition_id"],
            "sort-orders": sort_orders,
            "default-sort-order-id": table_record["default_sort_order_id"],
            "snapshots": snapshots_list,
            "refs": refs,
            "current-snapshot-id": table_record["current_snapshot_id"],
            "last-sequence-number": table_record["last_sequence_number"]
        }
        
        # Add optional fields if they exist
        if table_record.get("row_lineage") is not None:
            table_metadata_dict["row-lineage"] = table_record["row_lineage"]
        
        if table_record.get("next_row_id") is not None:
            table_metadata_dict["next-row-id"] = table_record["next_row_id"]
        
        # Parse into TableMetadata object
        return TableMetadata.parse_obj(table_metadata_dict)
    
    @staticmethod
    async def commit_transaction(request: CommitTransactionRequest) -> None:
        """
        Commits multiple table changes in a single transaction.
        """
        logger.info(f"Processing transaction with {len(request.table_changes)} table changes")
        
        try:
            async with db.transaction():
                # Create transaction record
                transaction_id = uuid.uuid4()
                transaction_query = """
                INSERT INTO transactions (transaction_id, status)
                VALUES ($1, $2)
                """
                await db.execute(transaction_query, str(transaction_id), "committing")
                
                # Process each table change
                for table_change in request.table_changes:
                    if not table_change.identifier:
                        raise ValueError("Table identifier is required for transaction changes")
                    
                    namespace_levels = table_change.identifier.namespace.__root__
                    table_name = table_change.identifier.name
                    
                    # Get namespace ID
                    namespace_id = await TableService._get_namespace_id(namespace_levels)
                    if namespace_id is None:
                        raise ValueError(f"Namespace not found: {namespace_levels}")
                    
                    # Get table ID
                    query = """
                    SELECT id FROM tables
                    WHERE namespace_id = $1 AND name = $2
                    """
                    table_record = await db.fetch_one(query, namespace_id, table_name)
                    if not table_record:
                        raise ValueError(f"Table not found: {namespace_levels}.{table_name}")
                    
                    table_id = table_record["id"]
                    
                    # Get full table record
                    query = """
                    SELECT * FROM tables WHERE id = $1
                    """
                    table_record = await db.fetch_one(query, table_id)
                    
                    # Check all requirements
                    for requirement in table_change.requirements:
                        if not await TableService._validate_requirement(table_id, table_record, requirement):
                            requirement_type = getattr(requirement, "type", "Unknown")
                            raise ValueError(f"Table requirement not met: {requirement_type}")
                    
                    # Apply all updates
                    for update in table_change.updates:
                        await TableService._apply_update(table_id, table_record, update.__root__)
                        
                        # Reload table record after each update
                        table_record = await db.fetch_one(query, table_id)
                
                # Mark transaction as completed
                completion_query = """
                UPDATE transactions
                SET status = $1, updated_at = NOW()
                WHERE transaction_id = $2
                """
                await db.execute(completion_query, "completed", str(transaction_id))
                
                logger.info(f"Successfully committed transaction {transaction_id}")
        
        except ValueError:
            # Re-raise ValueError
            raise
        except Exception as e:
            logger.error(f"Error processing transaction: {str(e)}", exc_info=True)
            raise
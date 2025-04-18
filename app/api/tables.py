# app/api/tables.py
from fastapi import APIRouter, HTTPException, Query, Header, Path
from typing import Optional, List
from app.models.base import IcebergErrorResponse
from app.models.namespace import Namespace
from app.models.table import (
    TableIdentifier, ListTablesResponse, CreateTableRequest, RegisterTableRequest,
    LoadTableResult, CommitTableRequest, CommitTableResponse, StorageCredential,
    LoadCredentialsResponse, ReportMetricsRequest, RenameTableRequest
)
from app.services.namespace import NamespaceService
from app.services.table import TableService
from app.utils.logger import logger
from fastapi.responses import Response
import json
router = APIRouter()

@router.get("/v1/{prefix}/namespaces/{namespace}/tables",
    response_model=ListTablesResponse,
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def list_tables(
    prefix: str,
    namespace: str,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None
):
    """
    List all table identifiers underneath a given namespace.
    """
    try:
        logger.info(f"List tables request. prefix: {prefix}, namespace: {namespace}, page_token: {page_token}, page_size: {page_size}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        return await TableService.list_tables(namespace_levels, page_token, page_size)
    except ValueError as e:
        # Handle namespace not found
        if "not found" in str(e).lower():
            logger.warning(f"Namespace not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchNamespaceException",
                        "code": 404
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error listing tables: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.post("/v1/{prefix}/namespaces/{namespace}/tables",
    response_model=LoadTableResult,
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        409: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def create_table(
    prefix: str,
    namespace: str,
    request: CreateTableRequest,
    x_iceberg_access_delegation: Optional[str] = Header(None)
):
    """
    Create a table in the given namespace.
    """
    try:
        logger.info(f"Create table request. prefix: {prefix}, namespace: {namespace}, table name: {request.name}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        result = await TableService.create_table(namespace_levels, request, x_iceberg_access_delegation)
        
        # Set ETag header
        etag = f'"{result.metadata.table_uuid}-{result.metadata.last_updated_ms}"'
        return Response(
            content=result.json(by_alias=True),
            media_type="application/json",
            headers={"ETag": etag}
        )
    except ValueError as e:
        # Handle specific errors
        if "not found" in str(e).lower() and "namespace" in str(e).lower():
            logger.warning(f"Namespace not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchNamespaceException",
                        "code": 404
                    }
                }
            )
        elif "already exists" in str(e).lower():
            logger.warning(f"Table already exists: {str(e)}")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "AlreadyExistsException",
                        "code": 409
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error creating table: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.get("/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    response_model=LoadTableResult,
    responses={
        304: {"description": "Not Modified"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def load_table(
    prefix: str,
    namespace: str,
    table: str,
    snapshots: Optional[str] = Query(None, regex="^(all|refs)$"),
    x_iceberg_access_delegation: Optional[str] = Header(None),
    if_none_match: Optional[str] = Header(None)
):
    """
    Load a table from the catalog.
    """
    try:
        logger.info(f"Load table request. prefix: {prefix}, namespace: {namespace}, table: {table}, snapshots: {snapshots}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        
        # We'll replace the standard call with our own implementation to handle 304 properly
        table_id, etag, table_metadata = await TableService.get_table_basic_info(namespace_levels, table, if_none_match)
        
        # Always load credentials regardless of 304 status
        config = await TableService.get_table_config(table_id)
        credentials = await TableService.get_storage_credentials(table_id)
        
        # If get_table_basic_info returned None for table_metadata, we need to return 304
        if table_metadata is None:
            # Return 304 with credentials in body
            logger.info(f"Table {namespace_levels}.{table} not modified, returning 304 with credentials")
            
            # Get the last successful response and add credentials
            cached_result = await TableService.get_cached_table_metadata(namespace_levels, table)
            if cached_result:
                cached_result["config"] = config
                # cached_result["storage-credentials"] = [c.dict(by_alias=True) for c in credentials]
                # cached_result["storage-credentials"] = [
                #     json.loads(StorageCredential(**cred).json()) 
                #     for cred in credentials
                # ]
                # Fix this line in the tables.py route handler
                cached_result["storage-credentials"] = [c.dict(by_alias=True) for c in credentials] if credentials else None
                
                return Response(
                    content=json.dumps(cached_result),
                    media_type="application/json",
                    status_code=200,
                    headers={"ETag": etag}
                )
            else:
                # No cached response, return standard 304
                return Response(status_code=304)
        
        # If we're here, we need to build the full response
        result = await TableService.build_table_response(table_id, table_metadata, snapshots)
        
        # Add config and credentials
        result_dict = result.dict(by_alias=True)
        result_dict["config"] = config
        result_dict["storage-credentials"] = [c.dict(by_alias=True) for c in credentials]
        
        # Cache this response for future 304 requests
        await TableService.cache_table_metadata(namespace_levels, table, result_dict)
        
        return Response(
            content=json.dumps(result_dict),
            media_type="application/json",
            headers={"ETag": etag}
        )
    except ValueError as e:
        # Handle table not found
        if "not found" in str(e).lower():
            logger.warning(f"Table not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchTableException",
                        "code": 404
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error loading table: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.head("/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    status_code=204,
    responses={
        204: {"description": "Success, no content"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def table_exists(
    prefix: str,
    namespace: str,
    table: str
):
    """
    Check if a table exists. The response does not contain a body.
    """
    try:
        logger.info(f"Check table exists request. prefix: {prefix}, namespace: {namespace}, table: {table}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        exists = await TableService.table_exists(namespace_levels, table)
        
        if not exists:
            logger.warning(f"Table not found: {namespace}.{table}")
            raise HTTPException(status_code=404)
        
        # 204 No Content is returned automatically for success
    except HTTPException:
        # Pass through HTTP exceptions
        raise
    except Exception as e:
        # Server error
        logger.error(f"Error checking table existence: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.delete("/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    status_code=204,
    responses={
        204: {"description": "Success, no content"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def drop_table(
    prefix: str,
    namespace: str,
    table: str,
    purge_requested: bool = Query(False, description="Whether to purge the underlying table's data and metadata")
):
    """
    Drop a table from the catalog.
    """
    try:
        logger.info(f"Drop table request. prefix: {prefix}, namespace: {namespace}, table: {table}, purge_requested: {purge_requested}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        await TableService.drop_table(namespace_levels, table, purge_requested)
        
        # 204 No Content is returned automatically for success
    except ValueError as e:
        # Handle specific errors
        if "not found" in str(e).lower() and "namespace" in str(e).lower():
            logger.warning(f"Namespace not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchNamespaceException",
                        "code": 404
                    }
                }
            )
        elif "not found" in str(e).lower() and "table" in str(e).lower():
            logger.warning(f"Table not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchTableException",
                        "code": 404
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error dropping table: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.get("/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    response_model=LoadCredentialsResponse,
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def load_credentials(
    prefix: str,
    namespace: str,
    table: str
):
    """
    Load vended credentials for a table from the catalog.
    """
    try:
        logger.info(f"Load credentials request. prefix: {prefix}, namespace: {namespace}, table: {table}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        return await TableService.load_credentials(namespace_levels, table)
    except ValueError as e:
        # Handle table not found
        if "not found" in str(e).lower():
            logger.warning(f"Table not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchTableException",
                        "code": 404
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error loading credentials: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.post("/v1/{prefix}/tables/rename",
    status_code=204,
    responses={
        204: {"description": "Success, no content"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        406: {"model": IcebergErrorResponse},
        409: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def rename_table(
    prefix: str,
    request: RenameTableRequest
):
    """
    Rename a table from its current name to a new name.
    """
    try:
        logger.info(f"Rename table request. prefix: {prefix}, source: {request.source.namespace.__root__}.{request.source.name}, destination: {request.destination.namespace.__root__}.{request.destination.name}")
        await TableService.rename_table(request)
        
        # 204 No Content is returned automatically for success
    except ValueError as e:
        # Handle specific errors
        if "not found" in str(e).lower() and "namespace" in str(e).lower():
            logger.warning(f"Namespace not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchNamespaceException",
                        "code": 404
                    }
                }
            )
        elif "not found" in str(e).lower() and "table" in str(e).lower():
            logger.warning(f"Table not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchTableException",
                        "code": 404
                    }
                }
            )
        elif "already exists" in str(e).lower():
            logger.warning(f"Table already exists: {str(e)}")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "AlreadyExistsException",
                        "code": 409
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error renaming table: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )

@router.post("/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    status_code=204,
    responses={
        204: {"description": "Success, no content"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def report_metrics(
    prefix: str,
    namespace: str,
    table: str,
    request: ReportMetricsRequest
):
    """
    Send a metrics report to this endpoint to be processed by the backend.
    """
    try:
        logger.info(f"Report metrics request. prefix: {prefix}, namespace: {namespace}, table: {table}, report_type: {request.report_type}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        await TableService.report_metrics(namespace_levels, table, request)
        
        # 204 No Content is returned automatically for success
    except ValueError as e:
        # Handle table not found
        if "not found" in str(e).lower():
            logger.warning(f"Table not found: {str(e)}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NoSuchTableException",
                        "code": 404
                    }
                }
            )
        else:
            # Other validation errors
            logger.warning(f"Bad request: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "BadRequestException",
                        "code": 400
                    }
                }
            )
    except Exception as e:
        # Server error
        logger.error(f"Error reporting metrics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )
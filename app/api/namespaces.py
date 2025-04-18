# app/api/namespaces.py
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from app.models.base import IcebergErrorResponse
from app.models.namespace import (
    Namespace, CreateNamespaceRequest, CreateNamespaceResponse,
    GetNamespaceResponse, ListNamespacesResponse, PageToken,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse
)
from app.services.namespace import NamespaceService
from app.utils.logger import logger

router = APIRouter()

@router.get("/v1/{prefix}/namespaces",
    response_model=ListNamespacesResponse,
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
async def list_namespaces(
    prefix: str,
    parent: Optional[str] = None,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None
):
    """
    List namespaces, optionally providing a parent namespace to list underneath.
    
    If table accounting.tax.paid.info exists, using 'SELECT NAMESPACE IN accounting' would
    translate into `GET /namespaces?parent=accounting` and must return a namespace, ["accounting", "tax"] only.
    Using 'SELECT NAMESPACE IN accounting.tax' would
    translate into `GET /namespaces?parent=accounting%1Ftax` and must return a namespace, ["accounting", "tax", "paid"].
    If `parent` is not provided, all top-level namespaces should be listed.
    """
    try:
        logger.info(f"List namespaces request. prefix: {prefix}, parent: {parent}, page_token: {page_token}, page_size: {page_size}")
        return await NamespaceService.list_namespaces(parent, page_token, page_size)
    except ValueError as e:
        # Client error (not found, invalid input)
        if "not found" in str(e).lower():
            logger.warning(f"Parent namespace not found: {str(e)}")
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
        logger.error(f"Error listing namespaces: {str(e)}", exc_info=True)
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

@router.post("/v1/{prefix}/namespaces",
    response_model=CreateNamespaceResponse,
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        406: {"model": IcebergErrorResponse},
        409: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def create_namespace(
    prefix: str,
    request: CreateNamespaceRequest
):
    """
    Create a namespace, with an optional set of properties.
    The server might also add properties, such as `last_modified_time` etc.
    """
    try:
        logger.info(f"Create namespace request. prefix: {prefix}, namespace: {request.namespace.__root__}")
        return await NamespaceService.create_namespace(request)
    except ValueError as e:
        # Handle namespace already exists
        if "already exists" in str(e).lower():
            logger.warning(f"Namespace already exists: {str(e)}")
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
        logger.error(f"Error creating namespace: {str(e)}", exc_info=True)
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

@router.get("/v1/{prefix}/namespaces/{namespace}",
    response_model=GetNamespaceResponse,
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
async def load_namespace_metadata(
    prefix: str,
    namespace: str
):
    """
    Load the metadata properties for a namespace
    """
    try:
        logger.info(f"Load namespace metadata request. prefix: {prefix}, namespace: {namespace}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        return await NamespaceService.get_namespace(namespace_levels)
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
        logger.error(f"Error getting namespace metadata: {str(e)}", exc_info=True)
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

@router.head("/v1/{prefix}/namespaces/{namespace}",
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
async def namespace_exists(
    prefix: str,
    namespace: str
):
    """
    Check if a namespace exists. The response does not contain a body.
    """
    try:
        logger.info(f"Check namespace exists request. prefix: {prefix}, namespace: {namespace}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        exists = await NamespaceService.namespace_exists(namespace_levels)
        
        if not exists:
            logger.warning(f"Namespace not found: {namespace}")
            raise HTTPException(status_code=404)
        
        # 204 No Content is returned automatically for success
    except HTTPException:
        # Pass through HTTP exceptions
        raise
    except Exception as e:
        # Server error
        logger.error(f"Error checking namespace existence: {str(e)}", exc_info=True)
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

@router.delete("/v1/{prefix}/namespaces/{namespace}",
    status_code=204,
    responses={
        204: {"description": "Success, no content"},
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
async def drop_namespace(
    prefix: str,
    namespace: str
):
    """
    Drop a namespace from the catalog. Namespace must be empty.
    """
    try:
        logger.info(f"Drop namespace request. prefix: {prefix}, namespace: {namespace}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        await NamespaceService.drop_namespace(namespace_levels)
        
        # 204 No Content is returned automatically for success
    except ValueError as e:
        # Handle specific errors
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
        elif "not empty" in str(e).lower():
            logger.warning(f"Namespace not empty: {str(e)}")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "NamespaceNotEmptyException",
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
        logger.error(f"Error dropping namespace: {str(e)}", exc_info=True)
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

@router.post("/v1/{prefix}/namespaces/{namespace}/properties",
    response_model=UpdateNamespacePropertiesResponse,
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        404: {"model": IcebergErrorResponse},
        406: {"model": IcebergErrorResponse},
        422: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def update_properties(
    prefix: str,
    namespace: str,
    request: UpdateNamespacePropertiesRequest
):
    """
    Set or remove properties on a namespace.
    """
    try:
        logger.info(f"Update namespace properties request. prefix: {prefix}, namespace: {namespace}")
        namespace_levels = NamespaceService.parse_namespace(namespace)
        return await NamespaceService.update_properties(namespace_levels, request)
    except ValueError as e:
        # Handle specific errors
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
        elif "cannot remove and update" in str(e).lower():
            logger.warning(f"Property key conflict: {str(e)}")
            raise HTTPException(
                status_code=422,
                detail={
                    "error": {
                        "message": str(e),
                        "type": "UnprocessableEntityException",
                        "code": 422
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
        logger.error(f"Error updating namespace properties: {str(e)}", exc_info=True)
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
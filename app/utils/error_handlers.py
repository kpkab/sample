# app/utils/error_handlers.py
from fastapi import HTTPException
from app.models.base import IcebergErrorResponse, ErrorModel

def create_error_response(status_code: int, message: str, error_type: str) -> IcebergErrorResponse:
    """Create a standardized error response"""
    return IcebergErrorResponse(
        error=ErrorModel(
            message=message,
            type=error_type,
            code=status_code
        )
    )

def not_found_error(resource_type: str, identifier: str) -> IcebergErrorResponse:
    """Create a not found error response"""
    return create_error_response(
        status_code=404,
        message=f"The given {resource_type} does not exist: {identifier}",
        error_type=f"NoSuch{resource_type.capitalize()}Exception"
    )

def conflict_error(resource_type: str, identifier: str) -> IcebergErrorResponse:
    """Create a conflict error response"""
    return create_error_response(
        status_code=409,
        message=f"The given {resource_type} already exists: {identifier}",
        error_type="AlreadyExistsException"
    )
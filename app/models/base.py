from typing import List, Optional
from pydantic import BaseModel, Field

class ErrorModel(BaseModel):
    """
    JSON error payload returned in a response with further details on the error
    """
    message: str = Field(..., description='Human-readable error message')
    type: str = Field(
        ...,
        description='Internal type definition of the error',
        example='NoSuchNamespaceException',
    )
    code: int = Field(
        ..., description='HTTP response code', example=404, ge=400, le=600
    )
    stack: Optional[List[str]] = None

class IcebergErrorResponse(BaseModel):
    """
    JSON wrapper for all error responses (non-2xx)
    """
    error: ErrorModel
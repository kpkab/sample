# app/models/namespace.py
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class Namespace(BaseModel):
    """Reference to one or more levels of a namespace"""
    __root__: List[str] = Field(
        ...,
        description='Reference to one or more levels of a namespace',
        example=['accounting', 'tax'],
    )

class CreateNamespaceRequest(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Configured string to string map of properties for the namespace',
        example={"owner": "Hank Bendickson"},
    )

class CreateNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Properties stored on the namespace, if supported by the server.',
        example={"owner": "Ralph", "created_at": "1452120468"},
    )

class GetNamespaceResponse(BaseModel):
    namespace: Namespace
    properties: Optional[Dict[str, str]] = Field(
        {},
        description='Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.',
        example={"owner": "Ralph", "transient_lastDdlTime": "1452120468"},
    )

class PageToken(BaseModel):
    __root__: Optional[str] = Field(
        None,
        description='An opaque token that allows clients to make use of pagination for list APIs',
    )

class ListNamespacesResponse(BaseModel):
    next_page_token: Optional[PageToken] = Field(None, alias='next-page-token')
    namespaces: Optional[List[Namespace]] = Field(None, unique_items=True)

class UpdateNamespacePropertiesRequest(BaseModel):
    removals: Optional[List[str]] = Field(
        None, example=['department', 'access_group'], unique_items=True
    )
    updates: Optional[Dict[str, str]] = Field(
        None, example={'owner': 'Hank Bendickson'}
    )

class UpdateNamespacePropertiesResponse(BaseModel):
    updated: List[str] = Field(
        ...,
        description='List of property keys that were added or updated',
        unique_items=True,
    )
    removed: List[str] = Field(..., description='List of properties that were removed')
    missing: Optional[List[str]] = Field(
        None,
        description="List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.",
    )
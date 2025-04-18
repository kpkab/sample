from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class CatalogConfig(BaseModel):
    """
    Server-provided configuration for the catalog.
    """
    overrides: Dict[str, str] = Field(
        ...,
        description='Properties that should be used to override client configuration; applied after defaults and client configuration.',
    )
    defaults: Dict[str, str] = Field(
        ...,
        description='Properties that should be used as default configuration; applied before client configuration.',
    )
    endpoints: Optional[List[str]] = Field(
        None,
        description='A list of endpoints that the server supports.',
        example=[
            'GET /v1/{prefix}/namespaces/{namespace}',
            'GET /v1/{prefix}/namespaces',
            'POST /v1/{prefix}/namespaces',
        ],
    )
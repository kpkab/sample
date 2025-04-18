from typing import Dict, Optional
from pydantic import BaseModel, Field

class CredentialRequest(BaseModel):
    prefix: str = Field(..., description="Organization unit prefix (e.g., dev, test, hr)")
    warehouse: str = Field(..., description="Storage location (e.g., s3://bucket/path/)")
    config: Dict[str, str] = Field(..., description="Credential configuration")
    table_id: Optional[int] = Field(None, description="Optional table ID for table-specific credentials")
    overwrite: bool = Field(False, description="Whether to overwrite existing credentials")
    
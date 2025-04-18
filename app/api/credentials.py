from fastapi import APIRouter, HTTPException, Response
from app.models.base import IcebergErrorResponse
from app.models.credentials import CredentialRequest
from app.services.credential import CredentialService
from app.utils.logger import logger

router = APIRouter()

@router.post("/v1/{prefix}/credentials",
    status_code=201,
    responses={
        201: {"description": "Credentials created successfully"},
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        409: {"model": IcebergErrorResponse}
    }
)
async def create_credentials(
    prefix: str,
    request: CredentialRequest
):
    """Create or update storage credentials."""
    try:
        logger.info(f"Creating credentials for prefix: {request.prefix}, warehouse: {request.warehouse}")
        
        # Check if credentials already exist
        existing = await CredentialService.get_credentials(
            request.prefix, 
            request.warehouse,
            request.table_id
        )
        
        if existing and not request.overwrite:
            logger.warning(f"Credentials already exist for prefix: {request.prefix}, warehouse: {request.warehouse}")
            raise HTTPException(
                status_code=409,
                detail={
                    "error": {
                        "message": f"Credentials already exist. Set overwrite=true to update.",
                        "type": "AlreadyExistsException",
                        "code": 409
                    }
                }
            )
            
        # Create or update credentials
        cred_id = await CredentialService.upsert_credentials(
            request.prefix,
            request.warehouse,
            request.config,
            request.table_id
        )
        
        logger.info(f"Credentials created/updated with ID: {cred_id}")
        return Response(status_code=201)
    except HTTPException:
        raise
    except ValueError as e:
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
        logger.error(f"Error creating credentials: {str(e)}", exc_info=True)
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
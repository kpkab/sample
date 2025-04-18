# app/api/config.py
from fastapi import APIRouter, HTTPException
from typing import Optional
from app.models.base import IcebergErrorResponse
from app.models.config import CatalogConfig
from app.services.config import ConfigService
from app.utils.logger import logger

router = APIRouter()

@router.get("/v1/config", 
    response_model=CatalogConfig, 
    responses={
        400: {"model": IcebergErrorResponse},
        401: {"model": IcebergErrorResponse},
        403: {"model": IcebergErrorResponse},
        419: {"model": IcebergErrorResponse},
        503: {"model": IcebergErrorResponse},
        500: {"model": IcebergErrorResponse}
    }
)
async def get_config(warehouse: Optional[str] = None):
    """
    List all catalog configuration settings.
    
    If a warehouse is specified, configuration specific to that warehouse is returned.
    Otherwise, the default configuration is returned.
    """
    try:
        logger.info(f"Received request for configuration. Warehouse: {warehouse}")
        config = await ConfigService.get_config(warehouse)
        logger.info("Successfully retrieved configuration")
        return config
    except Exception as e:
        logger.error(f"Error handling configuration request: {str(e)}", exc_info=True)
        
        # Return a 500 error
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": f"Internal server error while fetching configuration: {str(e)}",
                    "type": "InternalServerError",
                    "code": 500
                }
            }
        )
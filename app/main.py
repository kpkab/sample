# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.middleware.prefix_middleware import PrefixMiddleware 
import traceback
import os

from app.database import db
from app.api import config, namespaces, tables
from app.utils.logger import logger
# Import other API routers here as needed

# Create FastAPI app instance
app = FastAPI(
    title="Apache Iceberg REST Catalog API",
    description="Implementation of the Apache Iceberg REST Catalog API",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add our custom prefix rewriting middleware
app.add_middleware(PrefixMiddleware)

# Register routers
app.include_router(config.router, tags=["Configuration API"])
app.include_router(namespaces.router, tags=["Namespace API"])
app.include_router(tables.router, tags=["Table API"])
# Include other routers here

# Enable/disable debug based on environment
app.debug = os.getenv("ENVIRONMENT", "production").lower() == "development"

# Startup and shutdown events
@app.on_event("startup")
async def startup():
    logger.info("Starting up application")
    await db.connect()
    logger.info("Database connection established")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down application")
    await db.disconnect()
    logger.info("Database connection closed")

# Exception handler for IcebergErrorResponse format
@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception):
    status_code = 500
    if hasattr(exc, "status_code"):
        status_code = exc.status_code
    
    error_type = type(exc).__name__
    error_message = str(exc)
    
    logger.error(f"Unhandled exception: {error_message}", exc_info=True)
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "message": error_message,
                "type": error_type,
                "code": status_code,
                "stack": traceback.format_exception(type(exc), exc, exc.__traceback__) if app.debug else None
            }
        }
    )
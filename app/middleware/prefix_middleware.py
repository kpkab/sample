# app/middleware/prefix_middleware.py
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from fastapi import FastAPI
import re
from app.utils.logger import logger

class PrefixMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI):
        super().__init__(app)
        # Regex to match /{prefix}/v1/config pattern (special case)
        self.config_pattern = re.compile(r'^/([^/]+)/v1/config$')
        # Regex to match /{prefix}/v1/... pattern (for all other endpoints)
        self.client_pattern = re.compile(r'^/([^/]+)/v1/(.+)$')
        
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        
        # Special case for config endpoint
        config_match = self.config_pattern.match(path)
        if config_match:
            prefix = config_match.group(1)
            if prefix != 'v1':  # Skip if already in correct format
                # For config endpoint, the prefix becomes a warehouse parameter
                new_path = "/v1/config"
                logger.info(f"Rewriting config path from '{path}' to '{new_path}' with warehouse={prefix}")
                
                # Create modified request scope with new path
                request.scope["path"] = new_path
                request.scope["raw_path"] = new_path.encode()
                
                # Add prefix as warehouse query parameter
                if "query_string" in request.scope:
                    existing_query = request.scope["query_string"].decode() if request.scope["query_string"] else ""
                    if existing_query:
                        if "warehouse=" in existing_query:
                            # Don't overwrite existing warehouse parameter
                            pass
                        else:
                            new_query = f"{existing_query}&warehouse={prefix}"
                            request.scope["query_string"] = new_query.encode()
                    else:
                        new_query = f"warehouse={prefix}"
                        request.scope["query_string"] = new_query.encode()
                
                return await call_next(request)
        
        # Regular endpoints with prefix
        match = self.client_pattern.match(path)
        if match:
            # Extract prefix and the rest of the path
            prefix = match.group(1)
            rest_of_path = match.group(2)
            
            # Skip rewriting for paths that are already in API format
            if prefix == 'v1':
                return await call_next(request)
            
            # Rewrite to /v1/{prefix}/...
            new_path = f"/v1/{prefix}/{rest_of_path}"
            
            logger.info(f"Rewriting path from '{path}' to '{new_path}'")
            
            # Create modified request scope with new path
            request.scope["path"] = new_path
            request.scope["raw_path"] = new_path.encode()
        
        # Continue with the request
        return await call_next(request)
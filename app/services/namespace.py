# app/services/namespace.py
import json
from typing import Dict, List, Optional, Tuple, Any
from app.database import db
from app.models.namespace import (
    Namespace, CreateNamespaceRequest, CreateNamespaceResponse,
    GetNamespaceResponse, ListNamespacesResponse, PageToken,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse
)
from app.utils.logger import logger
import base64

class NamespaceService:
    
    @staticmethod
    def encode_page_token(value: str) -> str:
        """Encode a page token value"""
        return base64.b64encode(value.encode()).decode()
    
    @staticmethod
    def decode_page_token(token: str) -> str:
        """Decode a page token value"""
        return base64.b64decode(token.encode()).decode()
    
    @staticmethod
    def parse_namespace(namespace_str: str) -> List[str]:
        """
        Parse a namespace string from a URL path parameter.
        Handles unit separator (\x1F) escaping as %1F.
        """
        if not namespace_str:
            return []
            
        # Replace %1F with unit separator character
        if '%1F' in namespace_str:
            return namespace_str.replace('%1F', '\x1F').split('\x1F')
        
        # If no separator found, treat as a single namespace part
        return [namespace_str]
    
    @staticmethod
    async def list_namespaces(
        parent: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: Optional[int] = None
    ) -> ListNamespacesResponse:
        """
        List all namespaces at a certain level, optionally under a parent namespace.
        Supports pagination.
        """
        logger.info(f"Listing namespaces. Parent: {parent}, Page token: {page_token}, Page size: {page_size}")
        
        # Parse parent namespace if provided
        parent_levels = None
        if parent:
            parent_levels = NamespaceService.parse_namespace(parent)
            
            # Verify parent namespace exists
            parent_exists = await NamespaceService.namespace_exists(parent_levels)
            if not parent_exists:
                logger.warning(f"Parent namespace not found: {parent_levels}")
                raise ValueError(f"Parent namespace not found: {parent}")
        
        # Start building query
        query = "SELECT levels FROM namespaces"
        params = []
        
        # Add parent namespace filter if provided
        if parent_levels:
            query += " WHERE levels @> $1 AND array_length(levels, 1) = $2"
            params.extend([parent_levels, len(parent_levels) + 1])
        
        # Handle pagination
        if page_token:
            try:
                last_seen = NamespaceService.decode_page_token(page_token)
                where_clause = " WHERE" if "WHERE" not in query else " AND"
                query += f"{where_clause} levels > $%s"
                params.append(last_seen)
            except Exception as e:
                logger.error(f"Invalid page token: {page_token}", exc_info=True)
                raise ValueError(f"Invalid page token: {page_token}")
        
        # Add ordering
        query += " ORDER BY levels"
        
        # Add limit for pagination
        if page_size:
            # Request one more than needed to check if there are more results
            query += f" LIMIT ${len(params) + 1}"
            params.append(page_size + 1)
        
        # Execute query
        try:
            namespace_records = await db.fetch_all(query, *params)
            
            # Handle pagination
            has_more = False
            if page_size and len(namespace_records) > page_size:
                has_more = True
                namespace_records = namespace_records[:page_size]
            
            # Convert to model
            namespaces = [Namespace(__root__=record["levels"]) for record in namespace_records]
            
            # Build response
            response = ListNamespacesResponse(namespaces=namespaces)
            
            # Add next page token if there are more results
            if has_more:
                last_namespace = namespace_records[-1]["levels"]
                next_token = NamespaceService.encode_page_token(last_namespace)
                response.next_page_token = PageToken(__root__=next_token)
            
            return response
            
        except Exception as e:
            logger.error(f"Error listing namespaces: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def create_namespace(request: CreateNamespaceRequest) -> CreateNamespaceResponse:
        """
        Create a new namespace with optional properties.
        """
        logger.info(f"Creating namespace: {request.namespace.__root__}")
        
        # Check if namespace already exists
        namespace_exists = await NamespaceService.namespace_exists(request.namespace.__root__)
        if namespace_exists:
            logger.warning(f"Namespace already exists: {request.namespace.__root__}")
            raise ValueError(f"Namespace already exists: {request.namespace.__root__}")
        
        # Insert new namespace
        query = """
        INSERT INTO namespaces (levels, properties)
        VALUES ($1, $2)
        RETURNING id
        """
        
        properties = request.properties or {}
        
        try:
            await db.execute(query, request.namespace.__root__, json.dumps(properties))
            
            # Return the created namespace
            return CreateNamespaceResponse(
                namespace=request.namespace,
                properties=properties
            )
        except Exception as e:
            logger.error(f"Error creating namespace: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def get_namespace(namespace_levels: List[str]) -> GetNamespaceResponse:
        """
        Get metadata for a specific namespace.
        """
        logger.info(f"Getting namespace metadata: {namespace_levels}")
        
        query = """
        SELECT levels, properties FROM namespaces
        WHERE levels = $1
        """
        
        try:
            namespace_record = await db.fetch_one(query, namespace_levels)
            
            if not namespace_record:
                logger.warning(f"Namespace not found: {namespace_levels}")
                raise ValueError(f"Namespace not found: {namespace_levels}")
            
            # Parse properties if it's a string
            properties = namespace_record["properties"]
            if isinstance(properties, str):
                properties = json.loads(properties)
            
            return GetNamespaceResponse(
                namespace=Namespace(__root__=namespace_record["levels"]),
                properties=properties
            )
        except ValueError:
            # Re-raise ValueError for not found
            raise
        except Exception as e:
            logger.error(f"Error getting namespace: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def namespace_exists(namespace_levels: List[str]) -> bool:
        """
        Check if a namespace exists.
        """
        logger.info(f"Checking if namespace exists: {namespace_levels}")
        
        query = """
        SELECT EXISTS(SELECT 1 FROM namespaces WHERE levels = $1)
        """
        
        try:
            result = await db.fetch_one(query, namespace_levels)
            return result and result["exists"]
        except Exception as e:
            logger.error(f"Error checking namespace existence: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def drop_namespace(namespace_levels: List[str]) -> None:
        """
        Drop a namespace. Namespace must be empty.
        """
        logger.info(f"Dropping namespace: {namespace_levels}")
        
        # First check if namespace exists
        exists = await NamespaceService.namespace_exists(namespace_levels)
        if not exists:
            logger.warning(f"Namespace not found: {namespace_levels}")
            raise ValueError(f"Namespace not found: {namespace_levels}")
        
        # Check if namespace has any tables or views
        query = """
        SELECT (
            SELECT EXISTS(SELECT 1 FROM tables WHERE namespace_id = (SELECT id FROM namespaces WHERE levels = $1))
            OR
            EXISTS(SELECT 1 FROM views WHERE namespace_id = (SELECT id FROM namespaces WHERE levels = $1))
        ) as has_children
        """
        
        try:
            result = await db.fetch_one(query, namespace_levels)
            
            if result and result["has_children"]:
                logger.warning(f"Cannot drop namespace, it is not empty: {namespace_levels}")
                raise ValueError(f"Namespace is not empty: {namespace_levels}")
            
            # Delete the namespace
            delete_query = """
            DELETE FROM namespaces WHERE levels = $1
            """
            
            await db.execute(delete_query, namespace_levels)
            
        except ValueError:
            # Re-raise ValueError for not found or not empty
            raise
        except Exception as e:
            logger.error(f"Error dropping namespace: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def update_properties(
        namespace_levels: List[str],
        request: UpdateNamespacePropertiesRequest
    ) -> UpdateNamespacePropertiesResponse:
        """
        Set or remove properties on a namespace.
        """
        logger.info(f"Updating namespace properties: {namespace_levels}")
        
        # Check if namespace exists
        exists = await NamespaceService.namespace_exists(namespace_levels)
        if not exists:
            logger.warning(f"Namespace not found: {namespace_levels}")
            raise ValueError(f"Namespace not found: {namespace_levels}")
        
        # Check if any property key appears in both removals and updates
        removals = request.removals or []
        updates = request.updates or {}
        
        common_keys = set(removals).intersection(set(updates.keys()))
        if common_keys:
            logger.warning(f"Property keys in both removals and updates: {common_keys}")
            raise ValueError(f"Cannot remove and update the same property keys: {common_keys}")
        
        # Get current properties
        query = """
        SELECT properties FROM namespaces WHERE levels = $1
        """
        
        try:
            namespace_record = await db.fetch_one(query, namespace_levels)
            
            # Parse properties if it's a string
            properties = namespace_record["properties"]
            if isinstance(properties, str):
                properties = json.loads(properties)
            
            # Track missing properties (requested for removal but not found)
            missing_keys = []
            
            # Process removals
            removed_keys = []
            for key in removals:
                if key in properties:
                    del properties[key]
                    removed_keys.append(key)
                else:
                    missing_keys.append(key)
            
            # Process updates
            updated_keys = []
            for key, value in updates.items():
                properties[key] = value
                updated_keys.append(key)
            
            # Update namespace properties
            update_query = """
            UPDATE namespaces
            SET properties = $1, updated_at = NOW()
            WHERE levels = $2
            """
            
            await db.execute(update_query, json.dumps(properties), namespace_levels)
            
            # Prepare response
            response = UpdateNamespacePropertiesResponse(
                updated=updated_keys,
                removed=removed_keys
            )
            
            if missing_keys:
                response.missing = missing_keys
                
            return response
            
        except ValueError:
            # Re-raise ValueError for not found
            raise
        except Exception as e:
            logger.error(f"Error updating namespace properties: {str(e)}", exc_info=True)
            raise
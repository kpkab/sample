import json
from typing import Dict, List, Optional
from app.database import db
from app.utils.logger import logger

class CredentialService:
    @staticmethod
    async def get_credentials(
        prefix: str,
        warehouse: str,
        table_id: Optional[int] = None
    ) -> Optional[Dict]:
        """Get credentials for a specific prefix, warehouse, and optional table_id."""
        query = """
        SELECT id, prefix, warehouse, config
        FROM storage_credentials
        WHERE prefix = $1 AND warehouse = $2
        """
        
        params = [prefix, warehouse]
        
        if table_id is not None:
            query += " AND table_id = $3"
            params.append(table_id)
        else:
            query += " AND table_id IS NULL"
        
        try:
            record = await db.fetch_one(query, *params)
            return record if record else None
        except Exception as e:
            logger.error(f"Error retrieving credentials: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def upsert_credentials(
        prefix: str,
        warehouse: str,
        config: Dict[str, str],
        table_id: Optional[int] = None
    ) -> int:
        """Create or update credentials."""
        try:
            # Check if credentials exist
            existing = await CredentialService.get_credentials(prefix, warehouse, table_id)
            
            if existing:
                # Update existing credentials
                query = """
                UPDATE storage_credentials
                SET config = $1, updated_at = NOW()
                WHERE id = $2
                RETURNING id
                """
                result = await db.fetch_one(query, json.dumps(config), existing["id"])
                return result["id"]
            else:
                # Insert new credentials
                query = """
                INSERT INTO storage_credentials (prefix, warehouse, config, table_id)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """
                result = await db.fetch_one(query, prefix, warehouse, json.dumps(config), table_id)
                return result["id"]
        except Exception as e:
            logger.error(f"Error upserting credentials: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    async def get_credentials_for_location(
        location: str
    ) -> List[Dict]:
        """Get credentials for a specific storage location by prefix matching."""
        query = """
        SELECT id, prefix, warehouse, config
        FROM storage_credentials
        WHERE table_id IS NULL AND
              $1 LIKE CONCAT(warehouse, '%')
        ORDER BY LENGTH(warehouse) DESC
        """
        
        try:
            records = await db.fetch_all(query, location)
            return records
        except Exception as e:
            logger.error(f"Error retrieving credentials for location: {str(e)}", exc_info=True)
            raise
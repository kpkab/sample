# app/services/config.py
import json
from typing import Optional
from app.database import db
from app.models.config import CatalogConfig
from app.utils.logger import logger

class ConfigService:
    @staticmethod
    async def get_config(warehouse: Optional[str] = None) -> CatalogConfig:
        """
        Get catalog configuration.
        
        If warehouse is specified, return configuration for that specific warehouse.
        Otherwise, return the default configuration.
        """
        # Query to fetch configuration from the database
        config_query = """
        SELECT config_json FROM catalog_config 
        WHERE catalog_name = $1
        """
        
        # Use the provided warehouse or fall back to 'default'
        catalog_name = warehouse or "default"
        
        logger.info(f"Fetching configuration for catalog: {catalog_name}")
        
        try:
            # Execute the query
            config_data = await db.fetch_one(config_query, catalog_name)
            
            if config_data:
                logger.debug(f"Found configuration data: {config_data}")
                
                # Extract the configuration JSON and ensure it's a dictionary
                config_json = config_data["config_json"]
                
                # If it's a string, parse it into a dictionary
                if isinstance(config_json, str):
                    logger.debug("Parsing JSON string into dictionary")
                    config_json = json.loads(config_json)
                
                # Return CatalogConfig
                return CatalogConfig(
                    overrides=config_json.get("overrides", {}),
                    defaults=config_json.get("defaults", {}),
                    endpoints=config_json.get("endpoints")
                )
            else:
                logger.info(f"No configuration found for catalog: {catalog_name}")
                
                # If the specific warehouse configuration is not found, try to fetch the default
                if warehouse:
                    logger.info("Trying to fetch default configuration")
                    config_data = await db.fetch_one(config_query, "default")
                    if config_data:
                        logger.debug(f"Found default configuration: {config_data}")
                        config_json = config_data["config_json"]
                        
                        # If it's a string, parse it into a dictionary
                        if isinstance(config_json, str):
                            logger.debug("Parsing JSON string into dictionary")
                            config_json = json.loads(config_json)
                        
                        return CatalogConfig(
                            overrides=config_json.get("overrides", {}),
                            defaults=config_json.get("defaults", {}),
                            endpoints=config_json.get("endpoints")
                        )
                
                logger.warning("No configuration found, returning empty config")
                # Return empty configuration if nothing found
                return CatalogConfig(overrides={}, defaults={}, endpoints=[])
        except Exception as e:
            logger.error(f"Error fetching configuration: {str(e)}", exc_info=True)
            raise
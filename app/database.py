# app/database.py
import asyncpg
import os
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from app.utils.logger import logger

class Database:
    def __init__(self, connection_string: Optional[str] = None):
        connection_str = connection_string or os.getenv("DATABASE_URL", "postgresql://iceberg:password@postgres:5432/iceberg_catalog")
        
        # Make sure we're using the correct scheme for asyncpg
        if connection_str.startswith('postgresql+asyncpg'):
            connection_str = connection_str.replace('postgresql+asyncpg', 'postgresql')
        
        self.connection_string = connection_str
        logger.info(f"Initializing database with connection string: {self.connection_string.split('@')[1]}")  # Don't log credentials
        self.pool = None

    async def connect(self):
        """Establish connection pool to the database"""
        logger.info("Connecting to database...")
        try:
            if self.pool is None:
                self.pool = await asyncpg.create_pool(self.connection_string)
                logger.info("Successfully connected to database")
            else:
                logger.info("Connection pool already exists")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}", exc_info=True)
            raise

    async def disconnect(self):
        """Close all connections in the pool"""
        logger.info("Disconnecting from database...")
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connections closed")
        else:
            logger.info("No active connection pool to close")

    async def fetch_one(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute a query and return one result as a dictionary"""
        logger.debug(f"Executing fetch_one query: {query}")
        if not self.pool:
            logger.info("No active connection pool, connecting now")
            await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                record = await conn.fetchrow(query, *args)
                logger.debug(f"Query result: {record is not None}")
                if record:
                    return dict(record.items())
                return None
        except Exception as e:
            logger.error(f"Database query error: {str(e)}", exc_info=True)
            raise

    async def fetch_all(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a query and return all results as dictionaries"""
        logger.debug(f"Executing fetch_all query: {query}")
        if not self.pool:
            logger.info("No active connection pool, connecting now")
            await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *args)
                logger.debug(f"Query returned {len(records)} records")
                return [dict(record.items()) for record in records]
        except Exception as e:
            logger.error(f"Database query error: {str(e)}", exc_info=True)
            raise

    async def execute(self, query: str, *args) -> str:
        """Execute a query without returning results"""
        logger.debug(f"Executing query: {query}")
        if not self.pool:
            logger.info("No active connection pool, connecting now")
            await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *args)
                logger.debug(f"Query execution result: {result}")
                return result
        except Exception as e:
            logger.error(f"Database query error: {str(e)}", exc_info=True)
            raise

    @asynccontextmanager
    async def transaction(self):
        """Start a transaction context"""
        logger.debug("Starting database transaction")
        if not self.pool:
            logger.info("No active connection pool, connecting now")
            await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    logger.debug("Transaction started successfully")
                    yield conn
                    logger.debug("Transaction completed successfully")
        except Exception as e:
            logger.error(f"Transaction error: {str(e)}", exc_info=True)
            raise

# Create a single database instance to be used throughout the application
db = Database()
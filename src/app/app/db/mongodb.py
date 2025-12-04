"""
MongoDB integration module for weather analytics
"""
import os
from datetime import datetime
from typing import Optional

from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


class MongoDBClient:
    """MongoDB client wrapper for weather data storage"""
    
    _instance: Optional["MongoDBClient"] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None
    _collection: Optional[Collection] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        mongo_config = {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin"),
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", "27017")),
            "authSource": "admin",
        }
        
        logger.info(f"Connecting to MongoDB at {mongo_config['host']}:{mongo_config['port']}")
        
        try:
            self._client = MongoClient(**mongo_config)
            self._db = self._client["weather_analytics_db"]
            self._collection = self._db["weather_data"]
            
            # Test connection
            self._client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            # Don't raise exception, allow app to run without MongoDB
            self._client = None
    
    @property
    def collection(self) -> Optional[Collection]:
        """Get MongoDB collection"""
        return self._collection
    
    def save_weather_request(
        self,
        latitude: float,
        longitude: float,
        request_data: dict,
        response_data: dict,
        status_code: int,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Save weather request and response to MongoDB
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            request_data: Request parameters
            response_data: API response data
            status_code: HTTP status code
            error_message: Error message if request failed
            
        Returns:
            True if saved successfully, False otherwise
        """
        if self._collection is None:
            logger.warning("MongoDB collection not available, skipping save")
            return False
        
        try:
            document = {
                "latitude": latitude,
                "longitude": longitude,
                "request": request_data,
                "response": response_data,
                "status_code": status_code,
                "error_message": error_message,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            self._collection.insert_one(document)
            logger.info(f"Saved weather data to MongoDB for lat={latitude}, lon={longitude}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save to MongoDB: {e}")
            return False
    
    def get_recent_weather_data(self, limit: int = 10) -> list:
        """
        Get recent weather data from MongoDB
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of weather data documents
        """
        if self._collection is None:
            logger.warning("MongoDB collection not available")
            return []
        
        try:
            cursor = self._collection.find(
                {},
                sort=[("created_at", -1)],
                limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to fetch from MongoDB: {e}")
            return []
    
    def get_weather_by_location(
        self,
        latitude: float,
        longitude: float,
        limit: int = 10
    ) -> list:
        """
        Get weather data for specific location
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            limit: Maximum number of records to return
            
        Returns:
            List of weather data documents for the location
        """
        if self._collection is None:
            logger.warning("MongoDB collection not available")
            return []
        
        try:
            cursor = self._collection.find(
                {
                    "latitude": latitude,
                    "longitude": longitude,
                    "status_code": 200
                },
                sort=[("created_at", -1)],
                limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to fetch from MongoDB: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")


# Global instance
mongo_client = MongoDBClient()

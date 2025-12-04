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
    _weather_collection: Optional[Collection] = None
    _forecast_collection: Optional[Collection] = None
    
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
            self._weather_collection = self._db["weather_current"]
            self._forecast_collection = self._db["weather_forecast"]
            
            # Test connection
            self._client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            # Don't raise exception, allow app to run without MongoDB
            self._client = None
    
    @property
    def weather_collection(self) -> Optional[Collection]:
        """Get current weather MongoDB collection"""
        return self._weather_collection
    
    @property
    def forecast_collection(self) -> Optional[Collection]:
        """Get forecast MongoDB collection"""
        return self._forecast_collection
    
    def save_current_weather(
        self,
        latitude: float,
        longitude: float,
        request_data: dict,
        response_data: dict,
        status_code: int,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Save current weather request and response to MongoDB
        
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
        if self._weather_collection is None:
            logger.warning("MongoDB weather collection not available, skipping save")
            return False
        
        try:
            document = {
                "type": "current",
                "latitude": latitude,
                "longitude": longitude,
                "request": request_data,
                "response": response_data,
                "status_code": status_code,
                "error_message": error_message,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            self._weather_collection.insert_one(document)
            logger.info(f"Saved current weather to MongoDB for lat={latitude}, lon={longitude}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save current weather to MongoDB: {e}")
            return False
    
    def save_forecast(
        self,
        latitude: float,
        longitude: float,
        hours: int,
        request_data: dict,
        response_data: dict,
        status_code: int,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Save forecast request and response to MongoDB
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            hours: Forecast horizon in hours
            request_data: Request parameters
            response_data: API response data
            status_code: HTTP status code
            error_message: Error message if request failed
            
        Returns:
            True if saved successfully, False otherwise
        """
        if self._forecast_collection is None:
            logger.warning("MongoDB forecast collection not available, skipping save")
            return False
        
        try:
            document = {
                "type": "forecast",
                "latitude": latitude,
                "longitude": longitude,
                "hours": hours,
                "request": request_data,
                "response": response_data,
                "status_code": status_code,
                "error_message": error_message,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            self._forecast_collection.insert_one(document)
            logger.info(f"Saved forecast to MongoDB for lat={latitude}, lon={longitude}, hours={hours}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save forecast to MongoDB: {e}")
            return False
    
    def get_recent_current_weather(self, limit: int = 10) -> list:
        """
        Get recent current weather data from MongoDB
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of current weather data documents
        """
        if self._weather_collection is None:
            logger.warning("MongoDB weather collection not available")
            return []
        
        try:
            cursor = self._weather_collection.find(
                {},
                sort=[("created_at", -1)],
                limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to fetch current weather from MongoDB: {e}")
            return []
    
    def get_recent_forecasts(self, limit: int = 10) -> list:
        """
        Get recent forecast data from MongoDB
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of forecast data documents
        """
        if self._forecast_collection is None:
            logger.warning("MongoDB forecast collection not available")
            return []
        
        try:
            cursor = self._forecast_collection.find(
                {},
                sort=[("created_at", -1)],
                limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to fetch forecasts from MongoDB: {e}")
            return []
    
    def get_current_weather_by_location(
        self,
        latitude: float,
        longitude: float,
        limit: int = 10
    ) -> list:
        """
        Get current weather data for specific location
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            limit: Maximum number of records to return
            
        Returns:
            List of current weather data documents for the location
        """
        if self._weather_collection is None:
            logger.warning("MongoDB weather collection not available")
            return []
        
        try:
            cursor = self._weather_collection.find(
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
            logger.error(f"Failed to fetch current weather from MongoDB: {e}")
            return []
    
    def get_forecasts_by_location(
        self,
        latitude: float,
        longitude: float,
        limit: int = 10
    ) -> list:
        """
        Get forecast data for specific location
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            limit: Maximum number of records to return
            
        Returns:
            List of forecast data documents for the location
        """
        if self._forecast_collection is None:
            logger.warning("MongoDB forecast collection not available")
            return []
        
        try:
            cursor = self._forecast_collection.find(
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
            logger.error(f"Failed to fetch forecasts from MongoDB: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")


# Global instance
mongo_client = MongoDBClient()

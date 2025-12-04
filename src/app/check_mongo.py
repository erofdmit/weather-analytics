"""
Скрипт для проверки подключения к MongoDB и просмотра данных
"""
import json
import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Конфигурация MongoDB
MONGO_CONFIG = {
    "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
    "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
    "host": os.getenv("MONGO_HOST"),
    "port": int(os.getenv("MONGO_PORT")),
    "authSource": "admin",
}

print(f"Connecting to MongoDB: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}")

try:
    client = MongoClient(**MONGO_CONFIG)
    
    # Проверка подключения
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB")
    
    # Получение базы данных и коллекций
    db = client["weather_analytics_db"]
    weather_collection = db["weather_current"]
    forecast_collection = db["weather_forecast"]
    
    # Подсчет документов
    weather_count = weather_collection.count_documents({})
    forecast_count = forecast_collection.count_documents({})
    print(f"\n📊 Statistics:")
    print(f"  Current weather records: {weather_count}")
    print(f"  Forecast records: {forecast_count}")
    print(f"  Total: {weather_count + forecast_count}")
    
    # Current weather
    if weather_count > 0:
        print("\n📝 Recent current weather (last 3 records):")
        print("=" * 80)
        
        results = weather_collection.find(
            {},
            sort=[("created_at", -1)],
            limit=3
        )
        
        for i, doc in enumerate(results, 1):
            print(f"\n--- Current Weather #{i} ---")
            print(f"Latitude: {doc.get('latitude')}, Longitude: {doc.get('longitude')}")
            print(f"Created At: {doc.get('created_at')}")
            
            if 'response' in doc and 'average_temperature_c' in doc['response']:
                print(f"Temperature: {doc['response']['average_temperature_c']}°C")
                print(f"Samples: {len(doc['response'].get('samples', []))}")
            print("-" * 80)
    
    # Forecasts
    if forecast_count > 0:
        print("\n🔮 Recent forecasts (last 3 records):")
        print("=" * 80)
        
        results = forecast_collection.find(
            {},
            sort=[("created_at", -1)],
            limit=3
        )
        
        for i, doc in enumerate(results, 1):
            print(f"\n--- Forecast #{i} ---")
            print(f"Latitude: {doc.get('latitude')}, Longitude: {doc.get('longitude')}")
            print(f"Horizon: {doc.get('hours')} hours")
            print(f"Created At: {doc.get('created_at')}")
            
            if 'response' in doc and 'forecasts' in doc['response']:
                print(f"Providers: {len(doc['response']['forecasts'])}")
            print("-" * 80)
    
    # Статистика по локациям (current weather)
    if weather_count > 0:
        print("\n📍 Current weather by location:")
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "lat": "$latitude",
                        "lon": "$longitude"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        location_stats = list(weather_collection.aggregate(pipeline))
        
        for stat in location_stats:
            loc = stat['_id']
            print(f"  • Lat: {loc['lat']}, Lon: {loc['lon']} - {stat['count']} records")
    
    # Статистика по локациям (forecasts)
    if forecast_count > 0:
        print("\n🔮 Forecasts by location:")
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "lat": "$latitude",
                        "lon": "$longitude",
                        "hours": "$hours"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        location_stats = list(forecast_collection.aggregate(pipeline))
        
        for stat in location_stats:
            loc = stat['_id']
            print(f"  • Lat: {loc['lat']}, Lon: {loc['lon']}, {loc['hours']}h - {stat['count']} records")
    
    client.close()
    print("\n✅ MongoDB connection closed")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    exit(1)


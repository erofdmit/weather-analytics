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
    "username": os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin"),
    "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin123"),
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": int(os.getenv("MONGO_PORT", "27017")),
    "authSource": "admin",
}

print(f"Connecting to MongoDB: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}")

try:
    client = MongoClient(**MONGO_CONFIG)
    
    # Проверка подключения
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB")
    
    # Получение базы данных и коллекции
    db = client["weather_analytics_db"]
    collection = db["weather_data"]
    
    # Подсчет документов
    count = collection.count_documents({})
    print(f"\n📊 Total documents in collection: {count}")
    
    if count > 0:
        print("\n📝 Recent weather data (last 5 records):")
        print("=" * 80)
        
        results = collection.find(
            {},
            sort=[("created_at", -1)],
            limit=5
        )
        
        for i, doc in enumerate(results, 1):
            print(f"\n--- Record {i} ---")
            print(f"Latitude: {doc.get('latitude')}")
            print(f"Longitude: {doc.get('longitude')}")
            print(f"Status Code: {doc.get('status_code')}")
            print(f"Created At: {doc.get('created_at')}")
            
            if 'response' in doc and 'average_temperature_c' in doc['response']:
                print(f"Average Temperature: {doc['response']['average_temperature_c']}°C")
                print(f"Average Humidity: {doc['response'].get('average_humidity')}%")
                print(f"Samples: {len(doc['response'].get('samples', []))}")
            
            print("-" * 80)
    else:
        print("\n⚠️  No data found in collection")
    
    # Статистика по локациям
    print("\n📍 Statistics by location:")
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
        {"$limit": 10}
    ]
    
    location_stats = list(collection.aggregate(pipeline))
    
    if location_stats:
        for stat in location_stats:
            loc = stat['_id']
            print(f"  • Lat: {loc['lat']}, Lon: {loc['lon']} - {stat['count']} records")
    else:
        print("  No location statistics available")
    
    client.close()
    print("\n✅ MongoDB connection closed")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    exit(1)


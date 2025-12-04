"""
Тестовый скрипт для проверки ETL процесса MongoDB -> PostgreSQL
"""
import sys
sys.path.append('/home/weather-analytics/src/airflow/dags')

from connector__mongo_postgres_logic import move_data_to_postgres

if __name__ == "__main__":
    print("🚀 Starting ETL test...")
    try:
        result = move_data_to_postgres()
        print(f"\n✅ ETL completed successfully!")
        print(f"   Current weather records: {result['current_weather_count']}")
        print(f"   Forecast records: {result['forecast_count']}")
    except Exception as e:
        print(f"\n❌ ETL failed: {e}")
        import traceback
        traceback.print_exc()


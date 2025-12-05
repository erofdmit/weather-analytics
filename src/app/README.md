# Weather Analytics API

FastAPI сервис-агрегатор погоды из нескольких публичных API.

## Описание

Сервис собирает данные о погоде из различных провайдеров:
- OpenWeather
- WeatherAPI
- Weatherbit
- Weatherstack
- Open-Meteo

## Запуск

```bash
docker compose up -d --build
```

## API

После запуска API доступен по адресу: http://localhost:8000

Документация: http://localhost:8000/docs

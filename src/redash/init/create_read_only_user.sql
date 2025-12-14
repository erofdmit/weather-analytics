-- Создание read-only пользователя для Redash
-- Этот пользователь будет использоваться для безопасного подключения к витринам данных

-- Создать пользователя (если не существует)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'redash_viewer') THEN
        CREATE USER redash_viewer WITH PASSWORD 'redash_secure_2025';
    END IF;
END
$$;

-- Дать базовые права на подключение
GRANT CONNECT ON DATABASE weather_dwh TO redash_viewer;

-- Дать доступ к схеме dm (витрины данных)
GRANT USAGE ON SCHEMA dm TO redash_viewer;

-- Дать SELECT права на все существующие таблицы в схеме dm
GRANT SELECT ON ALL TABLES IN SCHEMA dm TO redash_viewer;

-- Автоматически давать SELECT на новые таблицы в схеме dm
ALTER DEFAULT PRIVILEGES IN SCHEMA dm
    GRANT SELECT ON TABLES TO redash_viewer;

-- Опционально: дать доступ к схеме ods (если нужен для аналитики)
-- GRANT USAGE ON SCHEMA ods TO redash_viewer;
-- GRANT SELECT ON ALL TABLES IN SCHEMA ods TO redash_viewer;

-- Проверка созданного пользователя
\echo 'Read-only user created successfully!'
\echo 'Username: redash_viewer'
\echo 'Access: SELECT on schema dm'

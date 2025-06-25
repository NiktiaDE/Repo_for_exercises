-- Создание таблиц
CREATE TABLE IF NOT EXISTS  users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

--Создание функции логирования по трем полям 
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN

--Поле name
	IF OLD.name IS DISTINCT FROM NEW.name THEN
	INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value) VALUES
	(OLD.id, CURRENT_USER, 'name', OLD.name, NEW.name);
	END IF;

--Поле email
	IF OLD.email IS DISTINCT FROM NEW.email THEN
	INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value) VALUES
	(OLD.id, CURRENT_USER, 'email', OLD.email, NEW.email);
	END IF;

--Поле role
	IF OLD.role IS DISTINCT FROM NEW.role THEN
	INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value) VALUES
	(OLD.id, CURRENT_USER, 'role', OLD.role, NEW.role);
	END IF;

--Обновление значения в таблице users на время когда было внесено изменение
	NEW.updated_at = CURRENT_TIMESTAMP;
	
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Создание триггера на таблицу users
CREATE TRIGGER user_changes_trigger
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();

--Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

--Создание функции для экспорта данных из аудита в csv формат
CREATE OR REPLACE FUNCTION export_daily_user_audit()
RETURNS void AS $$
DECLARE
	file_path TEXT;
	today_date TEXT;
BEGIN
	today_date := TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD');
	file_path := '/tmp/users_audit_export_' || today_date || '.csv';
	
	EXECUTE FORMAT ('COPY ( SELECT * FROM users_audit WHERE DATE(changed_at) = CURRENT_DATE) TO %L WITH CSV HEADER', file_path);

END;
$$ LANGUAGE plpgsql;


--Планировщик на три часа ночи каждый день
SELECT cron.schedule ('exp_daily_user_audit' , '0 3 * * *', 'export_daily_user_audit()');

--SELECT * FROM cron.job;


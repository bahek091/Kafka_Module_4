# Debezium PostgreSQL → Kafka CDC + Prometheus + Grafana

## Описание проекта

Проект реализует **Change Data Capture (CDC)** для PostgreSQL: изменения таблиц `users` и `orders` автоматически попадают в Apache Kafka через Debezium Connector. Дополнительно настроен мониторинг: Prometheus собирает метрики JMX с брокеров Kafka и Kafka Connect, Grafana визуализирует их.
Создание таблиц и подключение коннектора включены в скрипт.

### Компоненты

| Компонент | Порт | Назначение |
|-----------|------|-----------|
| **PostgreSQL** | 5432 | Источник данных, `wal_level=logical`, БД `customers` |
| **kafka-0/1/2** | 9094/9095/9096 | KRaft-кластер из 3 брокеров |
| **Schema Registry** | 8081 | Реестр схем (не используется активно, но присутствует) |
| **Kafka UI** | 8086 | Веб-интерфейс для просмотра топиков |
| **Kafka Connect** | 8083 | REST API + Debezium Connector |
| **JMX Exporter (Connect)** | 9876 | Метрики Debezium для Prometheus |
| **JMX Exporter (брокеры)** | 9404 | Метрики брокеров и JVM |
| **Prometheus** | 9090 | Сбор и хранение метрик |
| **Grafana** | 3000 | Визуализация (admin/admin) |

### Запуск инфраструктуры
```bash
docker compose up -d --build
```
Проверка, что БД поднялась
```bash
docker compose logs postgres | grep "database system is ready"
```

Проверка, что таблицы созданы
```bash
docker exec -it postgres psql -U postgres-user -d customers -c "\dt public.*"
```
```bash
public | orders | table | postgres-user
public | users  | table | postgres-user
```

Если таблиц нет, требуется создать вручную:
```bash
docker exec -it postgres psql -U postgres-user -d customers -c "
CREATE TABLE IF NOT EXISTS users (
id SERIAL PRIMARY KEY,
name VARCHAR(100),
email VARCHAR(100),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS orders (
id SERIAL PRIMARY KEY,
user_id INT REFERENCES users(id),
product_name VARCHAR(100),
quantity INT,
order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"
```

#### Регистрация Debezium Connector
Регистрация коннектора должна выполнится скриптом в контейнере connector-init. Проверить вывод скрипта:
``` bash
docker compose logs connector-init
```
В логах должно быть HTTP/1.1 201 Created или 200 OK. Если создание не выполнено, можно повторить операцию вручную:
``` bash
curl -i -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @register-postgres.json
```

Проверка статуса коннектора:
``` bash
curl -s http://localhost:8083/connectors/inventory-connector/status | jq
``` 
Ожидаемый вывод:
``` bash
{
"connector": { "state": "RUNNING" },
"tasks": [{ "state": "RUNNING" }]
}
```
#### Тест 1. Проверка снимка (snapshot) в Kafka
При первом запуске Debezium отправляет все существующие строки как события "op": "r". Проверьте:

``` bash
docker exec -it kafka-0 kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic inventory.public.users \
--from-beginning --max-messages 1 \
--property print.value=true
``` 

Ожидается JSON с "op": "r" и данными из users (John Doe и др.).

#### Тест 2. Проверка INSERT (событие "op": "c")
```  bash
docker exec -it postgres psql -U postgres-user -d customers -c \
"INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');"
``` 

Прочитайте топик:

``` bash
docker exec -it kafka-0 kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic inventory.public.users \
--max-messages 1 \
--property print.value=true
``` 

Ожидается "op": "c" с полями новой записи в after.

#### Тест 3. Проверка UPDATE (событие "op": "u")
```  bash
docker exec -it postgres psql -U postgres-user -d customers -c \
"UPDATE users SET name = 'Updated User' WHERE email = 'test@example.com';"
``` 
В топике появится событие с "op": "u", где before — старое значение, after — новое.

#### Тест 4. Проверка DELETE (событие "op": "d")
``` bash
docker exec -it postgres psql -U postgres-user -d customers -c \
"DELETE FROM users WHERE email = 'test@example.com';"
``` 
В топике появится событие с "op": "d", after = null. Debezium может также отправить tombstone (значение null).

#### Тест 5. Проверка Java Consumer
```  bash
cd java-consumer
mvn compile exec:java
``` 
Consumer подписывается на inventory.public.users и inventory.public.orders и выводит события в терминал. При вставке новой записи в PostgreSQL событие должно появиться в консоли в течение 1-2 секунд.


#!/bin/bash
# Ждём, пока Kafka Connect поднимется
echo "Ожидание Kafka Connect..."
while [ $(curl -s -o /dev/null -w %{http_code} http://kafka-connect:8083/connectors) -ne 200 ]; do
  echo "Kafka Connect ещё не готов..."
  sleep 5
done
echo "Kafka Connect готов. Регистрация коннектора..."


curl -i -X POST -H "Content-Type: application/json" \
  http://kafka-connect:8083/connectors \
  -d @/config/register-postgres.json

echo "Готово."
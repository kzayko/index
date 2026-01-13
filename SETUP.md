# Инструкция по настройке runtime конфигурации

## Быстрый старт

1. **Создайте файл с секретами:**
```bash
cp secrets.env.example secrets.env
```

2. **Отредактируйте `secrets.env` и установите безопасные пароли:**
```env
# Kafka SASL Configuration
KAFKA_ADMIN_USERNAME=kafka_admin
KAFKA_ADMIN_PASSWORD=your_secure_kafka_admin_password
KAFKA_USERNAME=indexer_user
KAFKA_PASSWORD=your_secure_kafka_password
PRODUCER_USERNAME=producer_user
PRODUCER_PASSWORD=your_secure_producer_password

# Elasticsearch Configuration
ELASTIC_PASSWORD=your_secure_elastic_password
ELASTICSEARCH_APP_USERNAME=app_user
ELASTICSEARCH_APP_PASSWORD=your_secure_app_password
```

3. **Создайте файл `.env` с основной конфигурацией:**
```env
KAFKA_TOPIC=messages
KAFKA_GROUP_ID=indexer-group
ELASTICSEARCH_INDEX=messages_index
API_HOST=0.0.0.0
API_PORT=8000
```

4. **Запустите все сервисы:**
```bash
docker-compose up -d
```

## Что происходит при запуске

1. **Генерация конфигурации Kafka:**
   - Сервис `generate-kafka-config` создает `kafka-jaas.conf` из переменных в `secrets.env`
   - Конфигурация сохраняется в Docker volume `kafka-config`

2. **Создание пользователей Elasticsearch:**
   - Сервис `setup-elasticsearch` автоматически создает пользователя `app_user`
   - Пользователь получает права `superuser` для работы с индексами

3. **Запуск сервисов:**
   - Kafka запускается с SASL/PLAIN аутентификацией
   - Elasticsearch запускается с включенной безопасностью
   - Indexer и API подключаются используя учетные данные из `secrets.env`

## Пользователи по умолчанию

Если не указаны в `secrets.env`, используются следующие значения:

### Kafka
- **kafka_admin**: `kafka_admin_password_123`
- **indexer_user**: `indexer_password_123`
- **producer_user**: `producer_password_123`

### Elasticsearch
- **elastic**: `elastic_password_123` (суперпользователь)
- **app_user**: `app_password_123` (пользователь приложения)

## Локальная разработка

Для локальной разработки без Docker:

1. **Сгенерируйте конфигурацию Kafka:**
```bash
python generate_kafka_jaas.py
# или
bash generate_kafka_jaas.sh
```

2. **Создайте пользователей Elasticsearch:**
```bash
# Убедитесь, что Elasticsearch запущен
python setup_elasticsearch_users.py
```

3. **Настройте `.env` для локального подключения:**
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=indexer_user
KAFKA_SASL_PASSWORD=indexer_password_123

ELASTICSEARCH_HOSTS=http://localhost:9200
ELASTICSEARCH_USERNAME=app_user
ELASTICSEARCH_PASSWORD=app_password_123
```

## Безопасность

⚠️ **ВАЖНО:**
- Никогда не коммитьте `secrets.env` в git
- Используйте сложные пароли в production
- Регулярно меняйте пароли
- Ограничьте доступ к файлу `secrets.env` (chmod 600)

## Проверка конфигурации

Проверить подключение к Kafka:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test \
  --from-beginning \
  --consumer.config /etc/kafka/consumer.properties
```

Проверить подключение к Elasticsearch:
```bash
curl -u app_user:app_password_123 http://localhost:9200/_cluster/health
```

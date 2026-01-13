# Message Indexer

Система для индексирования сообщений из Kafka в Elasticsearch с HTTP API для поиска.

## Структура проекта

```
.
├── api.py                 # HTTP API для поиска (POST /search)
├── bulk_load.py          # Скрипт массовой загрузки из файла
├── config.py             # Конфигурация из .env
├── elasticsearch_client.py  # Клиент Elasticsearch
├── indexer.py            # Основной сервис индексирования
├── init_index.py         # Скрипт инициализации индекса
├── kafka_consumer.py     # Kafka consumer
├── requirements.txt      # Python зависимости
├── Dockerfile           # Docker образ приложения
├── docker-compose.yml   # Docker Compose конфигурация
└── .env                 # Конфигурация (создать на основе .env.example)
```

## Установка и запуск

### Локальная установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` на основе `.env.example` и настройте параметры подключения.

3. Инициализируйте индекс Elasticsearch:
```bash
python init_index.py
```

4. Запустите индексатор:
```bash
python indexer.py
```

5. В другом терминале запустите API:
```bash
python api.py
```

### Запуск через Docker Compose

1. Создайте файл `secrets.env` на основе `secrets.env.example` с паролями:
```bash
cp secrets.env.example secrets.env
# Отредактируйте secrets.env и установите безопасные пароли
```

2. Создайте файл `.env` с необходимыми параметрами:
```env
KAFKA_TOPIC=messages
KAFKA_GROUP_ID=indexer-group
ELASTICSEARCH_INDEX=messages_index
```

3. Запустите все сервисы:
```bash
docker-compose up -d
```

4. Инициализируйте индекс:
```bash
docker-compose exec indexer python init_index.py
```

5. API будет доступно по адресу: http://localhost:8000

**Примечание:** Docker Compose автоматически:
- Создаст пользователей Kafka с SASL/PLAIN аутентификацией
- Создаст пользователей Elasticsearch
- Настроит все подключения с использованием учетных данных из `secrets.env`

## Использование

### Массовая загрузка из файла

Загрузите сообщения из текстового файла (один JSON на строку):
```bash
python bulk_load.py messages.txt
```

Или через Docker:
```bash
docker-compose exec indexer python bulk_load.py /path/to/messages.txt
```

### Поиск через API

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "телефон доверия"}'
```

Ответ:
```json
{
  "message_ids": ["79aac8d6-faf8-4068-a7e4-8c37fd2b678c", ...],
  "count": 1
}
```

### Health check

```bash
curl http://localhost:8000/health
```

## Формат данных

Сообщения из Kafka должны иметь следующий формат:
```json
{
  "user_id": "135",
  "event_properties": {
    "chat_id": "a87e85e2-443f-4dd2-b09c-9da7635c5e18",
    "message_id": "79aac8d6-faf8-4068-a7e4-8c37fd2b678c",
    "text": "Текст сообщения"
  }
}
```

Все поля (`user_id`, `chat_id`, `message_id`, `text`) обязательны для индексирования.

## Конфигурация

### Файлы конфигурации

1. **`.env`** - Основная конфигурация приложения:
   - `KAFKA_TOPIC` - Топик Kafka
   - `KAFKA_GROUP_ID` - Группа потребителей
   - `ELASTICSEARCH_INDEX` - Имя индекса
   - `API_HOST`, `API_PORT` - Настройки API

2. **`secrets.env`** - Секреты и пароли (НЕ коммитить в git!):
   - `KAFKA_ADMIN_USERNAME`, `KAFKA_ADMIN_PASSWORD` - Администратор Kafka
   - `KAFKA_USERNAME`, `KAFKA_PASSWORD` - Пользователь для индексатора
   - `ELASTIC_PASSWORD` - Пароль пользователя `elastic` (суперпользователь)
   - `ELASTICSEARCH_APP_USERNAME`, `ELASTICSEARCH_APP_PASSWORD` - Пользователь приложения

### Пользователи и пароли

**Kafka (SASL/PLAIN):**
- `kafka_admin` - Администратор (по умолчанию: `kafka_admin_password_123`)
- `indexer_user` - Пользователь для индексатора (по умолчанию: `indexer_password_123`)
- `producer_user` - Пользователь для продюсеров (по умолчанию: `producer_password_123`)

**Elasticsearch:**
- `elastic` - Суперпользователь (по умолчанию: `elastic_password_123`)
- `app_user` - Пользователь приложения (по умолчанию: `app_password_123`)

Все пароли настраиваются в файле `secrets.env`.

## Docker Compose сервисы

- **zookeeper**: Zookeeper для Kafka
- **generate-kafka-config**: Генерация конфигурации Kafka из secrets.env
- **kafka**: Kafka брокер с SASL/PLAIN аутентификацией
- **elasticsearch**: Elasticsearch с включенной безопасностью
- **setup-elasticsearch**: Автоматическое создание пользователей Elasticsearch
- **kibana**: Kibana для визуализации (опционально)
- **indexer**: Сервис индексирования из Kafka
- **api**: HTTP API для поиска

### Генерация конфигурации Kafka

Конфигурация `kafka-jaas.conf` автоматически генерируется из `secrets.env` при запуске Docker Compose. Для локальной разработки:

```bash
# Используя Python
python generate_kafka_jaas.py

# Или используя bash
bash generate_kafka_jaas.sh
```

## Разработка

Для разработки рекомендуется использовать виртуальное окружение:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

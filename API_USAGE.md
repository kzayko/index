# API Usage Guide

## Search Endpoint

### Basic Search

Поиск по тексту:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "лапки"}'
```

### Search with User ID Filter

Поиск с фильтром по user_id:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "лапки",
    "user_id": "02104aaf-8bca-47de-823f-9a1762b8fb5d"
  }'
```

### Search with Date Range

Поиск с фильтром по диапазону дат:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "лапки",
    "date_from": "2024-01-01",
    "date_to": "2024-12-31"
  }'
```

### Search with All Filters

Поиск с фильтрами по user_id и диапазону дат:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "лапки",
    "user_id": "02104aaf-8bca-47de-823f-9a1762b8fb5d",
    "date_from": "2024-01-01T00:00:00",
    "date_to": "2024-12-31T23:59:59"
  }'
```

## Request Format

```json
{
  "query": "search text",           // Required: text search query
  "user_id": "user123",             // Optional: filter by exact user_id
  "date_from": "2024-01-01",        // Optional: start date (ISO 8601 format)
  "date_to": "2024-12-31"           // Optional: end date (ISO 8601 format)
}
```

## Date Format

Поддерживаются следующие форматы дат (ISO 8601):

- `"2024-01-01"` - только дата
- `"2024-01-01T00:00:00"` - дата с временем
- `"2024-01-01T00:00:00Z"` - дата с временем и часовым поясом
- `"2024-01-01T00:00:00+03:00"` - дата с временем и смещением часового пояса

## Response Format

```json
{
  "results": [
    {
      "user_id": "02104aaf-8bca-47de-823f-9a1762b8fb5d",
      "chat_id": "a87e85e2-443f-4dd2-b09c-9da7635c5e18",
      "message_id": "79aac8d6-faf8-4068-a7e4-8c37fd2b678c",
      "text": "Текст сообщения",
      "timestamp": "2024-12-24T10:30:00"
    },
    {
      "user_id": "02104aaf-8bca-47de-823f-9a1762b8fb5d",
      "chat_id": "another-chat-id",
      "message_id": "another-message-id",
      "text": "Другое сообщение",
      "timestamp": "2024-12-24T11:00:00"
    }
  ],
  "count": 2
}
```

Каждый документ в `results` содержит все поля из базы данных:
- `user_id` - идентификатор пользователя
- `chat_id` - идентификатор чата
- `message_id` - идентификатор сообщения
- `text` - текст сообщения
- `timestamp` - временная метка (может отсутствовать, если не была указана при индексации)

## Examples

### PowerShell Example

```powershell
$body = @{
    query = "лапки"
    user_id = "02104aaf-8bca-47de-823f-9a1762b8fb5d"
    date_from = "2024-01-01"
    date_to = "2024-12-31"
} | ConvertTo-Json -Compress

$response = Invoke-RestMethod -Uri "http://localhost:8000/search" `
    -Method POST `
    -Body $body `
    -ContentType "application/json; charset=utf-8"

Write-Host "Found: $($response.count) messages"
$response.results | ForEach-Object {
    Write-Host "Message ID: $($_.message_id)"
    Write-Host "User ID: $($_.user_id)"
    Write-Host "Text: $($_.text)"
    Write-Host "Timestamp: $($_.timestamp)"
    Write-Host ""
}
```

### Python Example

```python
import requests

response = requests.post(
    "http://localhost:8000/search",
    json={
        "query": "лапки",
        "user_id": "02104aaf-8bca-47de-823f-9a1762b8fb5d",
        "date_from": "2024-01-01",
        "date_to": "2024-12-31"
    }
)

data = response.json()
print(f"Found: {data['count']} messages")
for doc in data['results']:
    print(f"Message ID: {doc['message_id']}")
    print(f"User ID: {doc['user_id']}")
    print(f"Chat ID: {doc['chat_id']}")
    print(f"Text: {doc['text']}")
    print(f"Timestamp: {doc.get('timestamp', 'N/A')}")
    print()
```

## Notes

- `query` - обязательный параметр, не может быть пустым
- `user_id` - точное совпадение (keyword field)
- `date_from` и `date_to` - можно указать один или оба параметра
- Если указан только `date_from`, будут найдены все документы от этой даты до текущего момента
- Если указан только `date_to`, будут найдены все документы до этой даты
- Все фильтры применяются одновременно (AND логика)

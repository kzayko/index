# Тесты для CSV Processor

## Описание

Набор unit-тестов для модуля `csv_processor.py`, который обрабатывает CSV файлы (включая LZ4 сжатые) и отправляет сообщения в Kafka.

## Запуск тестов

### Через unittest (стандартная библиотека Python):
```bash
python -m unittest test_csv_processor -v
```

### Через pytest (после установки):
```bash
pip install pytest pytest-cov
pytest test_csv_processor.py -v
pytest test_csv_processor.py --cov=csv_processor --cov-report=html
```

## Покрытие тестами

### StateManager (6 тестов)
- ✅ Инициализация состояния
- ✅ Загрузка существующего состояния
- ✅ Сохранение состояния
- ✅ Отметка файла как обработанного
- ✅ Проверка статуса обработки файла
- ✅ Получение позиции для возобновления

### CSVProcessor (13 тестов)
- ✅ Конвертация валидной CSV строки в сообщение Kafka
- ✅ Обработка строк с отсутствующими полями
- ✅ Генерация message_id при отсутствии
- ✅ Обработка CSV файла
- ✅ Пропуск уже обработанных файлов
- ✅ Возобновление обработки с сохраненной позиции
- ✅ Обработка файлов с невалидными строками
- ✅ Декомпрессия LZ4 файлов
- ✅ Обработка LZ4 файлов
- ✅ Обработка всех файлов в директории
- ✅ Обработка ошибок отправки в Kafka
- ✅ Сохранение состояния после каждого сообщения
- ✅ Обработка временных меток

## Особенности тестирования

1. **Изоляция от Kafka**: Все тесты используют моки для `KafkaMessageProducer`, поэтому не требуют реального подключения к Kafka.

2. **Временные файлы**: Тесты создают временные директории и файлы, которые автоматически очищаются после выполнения.

3. **Реальные данные**: Тесты используют реальные CSV и LZ4 файлы для проверки функциональности декомпрессии и парсинга.

4. **Покрытие edge cases**: Тесты проверяют различные сценарии:
   - Отсутствующие поля
   - Пустые значения
   - Генерация UUID
   - Обработка ошибок
   - Возобновление после сбоя

## Структура тестов

```
test_csv_processor.py
├── TestStateManager
│   ├── test_initial_state
│   ├── test_load_existing_state
│   ├── test_save_state
│   ├── test_mark_file_complete
│   ├── test_is_file_processed
│   └── test_get_resume_position
└── TestCSVProcessor
    ├── test_csv_row_to_message_valid
    ├── test_csv_row_to_message_missing_fields
    ├── test_csv_row_to_message_generate_message_id
    ├── test_process_file_success
    ├── test_process_file_skip_processed
    ├── test_process_file_resume_from_position
    ├── test_process_file_with_invalid_rows
    ├── test_decompress_lz4
    ├── test_process_lz4_file
    ├── test_process_all_files
    ├── test_send_message_failure
    ├── test_state_saved_after_each_message
    └── test_timestamp_handling
```

## Зависимости

Тесты используют только стандартную библиотеку Python (`unittest`, `unittest.mock`) и библиотеки, уже используемые в проекте:
- `pandas` - для работы с CSV
- `lz4` - для тестирования декомпрессии

Все зависимости Kafka мокируются, поэтому реальное подключение к Kafka не требуется.

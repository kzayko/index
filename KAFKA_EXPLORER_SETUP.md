# Настройка Kafka Explorer для подключения к Kafka

## ⚠️ ВАЖНО: Решение ошибки "Failed to create new KafkaAdminClient"

Если вы видите эту ошибку, **самая частая причина** - неправильно установлен **Security Protocol**.

**Обязательно установите:**
- **Security Protocol**: `SASL_PLAINTEXT` (НЕ `PLAINTEXT`!)
- **SASL Mechanism**: `PLAIN`

Ошибка "Unexpected Kafka request of type METADATA during SASL handshake" означает, что клиент пытается подключиться без SASL, хотя Kafka требует SASL аутентификацию.

## Параметры подключения

### Основные настройки:

- **Bootstrap Servers**: `localhost:9092`
- **Security Protocol**: `SASL_PLAINTEXT` ⚠️ **КРИТИЧЕСКИ ВАЖНО: именно SASL_PLAINTEXT!**
- **SASL Mechanism**: `PLAIN`
- **Topic**: `messages` (или любой другой топик)

### Учетные данные:

Доступны три пользователя (пароли находятся в файле `secrets.env`):

#### 1. Администратор (полный доступ):
- **Username**: `kafka_admin`
- **Password**: (см. `KAFKA_ADMIN_PASSWORD` в `secrets.env`)

#### 2. Indexer User (для чтения):
- **Username**: `indexer_user`
- **Password**: (см. `KAFKA_PASSWORD` в `secrets.env`)

#### 3. Producer User (для записи):
- **Username**: `producer_user`
- **Password**: (см. `PRODUCER_PASSWORD` в `secrets.env`)

## Пошаговая настройка в Kafka Explorer

### Шаг 1: Создание нового подключения

1. Откройте Kafka Explorer
2. Нажмите "Add Connection" или "New Connection"
3. Введите имя подключения (например: "Local Kafka")

### Шаг 2: Настройка Bootstrap Servers

В поле **Bootstrap Servers** введите:
```
localhost:9092
```

### Шаг 3: Настройка безопасности

**ВАЖНО**: Убедитесь, что все настройки безопасности указаны ДО попытки подключения.

1. В разделе **Broker Security** или **Security**:
   - **Type** или **Security Protocol**: выберите `SASL Plaintext` или `SASL_PLAINTEXT` (обязательно!)
   - **SASL Mechanism**: выберите `PLAIN` (если доступно)

2. **JAAS Configuration** (для Kafka Offset Explorer):
   
   ⚠️ **ВАЖНО**: В поле **JAAS Config** нужно вводить ТОЛЬКО JAAS конфигурацию БЕЗ префикса `sasl.jaas.config=`!
   
   В поле **JAAS Config** введите (БЕЗ `sasl.jaas.config=` в начале):
   ```
   org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<PASSWORD_FROM_SECRETS_ENV>";
   ```
   
   Где `<PASSWORD_FROM_SECRETS_ENV>` замените на значение `KAFKA_ADMIN_PASSWORD` из файла `secrets.env`.
   
   **Правильный пример** (замените `<PASSWORD>` на ваш пароль из secrets.env):
   ```
   org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<PASSWORD>";
   ```
   
   **НЕПРАВИЛЬНО** (не добавляйте префикс `sasl.jaas.config=`):
   ```
   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<PASSWORD>";
   ```

3. **Альтернативно** (если есть отдельные поля):
   - **SASL Username**: `kafka_admin` (или другой пользователь)
   - **SASL Password**: (указать пароль из `secrets.env` для выбранного пользователя)

4. **Дополнительные настройки SASL** (если доступны):
   - Убедитесь, что **SASL** включен/активирован
   - Некоторые клиенты требуют явного указания `sasl.enabled=true`
   - Проверьте, что нет конфликтующих настроек SSL/TLS

### Шаг 4: Дополнительные настройки (опционально)

- **Client ID**: можно оставить по умолчанию или указать `kafka-explorer`
- **Request Timeout**: `30000` (30 секунд)
- **Session Timeout**: `10000` (10 секунд)

### Шаг 5: Сохранение и подключение

1. Нажмите "Save" или "Test Connection" для проверки
2. Если подключение успешно, нажмите "Connect"

## Настройка для Kafka Offset Explorer (детальная инструкция)

Если вы используете **Kafka Offset Explorer** (Kafka Tool), следуйте этим шагам:

### Шаг 1: Broker Security

1. В разделе **Broker Security**:
   - **Type**: выберите `SASL Plaintext` из выпадающего списка

### Шаг 2: JAAS Config

2. В поле **JAAS Config** (которое появится после выбора SASL Plaintext) введите:

⚠️ **КРИТИЧЕСКИ ВАЖНО**: В поле **JAAS Config** вводите ТОЛЬКО JAAS конфигурацию, **БЕЗ префикса `sasl.jaas.config=`**!

**ПРАВИЛЬНО** (введите именно это):
```
org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<ВАШ_ПАРОЛЬ>";
```

**НЕПРАВИЛЬНО** (не добавляйте `sasl.jaas.config=` в начале):
```
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<ВАШ_ПАРОЛЬ>";
```

**Как получить пароль:**
- Откройте файл `secrets.env` в корне проекта
- Найдите строку `KAFKA_ADMIN_PASSWORD=...`
- Скопируйте значение после знака `=` (без пробелов, полностью!)
- Вставьте его вместо `<ВАШ_ПАРОЛЬ>` в JAAS config

**Пример правильного ввода:**
Если в `secrets.env` указано:
```
KAFKA_ADMIN_PASSWORD=ваш_пароль_здесь
```

То в поле JAAS Config введите (замените `ваш_пароль_здесь` на реальный пароль):
```
org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="ваш_пароль_здесь";
```

**Важно:**
- Убедитесь, что пароль скопирован полностью (не обрезан)
- Пароль должен быть в кавычках
- После точки с запятой должен быть пробел перед закрывающей кавычкой

### Шаг 3: Альтернативный способ (через файл)

Если Offset Explorer требует файл конфигурации, создайте файл `kafka-client-jaas.conf`:

```
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="kafka_admin"
    password="<PASSWORD_FROM_SECRETS_ENV>";
};
```

И запустите Offset Explorer с параметром:
```bash
offsetexplorer.exe -J-Djava.security.auth.login.config=path/to/kafka-client-jaas.conf
```

## Альтернативная конфигурация (через файл конфигурации)

Если Kafka Explorer поддерживает файлы конфигурации, используйте следующий формат:

```properties
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<PASSWORD_FROM_SECRETS_ENV>";
```

**КРИТИЧЕСКИ ВАЖНО**: 
- `security.protocol` должен быть именно `SASL_PLAINTEXT` (не `PLAINTEXT`!)
- Пароль должен быть в кавычках
- После точки с запятой в `sasl.jaas.config` должен быть пробел перед закрывающей кавычкой

Где `<PASSWORD_FROM_SECRETS_ENV>` нужно заменить на значение `KAFKA_ADMIN_PASSWORD` из файла `secrets.env`.

## Проверка подключения

После подключения вы должны увидеть:
- Список топиков (включая `messages`)
- Метаданные брокера
- Возможность просмотра сообщений

## Устранение проблем

### Проблема: "Connection refused"
- Убедитесь, что Kafka запущен: `docker-compose ps kafka`
- Проверьте, что порт 9092 доступен

### Проблема: "Authentication failed" или "Failed to create new KafkaAdminClient"
- ⚠️ **Проверьте формат JAAS Config**: НЕ добавляйте префикс `sasl.jaas.config=` в поле JAAS Config!
- В поле JAAS Config должно быть ТОЛЬКО: `org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_admin" password="<PASSWORD>";`
- Проверьте правильность username и password из `secrets.env`
- Убедитесь, что пароль скопирован полностью (не обрезан)
- Убедитесь, что используется `SASL_PLAINTEXT` (не `PLAINTEXT`!)
- Убедитесь, что `SASL Mechanism` установлен в `PLAIN`
- Проверьте, что Security Protocol установлен ДО попытки подключения
- Убедитесь, что нет активных настроек SSL/TLS, которые могут конфликтовать

### Проблема: "Unexpected Kafka request of type METADATA during SASL handshake"
Эта ошибка означает, что клиент пытается отправить запросы до завершения SASL аутентификации:
- Убедитесь, что **Security Protocol** установлен в `SASL_PLAINTEXT` (не `PLAINTEXT`)
- Убедитесь, что **SASL Mechanism** установлен в `PLAIN`
- Проверьте, что все настройки безопасности применены ДО подключения
- Попробуйте перезапустить Kafka Explorer после изменения настроек
- Убедитесь, что пароль скопирован правильно (без лишних пробелов)

### Проблема: "No topics found"
- Это нормально, если топики еще не созданы
- Kafka автоматически создаст топик при первой записи

## Полезные команды для проверки

```bash
# Проверить статус Kafka
docker-compose ps kafka

# Просмотреть логи Kafka
docker-compose logs kafka

# Проверить список топиков через контейнер
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list --command-config /etc/kafka/client.properties
```

## Примечания

- Для чтения сообщений рекомендуется использовать `indexer_user`
- Для записи сообщений рекомендуется использовать `producer_user`
- Для административных операций используйте `kafka_admin`
- **Все пароли хранятся в файле `secrets.env`** (не коммитится в git)
- **ВАЖНО**: Не коммитьте файл `secrets.env` в git! Используйте `secrets.env.example` как шаблон

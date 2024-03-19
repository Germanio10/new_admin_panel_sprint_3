# Проектное задание третьего спринта первого модуля

## .env файл
Для настройки переменных среды нужно скопировать пример конфигурации [.env.example](.env.example)
```sh
cp .env.example .env
```

## Запуск проекта
Для запуска проекта необходимо ввести команду
```shell
docker compose up
```

# Переменные окружения, используемые в проекте

- [DB_NAME](#DB_NAME)
- [DB_USER](#DB_USER)
- [DB_PASSWORD](#DB_PASSWORD)
- [DB_HOST](#DB_HOST)
- [DB_PORT](#DB_PORT)
- [PARSE_SIZE](#PARSE_SIZE)
- [UPDATED_DAYS_LIMIT](#UPDATED_DAYS_LIMIT)
- [RERUN](#RERUN)
- [ELASTICSEARCH_URL](#ELASTICSEARCH_URL)
- [ELASTICSEARCH_LOAD_SIZE](#ELASTICSEARCH_LOAD_SIZE)

## <p id="DB_NAME">DB_NAME</p>
Имя БД, используемой в docker контейнере.

## <p id="DB_USER">DB_USER</p>
Пользователь БД, используемой в docker контейнере.

## <p id="DB_PASSWORD">DB_PASSWORD</p>
Пароль к БД, используемой в docker контейнере.

## <p id="DB_HOST">DB_HOST</p>
Адрес хоста БД, используемой в docker контейнере.

## <p id="DB_PORT">DB_PORT</p>
Порт хоста БД, используемой в docker контейнере.

## <p id="PARSE_SIZE">PARSE_SIZE</p>
Количество записей получаемых при запросе к БД.

## <p id="UPDATED_DAYS_LIMIT">UPDATED_DAYS_LIMIT</p>
Количество дней за которое сверяются новые данные.

## <p id="RERUN">RERUN</p>
Время между запусками скрипта по сверке данных (в секундах).

## <p id="ELASTICSEARCH_URL">ELASTICSEARCH_URL</p>
Адрес для подключения к Elasticsearch.

## <p id="ELASTICSEARCH_LOAD_SIZE">ELASTICSEARCH_LOAD_SIZE</p>
Размер пачки с данными, отправляемый в Elasticsearch при обновлении данных.


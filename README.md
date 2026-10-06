# Скоринг фродовых транзакций

Домашнее задание по потоковому inference. Сервис получает транзакции из Kafka,
применяет CatBoost на CPU и отправляет результат в топик `scores`. Отдельный
consumer сохраняет результаты в PostgreSQL. В Streamlit можно отправить CSV
и посмотреть результаты скоринга.

## Запуск

Нужны Docker с запущенным daemon и Docker Compose v2. На macOS подойдут
Docker Desktop или Colima. Для стенда желательно выделить 4 ГБ памяти.
Команды выполняются из корня репозитория:

```bash
cp .env.example .env
docker compose config -q
docker compose up -d --build
docker compose ps -a
```

Открыть http://localhost:8501. Контейнер `kafka-init` должен завершиться с кодом 0,
остальные сервисы — работать. Первый запуск занимает несколько минут.

Во вкладке «Отправить транзакции» загрузить `test.csv` или выбрать встроенный
пример и нажать «Отправить в Kafka». Затем во вкладке «Результаты» нажать
«Посмотреть результаты». Там выводятся:

- 10 последних записей из PostgreSQL с `fraud_flag = 1`;
- гистограмма скоров последних 100 записей. Если записей меньше, используются все.

Примеры в `examples/`:

- `test_sample.csv` — первые 100 строк реального test.csv;
- `test_sample_scores.csv` — их результаты офлайн-скоринга;
- `results_demo.csv` — 20 строк test.csv с высоким скором и 80 с низким,
  чтобы проверить оба раздела интерфейса. Это не случайная выборка.

Полные train.csv и test.csv для запуска контейнеров не нужны и не включены в git.
UI принимает CSV до 50 МБ и читает первые 10 000 строк.

## Устройство проекта

```text
Streamlit → Kafka: transactions → fraud_detector → Kafka: scores
                                                   ↓
                                             result_writer
                                                   ↓
                                               PostgreSQL
                                                   ↓
                                          Streamlit: Результаты
```

| Файл | Назначение |
|---|---|
| `fraud_detector/app/app.py` | Чтение и отправка сообщений Kafka |
| `fraud_detector/src/preprocessing.py` | Подготовка признаков |
| `fraud_detector/src/scorer.py` | Загрузка модели и CPU inference |
| `fraud_detector/models/` | Модель CatBoost и метаданные |
| `result_writer/app.py` | Запись результатов в PostgreSQL |
| `sql/init.sql` | Таблица `fraud_scores` и индексы |
| `interface/app.py` | Интерфейс Streamlit |
| `common/` | Общие функции Kafka, CSV и PostgreSQL |
| `scripts/` | Офлайн-обучение, отправка CSV и проверка стенда |

Все контейнеры находятся в одной сети. Kafka работает в режиме KRaft,
без ZooKeeper. Топики создаются до запуска приложений. Kafka и PostgreSQL
хранят данные в именованных томах.

## Данные и модель

Использован реальный train.csv соревнования
[МТС ШАД](https://www.kaggle.com/competitions/teta-ml-1-2025): 786 431 строка.
Обучение проводится отдельно. При сборке и запуске контейнеров выполняется
только inference сохранённой модели.

Из семинарского решения оставлены признаки времени и логарифмы суммы,
населения и расстояния между клиентом и продавцом. Расстояние считается по
формуле гаверсинуса. Категории `gender`, `merch`, `cat_id`, `one_city`,
`us_state`, `jobs` обрабатывает CatBoost. Target-encoding из семинара заменён
встроенной обработкой категорий, поэтому train.csv при старте не читается.

Для обучения и inference используется одна функция `prepare_features()`.
Имена и адреса не входят в признаки. Время без часового пояса считается UTC.
Категориальные пропуски заменяются на `__missing__`, числовые остаются NaN.
Сумма обязательна; отрицательные суммы, некорректные даты и координаты отклоняются.

Модель обучена на CPU: максимум 250 деревьев, глубина 6, learning rate 0.08,
seed 42, балансировка классов. Сохранено 217 деревьев. Разбиение по времени:
629 143 строки для обучения и 157 288 для валидации, граница —
13 декабря 2019 года, 07:25 UTC.

| Метрика на валидации | Значение |
|---|---:|
| ROC AUC | 0.997466 |
| Average Precision | 0.828177 |
| Precision | 0.796717 |
| Recall | 0.752086 |
| F1 | 0.773758 |

Порог **0.9326304997285554** выбран по F1 на той же валидации,
что использовалась для выбора лучшей итерации. Это не оценка на независимом
тесте. В test.csv нет target: проверены формат и применение модели ко всем
262 144 строкам. Отчёт: `reports/test_csv_check.json`.

Модель загружается один раз. Проверяются её хеш и порядок признаков.
Параметры, версии обучения и хеш train.csv записаны в `fraud_detector/models/metadata.json`.

## Сообщения Kafka

Входной топик — `transactions`. Каждое сообщение содержит ID и одну строку CSV:

```json
{"transaction_id":"txn-001","data":{"transaction_time":"2019-09-14 02:46","merch":"fraud_example","cat_id":"grocery_net","amount":25.79,"gender":"M","one_city":"Cross Plains","us_state":"TX","lat":32.1482,"lon":-99.1872,"population_city":1897,"jobs":"Chief Operating Officer","merchant_lat":31.772057,"merchant_lon":-99.103183}}
```

Названия полей соответствуют test.csv. Также принимается плоский JSON-объект.
Колонки из примера обязательны, дополнительные поля допустимы.

В `scores` отправляется объект с тремя полями:

```json
{"transaction_id":"txn-001","score":0.015,"fraud_flag":0}
```

`score` берётся из `predict_proba`, флаг определяется как `score >= threshold`.
Порог можно изменить через `FRAUD_THRESHOLD` в `.env`.

Если в CSV есть `transaction_id`, он сохраняется. Если нет, ID создаётся из
хеша файла и номера строки. Повторная отправка файла создаёт те же ID.
В PostgreSQL сохраняется первый результат для каждого ID; повторная доставка
не меняет его скор и время записи. Последними считаются записи по времени
сохранения в БД. Перескоринг с тем же ID не обновляет результат.

Kafka offset подтверждается после успешной доставки скора, а у writer — после
SQL commit. При сбое возможна повторная обработка. Некорректные сообщения
попадают в `transactions_dlq` или `scores_dlq` с исходными topic, partition,
offset и причиной ошибки.

## Проверка

Прошли 16 локальных тестов и сквозной тест с Kafka/PostgreSQL в Docker.
Кнопка результатов проверена внутри контейнера с настоящей базой: 10 фродовых
записей и гистограмма по 100 последним скорам. Результаты проверки сохранены
в `reports/verification.json`.

Отправить пример и проверить весь поток:

```bash
docker compose exec -T interface python -m scripts.send_csv examples/results_demo.csv --limit 100
docker compose exec -T interface python -m scripts.smoke_test
```

Сквозной тест сверяет Kafka-скоры с прямым вызовом модели и PostgreSQL,
проверяет дубли, запросы интерфейса и обработку ошибочного сообщения.
Он оставляет записи с префиксом `smoke-`. Служебная запись
`smoke-…-writer-marker` со скором 1 нужна для проверки writer и не является
предсказанием модели.

Локальные тесты и офлайн-проверка CSV:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m scripts.check_csv --test ../test.csv
```

Для повторного обучения:

```bash
pip install -r requirements-train.txt
python -m scripts.train_model --train ../train.csv
docker compose up -d --build fraud_detector interface
```

GitHub Actions запускает тесты, сборку и сквозную проверку после push.

Если сеть использует корпоративный HTTPS-прокси, при сборке нужен его CA:

```bash
BUILD_CA_FILE=/path/to/ca.pem docker compose -f docker-compose.yml -f docker-compose.proxy.yml up -d --build
```

Сертификат передаётся как build secret и не сохраняется в образе.
В обычной сети дополнительный compose-файл не нужен.

## Логи и остановка

```bash
docker compose logs --tail=100 fraud_detector result_writer
docker compose down
```

Данные сохраняются. `docker compose down -v` удаляет сообщения Kafka и данные
PostgreSQL. SQL-инициализация выполняется при первом запуске на пустом томе.

Если Docker не отвечает, проверить `docker info` и запустить Docker Desktop
или `colima start`. При ошибках старта смотреть `docker compose ps -a` и логи
соответствующего сервиса. Стенд рассчитан на локальную работу: один Kafka broker,
учётные данные в `.env.example`, без авторизации интерфейса.

Использованные справки: [Confluent Docker](https://docs.confluent.io/platform/current/installation/docker/config-reference.html),
[confluent-kafka API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
[PostgreSQL INSERT](https://www.postgresql.org/docs/16/sql-insert.html).

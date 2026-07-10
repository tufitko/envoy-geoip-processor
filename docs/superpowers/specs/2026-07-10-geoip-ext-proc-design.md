# envoy-geoip-processor — дизайн

Дата: 2026-07-10
Статус: утверждён

## Цель

Собственный внешний процессор (ext_proc) для Envoy Gateway, который определяет
геоданные клиента по MaxMind-базам и добавляет их в хедеры запроса перед
проксированием на upstream. Аналог ngx_http_geoip2_module, но как отдельный
gRPC-сервис, подключаемый к envoy-gateway через `EnvoyExtensionPolicy`.

Референс идеи: https://github.com/mburtless/geoip-processor (минимальный
пример: один хедер, база из файла, без обновлений).

## Требования

1. **Автоскачивание баз** — по HTTPS-ссылке (включая MaxMind download API с
   basic auth) и из S3 (стандартная AWS-цепочка кредов: env / IRSA / профиль).
2. **Периодическая проверка и обновление** — conditional-запросы
   (ETag / Last-Modified для HTTP, HeadObject+ETag для S3), скачивание только
   при изменении, атомарная подмена reader'а без рестарта и без блокировки
   трафика.
3. **Полный набор хедеров уровня ngx_http_geoip2_module** — произвольное число
   баз любых типов (City, Country, ASN, ISP, Anonymous-IP, ...), значение
   хедера задаётся lookup-путём в базе (`country.iso_code`,
   `subdivisions.0.iso_code`, `autonomous_system_number`, ...). Имя каждого
   хедера и сам набор конфигурируются.
4. **Выбор IP клиента** — упорядоченная цепочка источников: кастомные хедеры
   (x-real-ip, x-forwarded-for и т.д.) и атрибуты Envoy (source address).
   Побеждает первый валидный IP.
5. **Fail-режим — гибрид**: под не ready, пока обязательные базы не загружены;
   в рантайме fail-open — ошибка lookup'а не блокирует запрос, хедеры просто
   не ставятся. В `EnvoyExtensionPolicy` дополнительно `failOpen: true`.

## Отвергнутые альтернативы

- **Нативный `envoy.filters.http.geoip`** — нет автоскачивания баз, в
  envoy-gateway подключается только через хрупкий EnvoyPatchPolicy,
  фиксированный набор хедеров.
- **WASM-плагин** — фоновое скачивание и S3-подпись изнутри wasm-рантайма
  неоправданно сложны.

## Архитектура

```
client → Envoy Gateway ──ext_proc gRPC──> geoip-processor
                                             ├─ gRPC server (ExternalProcessor, только REQUEST_HEADERS)
                                             ├─ Resolver: IP из цепочки → lookup по базам → HeaderMutation
                                             ├─ DB Manager: N баз; скачивание (HTTP/S3), периодический
                                             │   conditional-check, атомарный swap reader'а, дисковый кэш
                                             └─ HTTP admin: /healthz, /readyz, /metrics (Prometheus)
```

Один Go-бинарь. Слои:

- `internal/config` — загрузка и валидация YAML-конфига.
- `internal/geodb` — менеджер баз: источники (http/s3), цикл обновления,
  `atomic.Pointer[maxminddb.Reader]`, lookup по пути.
- `internal/server` — реализация ext_proc gRPC + admin HTTP.
- `cmd/geoip-processor` — main: собрать всё, graceful shutdown.

Ключевая библиотека: `oschwald/maxminddb-golang/v2` — decode произвольного
пути из любой mmdb-базы без типизированных структур.

## Конфиг

```yaml
listen:
  grpc: :9000
  admin: :8080

cache_dir: /var/cache/geoip     # дисковый кэш баз (emptyDir/PVC)

ip_sources:                     # первый валидный IP побеждает
  - header: x-real-ip
  - header: x-forwarded-for     # берётся первый (левый) адрес из списка
  - envoy: source_address       # source address из атрибутов ext_proc

overwrite: true                 # затирать одноимённые клиентские хедеры (антиспуфинг)

databases:
  city:
    source: https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz
    auth:
      basic_env: MAXMIND_LICENSE   # "account_id:license_key" из env
    check_interval: 6h
    required: true                 # влияет на /readyz
  asn:
    source: s3://my-bucket/GeoLite2-ASN.mmdb
    check_interval: 6h

headers:
  x-geoip-country-code: {db: city, path: country.iso_code}
  x-geoip-country-name: {db: city, path: country.names.en}
  x-geoip-region:       {db: city, path: subdivisions.0.iso_code}
  x-geoip-region-name:  {db: city, path: subdivisions.0.names.en}
  x-geoip-city:         {db: city, path: city.names.en}
  x-geoip-latitude:     {db: city, path: location.latitude}
  x-geoip-longitude:    {db: city, path: location.longitude}
  x-geoip-postal-code:  {db: city, path: postal.code}
  x-geoip-timezone:     {db: city, path: location.time_zone}
  x-geoip-asn:          {db: asn, path: autonomous_system_number}
  x-geoip-org:          {db: asn, path: autonomous_system_organization}
```

Опции хедера: `db`, `path`, опционально `default` (значение, если lookup
ничего не вернул; без `default` хедер не ставится).

Правила:

- `source` — `https://...` или `s3://bucket/key`.
- Поддерживаемые форматы: голый `.mmdb` и `.tar.gz` (внутри ищется `*.mmdb`).
- `auth.basic_env` — имя env-переменной с `user:password` для HTTP basic auth.
  Для S3 — стандартная AWS-цепочка (env, IRSA, shared config).
- Валидация на старте: каждый `headers[].db` ссылается на существующую базу,
  пути непустые, интервалы > 0.

## Поток запроса

1. Envoy шлёт `ProcessingRequest{request_headers}` (остальные стадии — SKIP
   через `processing_mode`, body не трогаем).
2. Resolver идёт по `ip_sources`: для `header` — берёт значение хедера (для
   x-forwarded-for — первый адрес слева), парсит как IP; для
   `envoy: source_address` — атрибут запроса. Первый валидный — клиентский IP.
3. Для каждого сконфигурированного хедера: lookup IP в соответствующей базе,
   decode по `path`, приведение к строке (float — без экспоненты, uint — как
   есть).
4. Ответ — `HeadersResponse` с `HeaderMutation`: set (перезапись) всех
   получившихся хедеров.
5. Любая ошибка на шагах 2–3 → пустая мутация, запрос проходит (fail-open);
   ошибка логируется и инкрементит метрику.

## Обновление баз

- На старте: если в `cache_dir` есть валидная копия — загружаем её сразу
  (ready не ждёт сети), затем в фоне проверяем обновление.
- Цикл на базу: раз в `check_interval` — conditional-запрос
  (`If-None-Match`/`If-Modified-Since` или `HeadObject`); 304/совпавший ETag →
  ничего не делаем.
- Новая версия: скачиваем во временный файл в `cache_dir`, распаковываем при
  необходимости, валидируем открытием `maxminddb.Open`, `rename` в постоянное
  имя, атомарно подменяем reader, старый закрываем после дренажа.
- Метаданные (ETag/Last-Modified) храним рядом в `<db>.meta.json`.
- Ошибка проверки/скачивания: логируем, метрика, продолжаем работать на
  старой копии, повторная попытка через `check_interval` (с джиттером).

## Наблюдаемость

- `/healthz` — процесс жив. `/readyz` — все `required` базы загружены.
- Prometheus-метрики: `geoip_lookups_total{db,result}`,
  `geoip_db_update_total{db,result}`, `geoip_db_age_seconds{db}`,
  `geoip_db_last_check_timestamp{db}`, стандартные gRPC-метрики.
- Логи — slog, JSON.

## Тестирование

- **Юнит**: конфиг (валидные/невалидные случаи); выбор IP (x-real-ip,
  XFF-список, мусор в хедере, fallback на envoy-атрибут); lookup по path на
  официальных тестовых mmdb MaxMind (включая default и отсутствующий путь);
  conditional download против httptest-сервера (ETag/304/новая версия/битый
  файл); atomic swap под конкурентными lookup'ами (`-race`).
- **Интеграция**: docker-compose (Envoy + processor + эхо-бэкенд), curl с
  `x-real-ip`, проверка проставленных хедеров.

## Поставка

- Dockerfile: multi-stage, distroless, non-root.
- docker-compose для локальной проверки.
- Helm-чарт: Deployment (+emptyDir/PVC под кэш), Service, ConfigMap с
  конфигом, Secret с кредами, пример `EnvoyExtensionPolicy` (+`ReferenceGrant`
  при необходимости) для envoy-gateway.

## Вне скоупа

- Response-хедеры, обработка body.
- Собственный DNS/anycast-геолокатор, не-MaxMind форматы баз.
- Rate limiting / блокировки по гео (это делает сам gateway на основе хедеров).

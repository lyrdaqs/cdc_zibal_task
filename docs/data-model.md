# لایه‌ها، مدل و منطق incremental

همهٔ تعریف‌ها در [clickhouse/sql](../clickhouse/sql) هستند. فایل `02_stg.sql` لایهٔ stg را می‌سازد، `03_mart.sql` لایهٔ mart را و `04_quality.sql` تست‌ها را. اسکریپت [02_apply_sql.sh](../clickhouse/initdb/02_apply_sql.sh) این فایل‌ها را بار اول بالا آمدن ClickHouse اجرا می‌کند.

## اشیای هر لایه

| دیتابیس | شیء | موتور | چه کسی می‌نویسد | چه کسی می‌خواند |
|---|---|---|---|---|
| `raw` | `cdc_transactions`، `cdc_merchants`، `cdc_dead_letter` | MergeTree (فقط append) | MV روی Kafka engine | etl |
| `stg` | `transactions`، `merchants` | ReplacingMergeTree(_lsn, _is_deleted) | MV روی raw (لحظه‌ای) | etl |
| `stg` | `merchant_versions` | ReplacingMergeTree، کلید (merchant_id, _lsn) | MV روی raw (لحظه‌ای) | etl |
| `mart` | `dim_merchant` | ReplacingMergeTree(_lsn, _is_deleted) | MV روی stg (لحظه‌ای) | etl، metabase |
| `mart` | `dim_merchant_history`، به‌علاوهٔ نمای `dim_merchant_scd2` | ReplacingMergeTree، کلید (merchant_id, _lsn) | MV روی stg (لحظه‌ای) | etl، metabase |
| `mart` | `fct_transactions` | ReplacingMergeTree(_loaded_at, _is_deleted)، پارتیشن ماهانه | `pipeline.rmv_fct_transactions` (هر ۱ دقیقه) | etl، metabase |
| `mart` | `agg_txn_daily_channel`، `agg_merchant_daily_success` | ReplacingMergeTree(_version, _is_deleted) | `pipeline.rmv_agg_*` (`DEPENDS ON` fact) | etl، metabase |
| `meta` | `v_data_quality` (نما)، `dq_results` | View و MergeTree با TTL سی‌روزه | `pipeline.rmv_data_quality` (هر ۱۵ دقیقه) | etl، metabase |
| `pipeline` | Refreshable MVها | — | — | admin |
| `secure` | `pii_key` | TinyLog | init | فقط admin و MV |

**قاعدهٔ کلی**: MV معمولی فقط برای تبدیل ساده و بدون join استفاده می‌شود، چون با هر insert اجرا می‌شود و خطای آن مصرف از Kafka را متوقف می‌کند. هر چیزی که join، تجمیع یا محاسبهٔ دوباره دارد در Refreshable MV است که از ورود داده جداست.

ستون‌های فنی:

- `_lsn`: ترتیب واقعی تغییر در Postgres
- `_source_ts_ms`: زمان commit
- `_ingested_at`: زمان رسیدن به ClickHouse
- `_loaded_at` و `_version`: زمان ساخته شدن ردیف توسط Refreshable MV

## منطق incremental

1. **stg و dimها**: MV فقط blockهای تازهٔ raw را پردازش می‌کند. `ReplacingMergeTree` نسخه با LSN بالاتر را نگه می‌دارد و delete را با `FINAL` پنهان می‌کند. پیام تکراری Kafka (at-least-once) هم بی‌اثر است.
2. **fact** (`rmv_fct_transactions`، `REFRESH EVERY 1 MINUTE APPEND`):
   - watermark برابر `max(_loaded_at)` جدول مقصد منهای ۵ دقیقه است.
   - تراکنش‌هایی دوباره ساخته می‌شوند که یا رویداد تازه دارند، یا پذیرنده‌شان نسخه‌ای گرفته که `valid_from` آن قبل از `created_at` است (dimension دیررس).
   - آخرین نسخه با `argMax(..., _lsn)` انتخاب می‌شود، از جمله نشانگر delete.
   - ویژگی‌های پذیرنده با `ASOF LEFT JOIN` روی `created_at >= valid_from` گرفته می‌شوند.
   - بار اول جدول خالی است، پس کل ۵ میلیون تراکنش ساخته می‌شود و از آن به بعد فقط تغییرات.
3. **aggregateها** (`DEPENDS ON rmv_fct_transactions`):
   - روزهایی که ردیف fact تازه دارند پیدا می‌شوند و فقط همان روزها از `fct FINAL` دوباره حساب می‌شوند.
   - نتیجه با `_version` جدیدتر append می‌شود.
   - کلیدی که قبلاً برای آن روز بوده و حالا نیست (مثلاً پذیرنده‌ای که استانش با تغییر دیررس عوض شده) یک ردیف `_is_deleted = 1` می‌گیرد.
   - نتیجه: `FINAL` روی aggregate همیشه برابر بازمحاسبهٔ کامل است، و تست «سازگاری» همین را بررسی می‌کند.

## چرا این Grainها

- **fact در سطح تراکنش**: کوچک‌ترین واحد معنادار است. هر سؤال دیگری، مثل تفکیک BIN یا اقساط، بدون برگشتن به core جواب داده می‌شود. ویژگی‌های پذیرنده هم یک بار point-in-time تثبیت می‌شوند.
- **aggregate روزانه × کانال × وضعیت**: کارت‌های ۱ و ۲ فقط به همین ابعاد نیاز دارند. وضعیت به‌عنوان بُعد نگه داشته شده، نه فیلتر، تا نرخ موفقیت از همین جدول درآید.
- **aggregate روزانه × پذیرنده × ویژگی‌های زمان تراکنش، فقط موفق‌ها**: کارت‌های ۳ تا ۵ روی تراکنش‌های موفق‌اند. این grain حجم را حدود ۱۵ برابر کم می‌کند، ولی هنوز برای هر بازهٔ دلخواه، مثل ماه جاری یا ۹۰ روز، قابل جمع زدن است.
- **SCD2 به‌جای snapshot روزانه**: دقت تا لحظهٔ تغییر دارد، حجمش کم است و ASOF JOIN در ClickHouse ارزان است.

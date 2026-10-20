# معماری و تصمیم‌ها

## جریان داده، قدم‌به‌قدم

1. **PostgreSQL 16 (core)**:
   - با `wal_level=logical` اجرا می‌شود.
   - publication به نام `dbz_publication` را مالک جدول‌ها می‌سازد، پس Debezium فقط به `REPLICATION` و `SELECT` نیاز دارد (کمترین دسترسی).
   - `REPLICA IDENTITY FULL` روی هر دو جدول فعال است تا رویداد Update و Delete تصویر کامل «قبل» را داشته باشد. هزینه‌اش WAL کمی بزرگ‌تر است، که برای ردیف‌های حدود ۱۰۰ بایتی ناچیز است.
   - `max_slot_wal_keep_size=4GB` جلوی پر شدن دیسک را می‌گیرد اگر CDC مدتی متوقف بماند.
2. **Debezium 3.2 (Kafka Connect)**:
   - از plugin داخلی `pgoutput` استفاده می‌کند و افزونهٔ اضافه‌ای لازم نیست.
   - `snapshot.mode=initial`: اول snapshot کامل ۵ میلیون ردیف (`op=r`)، بعد stream از همان LSN، بدون از دست رفتن یا تکرار تغییرات.
   - SMT `ExtractNewRecordState` رکورد را مسطح می‌کند و `__op`، `__lsn` و `__source_ts_ms` را اضافه می‌کند. Delete هم با `__deleted=true` نگه داشته می‌شود (`rewrite`).
   - کلید پیام برابر PK است، پس همهٔ تغییرات یک رکورد در یک پارتیشن می‌روند و ترتیبشان حفظ می‌شود.
3. **Kafka 4.1 (KRaft)**: بافر بادوام بین منبع و مقصد. اگر ClickHouse از دسترس خارج شود، داده گم نمی‌شود و از offset قبلی ادامه پیدا می‌کند.
4. **ClickHouse 25.8**:
   - جدول `Kafka` با فرمت `JSONAsString` پیام را بدون فرض دربارهٔ schema می‌خواند، پس ستون جدید پایپ‌لاین را نمی‌شکند.
   - Materialized View فیلدها را با `JSONExtract` استخراج می‌کند. کلیدی که نیست NULL برمی‌گرداند و خطا نمی‌دهد، و payload کامل هم ذخیره می‌شود.
   - پیام خراب به `raw.cdc_dead_letter` می‌رود (`kafka_handle_error_mode='stream'`) و مصرف متوقف نمی‌شود.
5. **تبدیل‌ها داخل خود ClickHouse** ([جزئیات](data-model.md)):
   - raw ← stg ← dimها با MV معمولی و لحظه‌ای انجام می‌شود. این MVها فقط تبدیل نوع دارند و join ندارند.
   - fact و aggregateها با Refreshable MV ساخته می‌شوند: `REFRESH EVERY 1 MINUTE` در حالت `APPEND`، و با `DEPENDS ON` به ترتیب.
   - تست‌های کیفیت هم یک Refreshable MV هستند که هر ۱۵ دقیقه نتیجه را در `meta.dq_results` می‌نویسد.
   - وضعیت همهٔ مراحل در `system.view_refreshes` دیده می‌شود. دو اجرای یک مرحله هیچ‌وقت هم‌زمان نمی‌شوند و اجرای ناموفق با `refresh_retries` دوباره امتحان می‌شود.
6. **Metabase** با کاربر فقط‌خواندنی `metabase` وصل می‌شود که فقط به `mart.*` دسترسی دارد. داشبورد قدیمی هم با کاربر فقط‌خواندنی `bi_reader` به core وصل است.

## تضمین‌های T1

| معیار | چگونه |
|---|---|
| Snapshot و تغییرات بعدی | `snapshot.mode=initial`. ستون `_op` در raw مقدار r، c، u یا d دارد. |
| Update و Delete درست | `ReplacingMergeTree(_lsn, _is_deleted)`: نسخه با LSN بالاتر می‌ماند. Delete با FINAL پنهان می‌شود و در fact و aggregate هم پخش می‌شود. |
| ترتیب | ترتیب در هر پارتیشن Kafka (کلید = PK) حفظ می‌شود و LSN نسخه را تعیین می‌کند، پس رویداد تکراری یا دیررس (at-least-once) حالت جدید را خراب نمی‌کند. |
| تاریخچه | `raw.cdc_*` هر رویداد را برای همیشه نگه می‌دارد. `dim_merchant_history` همین را به SCD2 تبدیل می‌کند. |
| ستون `installments` | Debezium schema جدید را از پیام Relation در pgoutput می‌گیرد. در ClickHouse، `JSONExtract(... 'Nullable(Int32)')` از قبل وجود دارد و ستون‌های ناشناختهٔ بعدی هم در `_payload` می‌مانند. |

## گزینه‌هایی که کنار گذاشته شدند

- **ClickHouse `MaterializedPostgreSQL`**: آزمایشی است، فقط وضعیت جاری را نگه می‌دارد (تاریخچه ندارد) و با تغییر schema مشکل دارد.
- **PeerDB / Airbyte / Fivetran**: Fivetran پولی است و Airbyte برای ۱۶ گیگابایت RAM سنگین است و micro-batch کار می‌کند. Debezium استاندارد باز و سبک‌تری است.
- **Debezium Server یا Embedded به‌جای Kafka**: یک سرویس کمتر دارد، ولی بافر بادوام، replay و امکان چند مصرف‌کننده را از دست می‌دهیم. برای گفت‌وگو دربارهٔ مقیاس واقعی هم Kafka گزینهٔ طبیعی‌تری است.
- **Redpanda**: سبک‌تر است. Kafka رسمی به‌خاطر رایج بودن و سازگاری کامل با Connect انتخاب شد. با KRaft دیگر ZooKeeper لازم نیست و تفاوت حافظه کم است.
- **مقصد Postgres یا DuckDB**: Postgres برای اسکن تحلیلی میلیون‌ها ردیف کند است. DuckDB یک فایل محلی است و برای نوشتن هم‌زمان stream و خواندن هم‌زمان Metabase مناسب نیست.
- **ClickHouse Kafka Connect Sink به‌جای Kafka engine**: یک JVM دیگر لازم دارد. موتور داخلی Kafka ساده‌تر است و offsetها را خودش مدیریت می‌کند.
- **dbt**: تست، مستندسازی و lineage آماده دارد، ولی یک ابزار و یک سرویس زمان‌بند اضافه می‌کند. چون پلتفرم تحلیلی ClickHouse است، قابلیت‌های داخلی خودش (MV و Refreshable MV) برای این اندازه کافی است و همهٔ منطق در یک جا باقی می‌ماند. در مقیاس واقعی، همین SQLها به‌راحتی به مدل‌های dbt منتقل می‌شوند.
- **حلقهٔ Python یا cron برای اجرای SQL**: ساده است، ولی کاری را تکرار می‌کند که ClickHouse خودش انجام می‌دهد: زمان‌بندی، جلوگیری از اجرای هم‌زمان، retry و ثبت وضعیت.
- **MV معمولی و زنجیره‌ای (SummingMergeTree) برای fact و aggregateها**: لحظه‌ای‌تر است، ولی برای داده‌ای که تغییر می‌کند (PENDING ← SUCCESS ← REVERSED، delete و dimension دیررس) منطق retraction لازم دارد. با پیام تکراری Kafka دوباره شمرده می‌شود و idempotent نیست. خطای آن هم insert به raw و در نتیجه مصرف از Kafka را متوقف می‌کند. Refreshable MV یک‌دقیقه‌ای ساده‌تر، idempotent و جدا از ورود داده است.
- **Hash کردن PII در Debezium (`column.mask.hash`)**: طبق مستندات Debezium، خروجی hash به طول ستون کوتاه می‌شود. برای `VARCHAR(10)` این یعنی hash ده‌کاراکتری که برخورد زیادی دارد. به همین دلیل hash در اولین نقطهٔ ورود به ClickHouse و با کلید محرمانه انجام شد.
- **Cache نتایج Metabase**: زمان را کم می‌کند ولی مشکل اصلی (بار روی core و نبودن تاریخچه) را حل نمی‌کند. در بنچمارک هم با `ignore_cache` غیرفعال شده است.
- **Airflow یا Dagster**: برای یک job یک‌دقیقه‌ای چهار سرویس و حدود ۲ گیگابایت RAM اضافه می‌کنند. Airflow برای اجراهای دقیقه‌ای ساخته نشده: هر اجرا چند ثانیه سربار دارد و روزانه ۱۴۴۰ اجرا متادیتای آن را پر می‌کند. در مقیاس واقعی، با چند pipeline، backfill و هشدار، انتخاب طبیعی است.

## مصرف منابع (سقف `mem_limit`)

| سرویس | سقف حافظه |
|---|---|
| ClickHouse (شامل همهٔ تبدیل‌ها) | ۳.۵ گیگابایت |
| Metabase | ۱.۸ گیگابایت |
| Postgres | ۱.۵ گیگابایت |
| Kafka Connect | ۱.۳ گیگابایت |
| Kafka | ۱ گیگابایت |
| بقیه | زیر ۰.۵ گیگابایت |

مصرف واقعی اندازه‌گیری‌شده روی سرور آزمایش حدود ۴.۹ گیگابایت بود: ClickHouse ۱.۰، Metabase ۱.۴، Postgres ۱.۰، Kafka Connect ۰.۹ و Kafka ۰.۵ گیگابایت. Docker Desktop روی WSL2 به‌طور پیش‌فرض ۵۰٪ RAM را می‌گیرد که روی ماشین ۱۶ گیگابایتی حدود ۸ گیگابایت می‌شود و کافی است.

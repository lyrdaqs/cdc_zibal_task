# راهنمای اجرا، اتصال و تست

## ۰. پیش‌نیاز (Windows)

1. نصب WSL2 در PowerShell با دسترسی Administrator، و بعد ری‌استارت:

   ```bash
   wsl --install
   ```

2. نصب **Docker Desktop** و فعال بودن گزینهٔ «Use WSL 2 based engine».
3. (اختیاری) اگر RAM کم بود، فایل `%UserProfile%\.wslconfig` را بسازید:

   ```ini
   [wsl2]
   memory=10GB
   ```

لینوکس یا مک: فقط Docker و Compose v2 لازم است.

**سرور داخل ایران**:

- Docker Hub باید از طریق mirror در دسترس باشد. برای مثال، در `/etc/docker/daemon.json` تنظیم `"registry-mirrors": ["https://docker.arvancloud.ir"]` را بگذارید.
- اگر `files.pythonhosted.org` در دسترس نیست، در `.env` این مقدار را بگذارید:

  ```
  PIP_INDEX_URL=https://package-mirror.liara.ir/repository/pypi/simple
  ```

- image مربوط به Debezium از `quay.io` گرفته می‌شود.

**امنیت پورت‌ها**: همهٔ پورت‌ها فقط روی `127.0.0.1` باز می‌شوند. روی سرور راه دور، با تونل SSH وصل شوید:

```bash
ssh -L 3000:localhost:3000 <server>
```

## ۱. اجرا

```bash
docker compose up -d --build
```

| زمان تقریبی | اتفاق |
|---|---|
| ۰ تا ۳ دقیقه | ساخت imageها و seed پنج میلیون ردیف. `postgres` تا پایان seed وضعیت healthy نمی‌گیرد. |
| ۳ تا ۸ دقیقه | Snapshot در Debezium و رسیدن داده به ClickHouse. stg لحظه‌ای پر می‌شود و fact و aggregateها هر دقیقه. |
| بعد از آن | `metabase-setup` داشبوردها را می‌سازد، صبر می‌کند تا پایپ‌لاین به core برسد، بنچمارک می‌گیرد و `docs/results/benchmark.md` را می‌نویسد. |

برای دنبال کردن پیشرفت:

```bash
docker compose ps
```

```bash
docker compose logs -f metabase-setup
```

وضعیت مراحل تبدیل در ClickHouse:

```sql
SELECT view, status, last_success_time, last_success_duration_ms, written_rows, exception
FROM system.view_refreshes ORDER BY view;
```

## ۲. اتصال

| سرویس | آدرس از روی host | کاربر / رمز (مقادیر تستی `.env`) |
|---|---|---|
| Metabase | http://localhost:3000 | `admin@payyar.local` / `Payyar-admin-2026` |
| Postgres core | `localhost:15432`، دیتابیس `payyar` | `payyar_admin` / `admin_pw_local` (مدیر)، `bi_reader` / `reader_pw_local` (فقط‌خواندنی) |
| ClickHouse HTTP و Play UI | http://localhost:18123/play | `etl` / `etl_pw_local` (خواندن raw، stg، mart، meta)، `metabase` / `mb_ch_pw_local` (فقط mart)، `ch_admin` / `ch_admin_pw_local` (مدیر و definer همهٔ viewها) |
| Kafka Connect REST | http://localhost:8083/connectors/payyar-core/status | — |

در DBeaver یا DataGrip:

- Postgres: یک اتصال معمولی با پورت 15432.
- ClickHouse: درایور ClickHouse، host `localhost`، پورت `18123`، کاربر `etl`.

کلاینت خط فرمان داخل کانتینرها، Postgres:

```bash
docker compose exec postgres psql -U payyar_admin -d payyar
```

و ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user etl --password etl_pw_local
```

## ۳. سناریوهای تست

### الف) T0: محیط مبدأ

```sql
-- Postgres: حجم و توزیع
SELECT status, count(*), round(100.0*count(*)/sum(count(*)) over (),1) FROM core.transactions GROUP BY 1;
SELECT count(*) FROM core.merchants;              -- 2000
SELECT max(created_at) FROM core.transactions;    -- باید مدام جلو برود (شبیه‌ساز)
```

داشبورد «فروش روزانه» در Metabase روی core ساخته شده است. زمان هر کارت در `docs/results/benchmark.md` ستون core ثبت می‌شود.

### ب) T1: CDC

تست سرتاسری خودکار. خروجی `ALL CHECKS PASSED` یعنی همه‌چیز درست است:

```bash
docker compose run --rm ops python e2e_cdc_test.py
```

یا دستی در ClickHouse:

```sql
SELECT _op, count() FROM raw.cdc_transactions GROUP BY _op;          -- r (snapshot) + c/u/d
SELECT transaction_id, _op, status, _lsn FROM raw.cdc_transactions
WHERE transaction_id = (SELECT max(transaction_id) - 100 FROM raw.cdc_transactions) ORDER BY _lsn;  -- c→u(→u)
SELECT merchant_id, version_no, risk_level, province, valid_from, valid_to
FROM mart.dim_merchant_history WHERE merchant_id IN
 (SELECT merchant_id FROM mart.dim_merchant_history WHERE version_no > 1) ORDER BY 1,2;     -- تاریخچه
```

**تغییر Schema در حین کار**:

```bash
docker compose exec simulator python schema_change.py
```

بعد از حدود یک دقیقه:

```sql
SELECT installments, count() FROM mart.fct_transactions FINAL
WHERE created_at > now() - INTERVAL 5 MINUTE GROUP BY 1;   -- مقادیر 1/3/6/12
```

```bash
curl http://localhost:8083/connectors/payyar-core/status
```

در خروجی بالا وضعیت باید `RUNNING` باشد.

### ج) T2، T3 و T5: تست‌های کیفیت داده (داخل ClickHouse)

همهٔ تست‌ها در نمای `meta.v_data_quality` تعریف شده‌اند ([04_quality.sql](../clickhouse/sql/04_quality.sql)). هر ۱۵ دقیقه خودکار اجرا می‌شوند و نتیجه در `meta.dq_results` و داشبورد «کیفیت داده» در Metabase دیده می‌شود. برای اجرای فوری:

```bash
docker compose exec clickhouse bash /scripts/run_tests.sh
```

خروجی برای هر تست PASS، FAIL یا WARN است. اگر تستی در سطح error رد شود، دستور با کد خطا تمام می‌شود. تست‌ها:

- **کلید**: یکتایی کلیدها روی وضعیت جاری (`FINAL`)، و وجود `merchant_id` در dim
- **مقادیر مجاز**: `_op`، `channel`، `status` و `risk_level`، و منفی نبودن مبلغ و کارمزد
- **PII و مالکیت**:
  - نبودن ستون PII خام در stg و mart
  - hash شصت‌وچهار کاراکتری و mask بودن موبایل
  - داشتن `owner=` در COMMENT همهٔ جدول‌ها
- **point-in-time**: ریسک، استان و صنفِ زمان تراکنش با یک join مستقل روی بازه بررسی می‌شوند.
- **سازگاری**: aggregateها با fact برابرند.
- **تطبیق با core**: تعداد و مبلغ روزانه به تفکیک وضعیت، و تعداد و مبلغ تراکنش‌های موفق. Postgres مستقیم از طریق جدول `secure.core_transactions` با موتور PostgreSQL خوانده می‌شود و فقط روزهایی مقایسه می‌شوند که دست‌کم ۱۵ دقیقه از بسته شدنشان گذشته است.
- **سلامت پایپ‌لاین (warn)**: تازگی raw و fact، خطای Refreshable MVها، و پیام‌های dead-letter

مالک و ستون‌های حساس هر جدول:

```sql
SELECT database, name, comment FROM system.tables WHERE database IN ('stg','mart');
SELECT table, name, comment FROM system.columns WHERE database = 'mart' AND comment != '';
```

### د) T4: بنچمارک و مقایسه دقیق

بنچمارک در حالت زنده، با شبیه‌ساز در حال اجرا:

```bash
docker compose run --rm ops python benchmark.py
```

برای مقایسهٔ دقیق (بدون اثر تأخیر)، اول شبیه‌ساز را متوقف کنید:

```bash
docker compose stop simulator
```

بعد بنچمارک را با `--quiesce` اجرا کنید:

```bash
docker compose run --rm ops python benchmark.py --quiesce
```

در آخر شبیه‌ساز را دوباره روشن کنید:

```bash
docker compose start simulator
```

خروجی در `docs/results/benchmark.md` و `benchmark.json` ذخیره می‌شود. در نتیجهٔ مقایسه، کارت‌های ۱ تا ۳ و `4_current_attributes` باید `identical: true` باشند. کارت‌های ۴ و ۵ ممکن است عمداً متفاوت باشند، چون ویژگی‌های زمان تراکنش را نشان می‌دهند.

## ۴. عیب‌یابی و ریست

| علامت | بررسی |
|---|---|
| تغییر SQL در `clickhouse/sql` اعمال نمی‌شود | این فایل‌ها فقط روی volume خالی اجرا می‌شوند. ریست کامل لازم است (پایین). |
| داده به ClickHouse نمی‌رسد | `docker compose logs connect cdc-init`، و اجرای `SELECT * FROM raw.cdc_dead_letter` |
| mart به‌روز نمی‌شود | `SELECT view, status, exception FROM system.view_refreshes`. برای اجرای فوری: `SYSTEM REFRESH VIEW pipeline.rmv_fct_transactions` |
| Metabase کند بالا می‌آید | اولین بار حدود ۱ تا ۲ دقیقه طول می‌کشد. `metabase-setup` خودش صبر می‌کند. |

برای ریست کامل که همهٔ داده‌ها را پاک می‌کند:

```bash
docker compose down -v
```
